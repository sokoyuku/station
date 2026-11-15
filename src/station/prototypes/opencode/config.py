import json
import os
import shlex
from dataclasses import dataclass

from station.prototypes.boundary import ext_dict, ext_mapping_get
from station.prototypes.fs_paths import write_atomic
from station.prototypes.launch_settings import (
    LOCAL_LLM_PROVIDERS,
    collect_provider_env,
    launch_sections,
    merge_launch_settings,
    parse_command_args,
    resolve_local_llm,
    split_provider_model,
    uses_xai as provider_uses_xai,
    voice_settings_from_mapping,
)
from station.prototypes.workspace import is_windows_interop_path

def _linux_opencode_paths():
    return (
        "/usr/local/bin/opencode",
        os.path.join(os.path.expanduser("~"), ".local", "bin", "opencode"),
    )

def _usable_linux_opencode(path):
    if not path or not os.path.isabs(path) or is_windows_interop_path(path) or not os.path.exists(path):
        return ""
    resolved = os.path.realpath(path)
    if is_windows_interop_path(resolved):
        return ""
    if not os.path.isfile(resolved) or not os.access(resolved, os.X_OK):
        return ""
    try:
        with open(resolved, "rb") as handle:
            magic = handle.read(4)
    except OSError:
        return ""
    if magic != b"\x7fELF":
        return ""
    return resolved

def _resolve_opencode_command(server):
    raw = ext_mapping_get(server, "command", (str, list), None, allow_none=True)
    explicit = None
    if isinstance(raw, str):
        if raw.strip():
            explicit = shlex.split(raw)
    elif raw:
        explicit = parse_command_args(raw, "server.command")

    if explicit:
        usable = _usable_linux_opencode(explicit[0])
        if not usable:
            raise RuntimeError(
                "server.command must be an absolute path to the Linux OpenCode ELF at "
                "/usr/local/bin/opencode or ~/.local/bin/opencode"
            )
        return [usable, *explicit[1:]]

    for path in _linux_opencode_paths():
        usable = _usable_linux_opencode(path)
        if usable:
            return [usable]

    raise RuntimeError(
        "Linux OpenCode ELF not found at /usr/local/bin/opencode or ~/.local/bin/opencode"
    )

OPENCODE_LOCAL_PROVIDER = "local-llm"


def write_local_llm_opencode_config(path, *, settings):
    model_id = settings.model or "default"
    provider_id = settings.gateway_provider()
    api_key = settings.local_llm_api_key or "local"
    model_name = model_id.rsplit("/", 1)[-1]
    limit = {
        "context": settings.local_llm_context_window or 32768,
        "output": settings.local_llm_max_output or 8192,
    }
    payload = {
        "$schema": "https://opencode.ai/config.json",
        "model": "%s/%s" % (provider_id, model_id),
        "providers": {
            provider_id: {
                "package": "aisdk:@ai-sdk/openai-compatible",
                "name": "Local LLM",
                "settings": {
                    "baseURL": settings.local_llm_base_url,
                    "apiKey": api_key,
                    "chat_template_kwargs": {"enable_thinking": False},
                },
                "models": {
                    model_id: {
                        "name": model_name,
                        "capabilities": {
                            "tools": True,
                            "input": ["text"],
                            "output": ["text"],
                        },
                        "limit": limit,
                    },
                },
            }
        },
        "permissions": [
            {"action": "*", "resource": "*", "effect": "allow"},
        ],
    }
    extra = dict(settings.opencode_overrides or {})
    extra_providers = extra.pop("providers", None)
    payload.update(extra)
    if extra_providers is not None:
        extra_providers = ext_dict("opencode providers override", extra_providers)
        merged = dict(payload.get("providers") or {})
        merged.update(extra_providers)
        payload["providers"] = merged
    write_atomic(path, json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
    return path


@dataclass(slots=True)
class OpenCodeLaunchSettings:
    command: list
    command_cwd: str
    workspace_dir: str
    hostname: str
    port: int
    model: str
    provider: str
    agent: str
    variant: str
    approvals_mode: str
    server_password: str
    server_username: str
    extra_env: dict
    serve_args: list
    api_key: str
    local_llm_base_url: str
    local_llm_api_key: str
    local_llm_context_window: int
    local_llm_max_output: int
    opencode_overrides: dict
    voice: dict
    voice_reply_mode: str
    base_url: str

    def uses_local_llm(self):
        if self.local_llm_base_url:
            return True
        return self.provider.lower() in LOCAL_LLM_PROVIDERS

    def gateway_provider(self):
        if self.uses_local_llm():
            return OPENCODE_LOCAL_PROVIDER
        return self.provider

    def uses_xai(self):
        return provider_uses_xai(self.provider, self.model, local_llm=self.uses_local_llm())

    @classmethod
    def from_model_settings(cls, model_settings, *, default_workspace, config_file=None, secret_file=None, workspace_override=None, overlay=None):
        raw = merge_launch_settings(
            model_settings,
            config_file=config_file,
            secret_file=secret_file,
            overlay=overlay,
        )

        sections = launch_sections(raw, "model", "server", "workspace", "keys", "local_llm", "opencode", "voice")
        model = sections["model"]
        server = sections["server"]
        workspace = sections["workspace"]
        keys = sections["keys"]
        local_llm = sections["local_llm"]
        opencode_overrides = sections["opencode"]
        voice = sections["voice"]
        voice_reply_mode = voice_settings_from_mapping(voice)

        workspace_dir = (
            (workspace_override.strip() if workspace_override else "")
            or ext_mapping_get(workspace, "dir", (str,), "")
            or default_workspace
        )
        workspace_path = os.path.expanduser(workspace_dir)

        hostname = ext_mapping_get(server, "hostname", (str,), "") or "127.0.0.1"
        port = ext_mapping_get(server, "port", (int,), None, allow_none=True)
        if port is None:
            port = 0

        serve_args = parse_command_args(
            ext_mapping_get(server, "serve_args", (list,), None, allow_none=True),
            "server.serve_args",
        )

        provider, model_id = split_provider_model(
            ext_mapping_get(model, "model", (str,), ""),
            ext_mapping_get(model, "provider", (str,), ""),
        )
        if provider.lower() == "grok":
            provider = "xai"
        if not provider and model_id.lower().startswith("grok"):
            provider = "xai"

        extra_env = collect_provider_env(keys)
        (
            provider,
            local_llm_base_url,
            local_llm_api_key,
            uses_local_llm,
            local_llm_context_window,
            local_llm_max_output,
        ) = resolve_local_llm(
            model=model,
            keys=keys,
            local_llm=local_llm,
            extra_env=extra_env,
            provider=provider,
            empty_provider=OPENCODE_LOCAL_PROVIDER,
            max_output_default=8192,
        )
        api_key = (
            (local_llm_api_key or "local")
            if uses_local_llm
            else (
                ext_mapping_get(keys, "xai_api_key", (str,), "")
                or ext_mapping_get(model, "api_key", (str,), "")
            )
        )
        if uses_local_llm:
            local_llm_api_key = api_key
        elif api_key:
            extra_env.setdefault("XAI_API_KEY", api_key)

        return cls(
            command=_resolve_opencode_command(server),
            command_cwd=workspace_path if os.path.isdir(workspace_path) else os.getcwd(),
            workspace_dir=workspace_path,
            hostname=hostname,
            port=port,
            model=model_id,
            provider=provider,
            agent=ext_mapping_get(model, "agent", (str,), ""),
            variant=ext_mapping_get(model, "variant", (str,), ""),
            approvals_mode=ext_mapping_get(model, "approvals_mode", (str,), "always").lower(),
            server_password=ext_mapping_get(server, "password", (str,), ""),
            server_username=ext_mapping_get(server, "username", (str,), "") or "opencode",
            extra_env=extra_env,
            serve_args=serve_args,
            api_key=api_key,
            local_llm_base_url=local_llm_base_url,
            local_llm_api_key=local_llm_api_key,
            local_llm_context_window=local_llm_context_window,
            local_llm_max_output=local_llm_max_output,
            opencode_overrides=dict(opencode_overrides),
            voice=dict(voice),
            voice_reply_mode=voice_reply_mode,
            base_url=local_llm_base_url,
        )

    def model_payload(self):
        provider = self.gateway_provider()
        if not self.model or not provider:
            return None
        payload = {"providerID": provider, "modelID": self.model}
        if self.variant:
            payload["variant"] = self.variant
        return payload
