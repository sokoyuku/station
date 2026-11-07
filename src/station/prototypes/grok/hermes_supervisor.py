import asyncio
import contextvars
import json
import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from station import logger
from station.errors import ExternalError
from station.config.config import merge_nested
from station.prototypes.boundary import ext_bool, ext_dict, ext_int, ext_list, ext_str
from station.prototypes.hermes.config import HermesLaunchSettings, hermes_home_path
from station.prototypes.hermes.gateway_client import HermesGatewayProcess
from station.prototypes.hermes.prototype import HermesPrototype

hermes_chat_context = contextvars.ContextVar("grok_hermes_chat", default=None)

MAX_TASKS = 3
PROGRESS_CHARS = 400
SUMMARY_CHARS = 2000

HERMES_TOOL_NAMES = frozenset({
    "hermes_delegate",
    "hermes_status",
    "hermes_steer",
    "hermes_interrupt",
    "hermes_approve",
    "hermes_result",
    "hermes_config",
})

_CONFIG_FIELDS = (
    "model",
    "provider",
    "base_url",
    "api_key",
    "context_window",
    "max_output",
    "reasoning_effort",
    "fast",
    "approvals_mode",
    "toolsets",
    "config",
)

_SECRET_KEYS = frozenset({
    "api_key",
    "apikey",
    "token",
    "secret",
    "password",
    "authorization",
    "local_llm_api_key",
})

HERMES_TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "hermes_delegate",
            "description": (
                "Hand a workspace task to hermes-agent and return immediately with task_id. "
                "The worker uses the current hermes_config, including its model and toolsets. "
                "You supervise it with hermes_status, hermes_steer, hermes_approve, hermes_interrupt, and hermes_result. "
                "Do not submit the same goal again after it finishes."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "goal": {"type": "string"},
                    "context": {"type": "string"},
                    "workspace": {"type": "string"},
                    "files": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["goal"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "hermes_status",
            "description": "Read one hermes task, or all of them when task_id is omitted. Does not wait.",
            "parameters": {
                "type": "object",
                "properties": {
                    "task_id": {"type": "string"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "hermes_steer",
            "description": "Interrupt the current hermes turn and send a new instruction.",
            "parameters": {
                "type": "object",
                "properties": {
                    "task_id": {"type": "string"},
                    "text": {"type": "string"},
                },
                "required": ["task_id", "text"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "hermes_interrupt",
            "description": "Stop the current hermes turn and leave the session in place.",
            "parameters": {
                "type": "object",
                "properties": {
                    "task_id": {"type": "string"},
                },
                "required": ["task_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "hermes_approve",
            "description": "Allow or deny a hermes approval, or answer a question the worker asked. Use only when status is needs_approval. Pass text when the worker asked a question.",
            "parameters": {
                "type": "object",
                "properties": {
                    "task_id": {"type": "string"},
                    "allow": {"type": "boolean"},
                    "text": {"type": "string"},
                },
                "required": ["task_id", "allow"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "hermes_result",
            "description": "Return the hermes task summary after it is done. Empty while it is still running.",
            "parameters": {
                "type": "object",
                "properties": {
                    "task_id": {"type": "string"},
                },
                "required": ["task_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "hermes_config",
            "description": (
                "Read or change the hermes-agent worker config, including the model it uses. "
                "Call with no changes to read it. "
                "Pass model, provider, base_url, api_key, context_window, max_output, reasoning_effort, fast, approvals_mode, toolsets, or config to change it. "
                "toolsets is the list of toolsets the worker may use, or [\"all\"]. "
                "config is merged into the hermes config document. Set replace_config true to replace that overlay. "
                "The next hermes_delegate uses the saved config. "
                "A running worker restarts. Interrupt running tasks first, or pass force true to drop them."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "model": {"type": "string"},
                    "provider": {"type": "string"},
                    "base_url": {"type": "string"},
                    "api_key": {"type": "string"},
                    "context_window": {"type": "integer"},
                    "max_output": {"type": "integer"},
                    "reasoning_effort": {"type": "string"},
                    "fast": {"type": "boolean"},
                    "approvals_mode": {"type": "string"},
                    "toolsets": {"type": "array", "items": {"type": "string"}},
                    "config": {"type": "object"},
                    "replace_config": {"type": "boolean"},
                    "force": {"type": "boolean"},
                },
            },
        },
    },
]


def _clip(text, limit):
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[-limit:]


@dataclass(slots=True)
class HermesTask:
    task_id: str
    session_id: str
    goal: str
    chat_id: str
    acct_id: str
    platform: str = ""
    chat_type: str = ""
    status: str = "running"
    progress: str = ""
    summary: str = ""
    error: str = ""
    approval: dict = field(default_factory=dict)
    wake_for: str = ""
    wake_pending: bool = False


class GrokHermesSupervisor:
    def _init_hermes_supervisor(self):
        if getattr(self, "_hermes_ready", False):
            return
        self._hermes_tasks = {}
        self._hermes_gateway = None
        self._hermes_settings = None
        self._hermes_lock = asyncio.Lock()
        self._hermes_drains = {}
        self._hermes_flushing = set()
        self._hermes_ready = True

    def _worker_config_path(self):
        raw = self.model_settings if isinstance(self.model_settings, dict) else {}
        worker = raw.get("hermes_worker")
        if isinstance(worker, dict):
            explicit = ext_str("hermes_worker.config_file", worker.get("config_file"))
            if explicit:
                return Path(explicit).expanduser()
        return Path(__file__).resolve().parents[5] / "config" / "dev" / "hermes" / "config.toml"

    def _worker_profile_path(self):
        return Path(self.storage_dir) / "hermes_worker.json"

    def _load_worker_profile(self):
        path = self._worker_profile_path()
        if not path.is_file():
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            raise ExternalError("hermes worker profile is not valid JSON")
        return ext_dict("hermes worker profile", data)

    def _save_worker_profile(self, profile):
        path = self._worker_profile_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(profile, indent=2), encoding="utf-8")

    def _apply_worker_profile(self, settings, profile):
        if "model" in profile:
            settings.model = str(profile.get("model") or "")
        if "provider" in profile:
            settings.provider = str(profile.get("provider") or "")
        if "base_url" in profile:
            settings.local_llm_base_url = str(profile.get("base_url") or "")
            settings.base_url = settings.local_llm_base_url
        if "api_key" in profile:
            settings.local_llm_api_key = str(profile.get("api_key") or "")
            settings.api_key = settings.local_llm_api_key
        if "context_window" in profile:
            settings.local_llm_context_window = int(profile.get("context_window") or 0)
        if "max_output" in profile:
            settings.local_llm_max_output = int(profile.get("max_output") or 0)
        if "reasoning_effort" in profile:
            settings.reasoning_effort = str(profile.get("reasoning_effort") or "")
        if "fast" in profile:
            settings.fast = bool(profile.get("fast"))
        if "approvals_mode" in profile:
            settings.approvals_mode = str(profile.get("approvals_mode") or "manual")
        if "toolsets" in profile:
            toolsets = profile.get("toolsets") or []
            if isinstance(toolsets, str):
                toolsets = [part.strip() for part in toolsets.split(",") if part.strip()]
            settings.enabled_toolsets = [str(item).strip() for item in toolsets if str(item).strip()]
        if isinstance(profile.get("config"), dict):
            settings.config_overrides = merge_nested(settings.config_overrides, profile["config"])
        if settings.enabled_toolsets is not None:
            enabled = {name.lower() for name in settings.enabled_toolsets}
            if "all" in enabled:
                disabled = []
            else:
                disabled = [name for name in ("file", "terminal", "web", "memory", "vision") if name not in enabled]
            settings.disabled_toolsets = disabled
            settings.config_overrides["disabled_toolsets"] = list(disabled)

    def _worker_settings(self):
        config_path = self._worker_config_path()
        if not config_path.is_file():
            raise ExternalError("hermes worker config not found")
        workspace = os.path.join(self.storage_dir, "hermes_workspace")
        settings = HermesLaunchSettings.from_model_settings(
            {},
            default_workspace=workspace,
            config_file=str(config_path),
        )
        profile = self._load_worker_profile()
        if "approvals_mode" not in profile:
            settings.approvals_mode = "manual"
        if "toolsets" not in profile:
            disabled = [item for item in settings.disabled_toolsets if item]
            if "memory" not in disabled:
                disabled.append("memory")
            settings.disabled_toolsets = disabled
            settings.config_overrides["disabled_toolsets"] = list(disabled)
        self._apply_worker_profile(settings, profile)
        if not settings.hermes_root.exists():
            raise ExternalError("hermes root not found")
        return settings

    def _gateway_running(self):
        gateway = getattr(self, "_hermes_gateway", None)
        return gateway is not None and gateway.proc is not None and gateway.proc.returncode is None

    async def _stop_hermes_process(self):
        drains = list(self._hermes_drains.values())
        self._hermes_drains = {}
        for task in drains:
            task.cancel()
        if drains:
            await asyncio.gather(*drains, return_exceptions=True)
        gateway = self._hermes_gateway
        self._hermes_gateway = None
        self._hermes_settings = None
        if gateway is not None:
            await gateway.close()

    async def _start_hermes_gateway(self):
        settings = self._worker_settings()
        Path(settings.workspace_dir).expanduser().mkdir(parents=True, exist_ok=True)
        hermes_home = hermes_home_path(self.storage_dir)
        HermesPrototype.__new__(HermesPrototype)._write_hermes_config(
            hermes_home=hermes_home,
            settings=settings,
        )
        gateway = HermesGatewayProcess(settings=settings, hermes_home=hermes_home)
        await gateway.start()
        self._hermes_settings = settings
        self._hermes_gateway = gateway
        logger.info(
            "Grok hermes worker started model_id=%s provider=%s model=%s",
            self.model_id,
            settings.gateway_provider(),
            settings.model,
        )
        return gateway

    async def _ensure_hermes_gateway(self):
        self._init_hermes_supervisor()
        async with self._hermes_lock:
            if self._gateway_running():
                return self._hermes_gateway
            return await self._start_hermes_gateway()

    def _running_tasks(self):
        return sum(1 for task in self._hermes_tasks.values() if task.status in {"running", "needs_approval"})

    def _task(self, task_id):
        task_id = (task_id or "").strip()
        if not task_id:
            raise ExternalError("task_id is required")
        task = self._hermes_tasks.get(task_id)
        if task is None:
            raise ExternalError("unknown task_id")
        return task

    def _snapshot(self, task):
        body = {
            "ok": True,
            "task_id": task.task_id,
            "status": task.status,
            "goal": task.goal,
            "progress": task.progress,
        }
        if task.summary:
            body["summary"] = _clip(task.summary, SUMMARY_CHARS)
        if task.error:
            body["error"] = task.error
        if task.status == "needs_approval" and task.approval:
            body["approval"] = {
                "kind": task.approval.get("kind") or "approval.request",
                "command": task.approval.get("command") or "",
                "description": (
                    task.approval.get("description")
                    or task.approval.get("reason")
                    or task.approval.get("question")
                    or task.approval.get("prompt")
                    or ""
                ),
            }
        return body

    def _resolve_workspace(self, workspace):
        root = os.path.realpath(self.storage_dir)
        if not workspace or not str(workspace).strip():
            path = root
        else:
            raw = str(workspace).strip()
            path = raw if os.path.isabs(raw) else os.path.join(root, raw)
        real = os.path.realpath(path)
        if os.path.commonpath([root, real]) != root:
            raise ExternalError("workspace must stay inside the model storage directory")
        os.makedirs(real, exist_ok=True)
        return real

    def _file_paths(self, files):
        paths = []
        for index, item in enumerate(files or []):
            ref = ext_str("files[%d]" % index, item)
            if not ref:
                continue
            path = self.resolve_file_id(ref)
            if not path or not os.path.isfile(path):
                raise ExternalError("file not found: %s" % ref)
            paths.append(path)
        return paths

    async def _cancel_drain(self, task_id):
        drain = self._hermes_drains.pop(task_id, None)
        if drain is None or drain.done():
            return
        drain.cancel()
        await asyncio.gather(drain, return_exceptions=True)

    def _chat_fields(self):
        chat = hermes_chat_context.get()
        if not chat:
            chat = getattr(self, "_hermes_chat", None) or {}
        return {
            "chat_id": chat.get("chat_id") or "",
            "acct_id": chat.get("acct_id") or "",
            "platform": chat.get("platform") or "",
            "chat_type": chat.get("chat_type") or "",
        }

    async def hermes_delegate(self, arguments):
        goal = ext_str("goal", arguments.get("goal"))
        if not goal:
            return {"ok": False, "error": "goal is required"}
        self._init_hermes_supervisor()
        context = ext_str("context", arguments.get("context"))
        workspace = self._resolve_workspace(arguments.get("workspace"))
        files = arguments.get("files")
        if files is not None:
            files = ext_list("files", files)
        paths = self._file_paths(files)
        gateway = await self._ensure_hermes_gateway()
        async with self._hermes_lock:
            if self._running_tasks() >= MAX_TASKS:
                return {"ok": False, "error": "too many hermes tasks"}
            task_id = uuid.uuid4().hex[:12]
            title = "grok:%s" % task_id
            created = ext_dict(
                "session.create result",
                await gateway.request(
                    "session.create",
                    gateway._session_params({"title": title, "cwd": workspace}),
                ),
            )
            session_id = ext_str("session.create session_id", created.get("session_id"))
            if not session_id:
                return {"ok": False, "error": "hermes session.create failed"}
            gateway.session_queue(session_id)
            chat = self._chat_fields()
            task = HermesTask(
                task_id=task_id,
                session_id=session_id,
                goal=goal,
                chat_id=chat["chat_id"],
                acct_id=chat["acct_id"],
                platform=chat["platform"],
                chat_type=chat["chat_type"],
            )
            self._hermes_tasks[task_id] = task
            prompt = goal
            if context:
                prompt = "%s\n\nContext:\n%s" % (goal, context)
            prompt = (
                "%s\n\nFinish with a short summary of what you did. "
                "Do not message the user yourself."
            ) % prompt
            try:
                for path in paths:
                    await gateway.request(
                        "file.attach",
                        {"session_id": session_id, "path": path, "name": os.path.basename(path)},
                    )
                await gateway.submit_prompt(session_id=session_id, text=prompt)
            except (RuntimeError, OSError) as exc:
                task.status = "failed"
                task.error = str(exc)
                return {"ok": False, "error": str(exc), "task_id": task_id}
            self._hermes_drains[task_id] = asyncio.create_task(
                self._drain_hermes_task(task),
                name="grok-hermes-%s" % task_id,
            )
        return {"ok": True, "task_id": task_id, "status": "running", "session_id": session_id}

    async def _drain_hermes_task(self, task):
        gateway = self._hermes_gateway
        if gateway is None:
            return
        queue = gateway.session_queue(task.session_id)
        deltas = []
        try:
            while task.status in {"running", "needs_approval"}:
                event = await asyncio.wait_for(queue.get(), timeout=1800.0)
                if event.session_id and event.session_id != task.session_id:
                    continue
                if event.type == "message.delta":
                    text = event.payload.get("text") if isinstance(event.payload, dict) else ""
                    if isinstance(text, str) and text:
                        deltas.append(text)
                        task.progress = _clip("".join(deltas), PROGRESS_CHARS)
                    continue
                if event.type == "tool.start":
                    name = ""
                    if isinstance(event.payload, dict):
                        raw = event.payload.get("name") or event.payload.get("tool") or ""
                        name = raw if isinstance(raw, str) else ""
                    if name:
                        task.progress = "tool %s" % name
                    continue
                if event.type == "approval.request":
                    payload = dict(event.payload) if isinstance(event.payload, dict) else {}
                    payload["kind"] = "approval.request"
                    task.approval = payload
                    task.status = "needs_approval"
                    command = payload.get("command") or payload.get("description") or ""
                    task.progress = command if isinstance(command, str) else ""
                    self._queue_hermes_wake(task)
                    continue
                if event.type in {"clarify.request", "secret.request", "sudo.request"}:
                    payload = dict(event.payload) if isinstance(event.payload, dict) else {}
                    payload["kind"] = event.type
                    task.approval = payload
                    task.status = "needs_approval"
                    question = payload.get("question") or payload.get("prompt") or payload.get("text") or event.type
                    task.progress = question if isinstance(question, str) else event.type
                    self._queue_hermes_wake(task)
                    continue
                if event.type == "message.complete":
                    payload = event.payload if isinstance(event.payload, dict) else {}
                    text = payload.get("text") if isinstance(payload.get("text"), str) else ""
                    task.summary = (text or "".join(deltas)).strip()
                    if payload.get("status") == "error":
                        err = payload.get("error") if isinstance(payload.get("error"), str) else ""
                        task.error = err or task.summary or "hermes error"
                        task.status = "failed"
                    elif task.status != "interrupted":
                        task.status = "done"
                    self._queue_hermes_wake(task)
                    return
                if event.type == "error":
                    payload = event.payload if isinstance(event.payload, dict) else {}
                    message = payload.get("message") or payload.get("error") or payload.get("text") or ""
                    task.error = message if isinstance(message, str) else "hermes error"
                    task.status = "failed"
                    self._queue_hermes_wake(task)
                    return
        except asyncio.TimeoutError:
            task.status = "failed"
            task.error = "hermes task timed out"
            self._queue_hermes_wake(task)
        except Exception as exc:
            logger.error("grok hermes drain failed task_id=%s error=%s", task.task_id, exc)
            task.status = "failed"
            task.error = str(exc)
            self._queue_hermes_wake(task)

    def _queue_hermes_wake(self, task):
        if task.status not in {"done", "failed", "needs_approval", "interrupted"}:
            return
        if task.wake_for == task.status:
            return
        task.wake_for = task.status
        task.wake_pending = True
        if not task.acct_id or not task.chat_id:
            return
        key = (task.acct_id, task.chat_id)
        if self._turn_is_active() or key in self._hermes_flushing:
            return
        asyncio.create_task(
            self._flush_hermes_wakes(task.acct_id, task.chat_id),
            name="grok-hermes-wake-%s" % task.task_id,
        )

    def _pop_hermes_wake(self, acct_id, chat_id):
        for task in self._hermes_tasks.values():
            if task.wake_pending and task.acct_id == acct_id and task.chat_id == chat_id:
                task.wake_pending = False
                return task
        return None

    async def _flush_hermes_wakes(self, acct_id, chat_id):
        self._init_hermes_supervisor()
        key = (acct_id, chat_id)
        if key in self._hermes_flushing:
            return
        self._hermes_flushing.add(key)
        held = False
        try:
            while True:
                task = self._pop_hermes_wake(acct_id, chat_id)
                if task is None:
                    break
                if self._turn_is_active():
                    task.wake_pending = True
                    held = True
                    break
                text = (
                    "Hermes task %s is %s. The goal was already submitted. "
                    "Tell the user, approve it, or steer it. Do not delegate this same goal again.\n%s"
                    % (
                        task.task_id,
                        task.status,
                        task.summary or task.error or task.progress or "",
                    )
                )
                try:
                    started = await self._run_chat_turn(
                        chat_id=chat_id,
                        acct_id=acct_id,
                        settings=self._build_settings(),
                        text=text,
                        photo_attachment=None,
                        voice_attachment=None,
                        platform=task.platform,
                        chat_type=task.chat_type,
                        user_recorded=False,
                        quiet_if_busy=True,
                    )
                except Exception:
                    logger.exception("grok hermes wake failed task_id=%s", task.task_id)
                    continue
                if started is False:
                    task.wake_pending = True
                    held = True
                    break
        finally:
            self._hermes_flushing.discard(key)
            if not held and self._pop_peek(acct_id, chat_id):
                asyncio.create_task(self._flush_hermes_wakes(acct_id, chat_id))

    def _pop_peek(self, acct_id, chat_id):
        for task in self._hermes_tasks.values():
            if task.wake_pending and task.acct_id == acct_id and task.chat_id == chat_id:
                return True
        return False

    async def hermes_status(self, arguments):
        self._init_hermes_supervisor()
        task_id = ext_str("task_id", arguments.get("task_id"))
        if not task_id:
            return {"ok": True, "tasks": [self._snapshot(task) for task in self._hermes_tasks.values()]}
        body = self._snapshot(self._task(task_id))
        body["ok"] = True
        return body

    async def hermes_result(self, arguments):
        task = self._task(ext_str("task_id", arguments.get("task_id")))
        body = self._snapshot(task)
        body["ok"] = True
        if task.status != "done":
            body["summary"] = ""
        return body

    async def hermes_steer(self, arguments):
        task = self._task(ext_str("task_id", arguments.get("task_id")))
        text = ext_str("text", arguments.get("text"))
        if not text:
            return {"ok": False, "error": "text is required"}
        if task.status == "needs_approval":
            return {"ok": False, "error": "approve or deny the pending request first"}
        gateway = await self._ensure_hermes_gateway()
        await self._cancel_drain(task.task_id)
        await gateway.interrupt_session(session_id=task.session_id)
        task.status = "running"
        task.wake_for = ""
        task.approval = {}
        await gateway.submit_prompt(session_id=task.session_id, text=text)
        self._hermes_drains[task.task_id] = asyncio.create_task(
            self._drain_hermes_task(task),
            name="grok-hermes-%s" % task.task_id,
        )
        body = self._snapshot(task)
        body["ok"] = True
        return body

    async def hermes_interrupt(self, arguments):
        task = self._task(ext_str("task_id", arguments.get("task_id")))
        gateway = await self._ensure_hermes_gateway()
        await gateway.interrupt_session(session_id=task.session_id)
        task.status = "interrupted"
        self._queue_hermes_wake(task)
        await self._cancel_drain(task.task_id)
        body = self._snapshot(task)
        body["ok"] = True
        return body

    async def _interrupt_hermes_chat(self, *, chat_id, acct_id):
        self._init_hermes_supervisor()
        targets = [
            task
            for task in self._hermes_tasks.values()
            if task.acct_id == acct_id
            and task.chat_id == chat_id
            and task.status in {"running", "needs_approval"}
        ]
        if not targets:
            return
        gateway = None
        try:
            gateway = await self._ensure_hermes_gateway()
        except Exception as exc:
            logger.error(
                "Hermes gateway unavailable for stop model_id=%s chat_id=%s error=%s",
                self.model_id,
                chat_id,
                exc,
                exc_info=exc,
            )
        for task in targets:
            if gateway is not None and task.session_id:
                try:
                    await gateway.interrupt_session(session_id=task.session_id)
                except Exception as exc:
                    logger.error(
                        "Hermes interrupt on stop failed model_id=%s task_id=%s error=%s",
                        self.model_id,
                        task.task_id,
                        exc,
                        exc_info=exc,
                    )
            task.status = "interrupted"
            await self._cancel_drain(task.task_id)

    async def hermes_approve(self, arguments):
        task = self._task(ext_str("task_id", arguments.get("task_id")))
        if task.status != "needs_approval":
            return {"ok": False, "error": "task is not waiting for approval"}
        allow = arguments.get("allow")
        if not isinstance(allow, bool):
            raise ExternalError("allow must be bool")
        text = ext_str("text", arguments.get("text"))
        kind = task.approval.get("kind") or "approval.request"
        request_id = task.approval.get("request_id") or ""
        gateway = await self._ensure_hermes_gateway()
        if kind == "approval.request":
            payload = {"choice": "once" if allow else "deny"}
            if request_id:
                payload["request_id"] = request_id
            method = "approval.respond"
        elif kind == "clarify.request":
            if allow and not text:
                return {"ok": False, "error": "text is required to answer the question"}
            payload = {"answer": text or "Denied by the supervisor.", "request_id": request_id}
            method = "clarify.respond"
        elif kind == "secret.request":
            if allow and not text:
                return {"ok": False, "error": "text is required to answer the question"}
            payload = {"value": text if allow else "", "request_id": request_id}
            method = "secret.respond"
        elif kind == "sudo.request":
            if allow and not text:
                return {"ok": False, "error": "text is required to answer the question"}
            payload = {"password": text if allow else "", "request_id": request_id}
            method = "sudo.respond"
        else:
            return {"ok": False, "error": "unsupported approval kind"}
        await gateway.respond(method=method, session_id=task.session_id, payload=payload)
        task.status = "running"
        task.approval = {}
        task.wake_for = ""
        body = self._snapshot(task)
        body["ok"] = True
        return body

    def _redact_config(self, value):
        if isinstance(value, dict):
            redacted = {}
            for key, item in value.items():
                if str(key).lower() in _SECRET_KEYS:
                    text = item if isinstance(item, str) else ""
                    redacted[key] = "****%s" % text[-4:] if len(text) > 4 else "(set)" if text else ""
                else:
                    redacted[key] = self._redact_config(item)
            return redacted
        if isinstance(value, list):
            return [self._redact_config(item) for item in value]
        return value

    def _public_worker_config(self, settings):
        secret = settings.api_key or settings.local_llm_api_key or ""
        toolsets = [part for part in settings.tui_toolsets().split(",") if part]
        payload = HermesPrototype.__new__(HermesPrototype)._hermes_config_payload(settings)
        return {
            "model": settings.model,
            "provider": settings.gateway_provider(),
            "base_url": settings.local_llm_base_url,
            "api_key_set": bool(secret),
            "context_window": settings.local_llm_context_window,
            "max_output": settings.local_llm_max_output,
            "reasoning_effort": settings.reasoning_effort,
            "fast": settings.fast,
            "approvals_mode": settings.approvals_mode,
            "toolsets": toolsets,
            "hermes_config": self._redact_config(payload),
        }

    def _update_worker_profile(self, profile, arguments):
        for key in ("model", "provider", "base_url", "api_key", "reasoning_effort", "approvals_mode"):
            if key in arguments:
                profile[key] = ext_str(key, arguments.get(key))
        if "context_window" in arguments:
            profile["context_window"] = ext_int("context_window", arguments.get("context_window"))
        if "max_output" in arguments:
            profile["max_output"] = ext_int("max_output", arguments.get("max_output"))
        if "fast" in arguments:
            profile["fast"] = ext_bool("fast", arguments.get("fast"))
        if "toolsets" in arguments:
            raw = arguments.get("toolsets")
            if isinstance(raw, str):
                raw = [part.strip() for part in raw.split(",") if part.strip()]
            items = ext_list("toolsets", raw)
            profile["toolsets"] = [ext_str("toolsets[]", item) for item in items if ext_str("toolsets[]", item)]
        if "config" in arguments:
            overlay = ext_dict("config", arguments.get("config"))
            if ext_bool("replace_config", arguments.get("replace_config")):
                profile["config"] = overlay
            else:
                current = profile.get("config") if isinstance(profile.get("config"), dict) else {}
                profile["config"] = merge_nested(current, overlay)

    async def hermes_config(self, arguments):
        self._init_hermes_supervisor()
        changing = any(key in arguments for key in _CONFIG_FIELDS)
        if not changing:
            return {
                "ok": True,
                "running": self._gateway_running(),
                "config": self._public_worker_config(self._worker_settings()),
            }
        force = ext_bool("force", arguments.get("force"))
        if self._gateway_running() and self._running_tasks() and not force:
            return {"ok": False, "error": "interrupt running hermes tasks first, or pass force=true"}
        async with self._hermes_lock:
            profile = self._load_worker_profile()
            self._update_worker_profile(profile, arguments)
            self._save_worker_profile(profile)
            restarted = False
            start_error = ""
            if self._gateway_running() or force:
                if force:
                    for task in self._hermes_tasks.values():
                        if task.status in {"running", "needs_approval"}:
                            task.status = "failed"
                            task.error = "hermes config changed"
                            task.wake_for = task.status
                            task.wake_pending = False
                await self._stop_hermes_process()
                try:
                    await self._start_hermes_gateway()
                    restarted = True
                except (RuntimeError, OSError, FileNotFoundError, asyncio.TimeoutError) as exc:
                    start_error = str(exc)
            settings = self._hermes_settings or self._worker_settings()
        body = {
            "ok": not start_error,
            "restarted": restarted,
            "running": self._gateway_running(),
            "config": self._public_worker_config(settings),
        }
        if start_error:
            body["saved"] = True
            body["error"] = start_error
        return body

    async def hermes_dispatch(self, name, arguments):
        self._init_hermes_supervisor()
        arguments = ext_dict("arguments", arguments if arguments is not None else {})
        try:
            if name == "hermes_delegate":
                return await self.hermes_delegate(arguments)
            if name == "hermes_status":
                return await self.hermes_status(arguments)
            if name == "hermes_steer":
                return await self.hermes_steer(arguments)
            if name == "hermes_interrupt":
                return await self.hermes_interrupt(arguments)
            if name == "hermes_approve":
                return await self.hermes_approve(arguments)
            if name == "hermes_result":
                return await self.hermes_result(arguments)
            if name == "hermes_config":
                return await self.hermes_config(arguments)
        except ExternalError as exc:
            return {"ok": False, "error": str(exc)}
        except (RuntimeError, FileNotFoundError, OSError) as exc:
            logger.error("grok hermes tool failed name=%s error=%s", name, exc)
            return {"ok": False, "error": str(exc)}
        return {"ok": False, "error": "unknown tool"}

    async def _close_hermes_supervisor(self):
        if not getattr(self, "_hermes_ready", False):
            return
        drains = list(self._hermes_drains.values())
        self._hermes_drains = {}
        for task in drains:
            task.cancel()
        if drains:
            await asyncio.gather(*drains, return_exceptions=True)
        gateway = self._hermes_gateway
        self._hermes_gateway = None
        if gateway is not None:
            await gateway.close()

    async def stop(self):
        await self._close_hermes_supervisor()
        return await super().stop()
