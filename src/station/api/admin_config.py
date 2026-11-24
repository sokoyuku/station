import os
import re
from pathlib import Path

from aiohttp import web

from station.api.http import read_json_object
from station.api.token import _require_admin
from station.config.reload import apply_reloaded_config, reload_from_disk
from station.errors import ExternalError
from station.prototypes.boundary import ext_bool, ext_dict, ext_int, ext_list, ext_str

# key, group, type, apply, secret
# apply: live (reload), restart (saved, process keeps the old value), env (file + os.environ)
_FIELDS = (
    ("ENV", "Process", "str", "restart", False),
    ("SERVER_HOST", "Process", "str", "restart", False),
    ("SERVER_PORT", "Process", "int", "restart", False),
    ("STATION_APP", "Process", "bool", "restart", False),
    ("FS_ROOT", "Process", "str", "restart", False),
    ("CONSUL_URL", "Process", "str", "live", False),
    ("STATION_ADMIN_TOKEN", "Process", "str", "live", True),
    ("CONSUL_USERNAME", "Process", "str", "restart", False),
    ("CONSUL_PASSWORD", "Process", "str", "restart", True),
    ("DB_BACKEND", "Database", "str", "restart", False),
    ("DB_PATH", "Database", "str", "restart", False),
    ("DB_DSN", "Database", "str", "restart", True),
    ("WIPE_ON_RESTART", "Database", "bool", "restart", False),
    ("STATION_INBOUND_WORKERS", "Server", "int", "restart", False),
    ("STATION_INBOUND_QUEUE", "Server", "int", "restart", False),
    ("STATION_INBOUND_BATCH", "Server", "int", "restart", False),
    ("STATION_INBOUND_AT_LEAST_ONCE", "Server", "bool", "restart", False),
    ("STATION_OUTBOUND_CONCURRENCY", "Server", "int", "restart", False),
    ("STATION_LISTEN_BACKLOG", "Server", "int", "restart", False),
    ("REQUEST_DEDUPE_TTL_SECONDS", "Server", "int", "restart", False),
    ("DEDUPE_CLEANUP_INTERVAL_SECONDS", "Server", "int", "restart", False),
    ("STATION_INSTANCE_ISOLATE_AFTER", "Server", "int", "live", False),
    ("STATION_OUTBOUND_RETRY_ATTEMPTS", "Server", "int", "live", False),
    ("STATION_OUTBOUND_RETRY_BASE_SECONDS", "Server", "float", "live", False),
    ("STATION_OUTBOUND_CIRCUIT_FAILURES", "Server", "int", "live", False),
    ("STATION_OUTBOUND_CIRCUIT_COOLDOWN_SECONDS", "Server", "float", "live", False),
    ("TELEGRAM_API_URL", "Platforms", "str", "live", False),
    ("TELEGRAM_WEBHOOK_MODE", "Platforms", "bool", "live", False),
    ("TELEGRAM_WEBHOOK_SECRET", "Platforms", "str", "live", True),
    ("DISCORD_API_URL", "Platforms", "str", "live", False),
    ("WHATSAPP_CLOUD_API_URL", "Platforms", "str", "live", False),
    ("WHATSAPP_WS_URL", "Platforms", "str", "live", False),
    ("WHATSAPP_CERT_URL", "Platforms", "str", "live", False),
    ("QQ_API_URL", "Platforms", "str", "live", False),
    ("QQ_TOKEN_URL", "Platforms", "str", "live", False),
    ("LOCAL_CONDUCTOR_PRIVATE_KEY_PATH", "Local conductor", "str", "restart", False),
    ("LOCAL_CONDUCTOR_PUBLIC_KEY_PATH", "Local conductor", "str", "restart", False),
    ("LOCAL_CONDUCTOR_ADDRESS", "Local conductor", "str", "restart", False),
    ("LOCAL_CONDUCTOR_WHATSAPP_BRIDGE_DIR", "Local conductor", "str", "env", False),
    ("LOCAL_CONDUCTOR_WHATSAPP_SESSION_DIR", "Local conductor", "str", "env", False),
    ("LOCAL_CONDUCTOR_NODE", "Local conductor", "str", "env", False),
    ("LOG_DIR_PATH", "Logs", "str", "restart", False),
    ("LOG_LEVEL", "Logs", "str", "live", False),
    ("LOG_FORMAT", "Logs", "str", "restart", False),
    ("LOG_MAX_BYTES", "Logs", "int", "restart", False),
    ("LOG_BACKUP_COUNT", "Logs", "int", "restart", False),
    ("SMOOTHING_WINDOW", "Logs", "int", "live", False),
)

_FIELD_BY_KEY = {row[0]: row for row in _FIELDS}
_PROTOTYPE_KEYS = ("name", "kind", "config_file", "secret_file", "ava", "reply_to")
_LIVE_PROTOTYPE_KEYS = ("ava", "reply_to")


def _running_value(config, key):
    server = config.server
    database = config.database
    recorder = config.recorder
    local = config.local_conductor
    values = {
        "ENV": config.env,
        "SERVER_HOST": config.host or "",
        "SERVER_PORT": config.port,
        "STATION_APP": config.station_app,
        "FS_ROOT": config.fs_root,
        "CONSUL_URL": config.consul_url,
        "STATION_ADMIN_TOKEN": config.admin_token,
        "DB_BACKEND": database.backend,
        "DB_PATH": database.path,
        "DB_DSN": database.dsn or "",
        "WIPE_ON_RESTART": database.wipe_on_restart,
        "STATION_INBOUND_WORKERS": server.inbound_workers,
        "STATION_INBOUND_QUEUE": server.inbound_queue,
        "STATION_INBOUND_BATCH": server.inbound_batch,
        "STATION_INBOUND_AT_LEAST_ONCE": server.inbound_at_least_once,
        "STATION_OUTBOUND_CONCURRENCY": server.outbound_concurrency,
        "STATION_LISTEN_BACKLOG": server.listen_backlog,
        "REQUEST_DEDUPE_TTL_SECONDS": server.request_dedupe_ttl_seconds,
        "DEDUPE_CLEANUP_INTERVAL_SECONDS": server.dedupe_cleanup_interval_seconds,
        "STATION_INSTANCE_ISOLATE_AFTER": server.isolate_after,
        "STATION_OUTBOUND_RETRY_ATTEMPTS": server.outbound_retry_attempts,
        "STATION_OUTBOUND_RETRY_BASE_SECONDS": server.outbound_retry_base_seconds,
        "STATION_OUTBOUND_CIRCUIT_FAILURES": server.outbound_circuit_failures,
        "STATION_OUTBOUND_CIRCUIT_COOLDOWN_SECONDS": server.outbound_circuit_cooldown_seconds,
        "TELEGRAM_API_URL": config.telegram_api_url,
        "TELEGRAM_WEBHOOK_MODE": config.telegram_webhook_mode,
        "TELEGRAM_WEBHOOK_SECRET": config.telegram_webhook_secret,
        "DISCORD_API_URL": config.discord_api_url,
        "WHATSAPP_CLOUD_API_URL": config.whatsapp_cloud_api_url,
        "WHATSAPP_WS_URL": config.whatsapp_ws_url,
        "WHATSAPP_CERT_URL": config.whatsapp_cert_url,
        "QQ_API_URL": config.qq_api_url,
        "QQ_TOKEN_URL": config.qq_token_url,
        "LOCAL_CONDUCTOR_PRIVATE_KEY_PATH": local.private_key_path,
        "LOCAL_CONDUCTOR_PUBLIC_KEY_PATH": local.public_key_path,
        "LOCAL_CONDUCTOR_ADDRESS": local.address,
        "LOG_DIR_PATH": recorder.dir_path or "",
        "LOG_LEVEL": recorder.level,
        "LOG_FORMAT": recorder.format,
        "LOG_MAX_BYTES": recorder.max_bytes,
        "LOG_BACKUP_COUNT": recorder.backup_count,
        "SMOOTHING_WINDOW": recorder.smoothing_window,
    }
    if key in values:
        value = values[key]
        return "" if value is None else value
    if key in config.data:
        return config.data[key]
    return os.environ.get(key, "")


def _file_value(config, key, running):
    if key in config.data:
        return config.data[key]
    return running


def _is_set(value):
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _same(left, right):
    if isinstance(left, float) or isinstance(right, float):
        try:
            return float(left) == float(right)
        except (TypeError, ValueError):
            return False
    return left == right


def _pending(apply, stored, running, key):
    if apply == "live":
        return False
    path_key = key.endswith("_PATH") or key in ("FS_ROOT", "DB_PATH", "LOG_DIR_PATH")
    if path_key:
        left = str(stored or "").strip()
        right = str(running or "").strip()
        if not left or not right:
            return left != right
        return os.path.abspath(left) != os.path.abspath(right)
    return not _same(stored, running)


def _field_row(config, spec):
    key, group, kind, apply, secret = spec
    running = _running_value(config, key)
    stored = _file_value(config, key, running)
    row = {
        "key": key,
        "group": group,
        "type": kind,
        "apply": apply,
        "secret": secret,
        "pending": _pending(apply, stored, running, key),
    }
    if secret:
        row["set"] = _is_set(stored) or _is_set(running)
        row["value"] = ""
        row["running"] = ""
    else:
        row["value"] = stored
        row["running"] = running
        row["set"] = _is_set(stored) or _is_set(running)
    return row


def _running_prototype(config, prototype_id):
    for item in config.hosted_prototypes:
        if item.id == prototype_id:
            return item
    return None


def _prototype_pending(config, entry):
    running = _running_prototype(config, entry.get("id"))
    if running is None:
        return True
    kind = str(entry.get("kind") or "station")
    config_file = str(entry.get("config_file") or "")
    secret_file = str(entry.get("secret_file") or "")
    name = str(entry.get("name") or "")
    return (
        name != (getattr(running, "name", "") or "")
        or kind != (running.kind or "station")
        or _pending("restart", config_file, running.config_file or "", "config_file")
        or _pending("restart", secret_file, running.secret_file or "", "secret_file")
    )


def _prototypes(config):
    raw = config.data.get("prototypes") or []
    rows = []
    if not isinstance(raw, list):
        return rows
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        prototype_id = entry.get("id")
        rows.append(
            {
                "id": prototype_id,
                "name": str(entry.get("name") or ""),
                "kind": str(entry.get("kind") or "station"),
                "config_file": str(entry.get("config_file") or ""),
                "secret_file": str(entry.get("secret_file") or ""),
                "ava": bool(entry.get("ava")),
                "reply_to": bool(entry.get("reply_to")),
                "token_set": bool(str(entry.get("token") or "").strip()),
                "pending": _prototype_pending(config, entry),
            }
        )
    return rows


def _credentials(config):
    username = str(config.data.get("CONSUL_USERNAME") or os.environ.get("CONSUL_USERNAME") or "")
    password = str(config.data.get("CONSUL_PASSWORD") or os.environ.get("CONSUL_PASSWORD") or "")
    return {
        "consul_username_set": bool(username.strip()),
        "consul_password_set": bool(password.strip()),
    }


async def handle_admin_get_config(request):
    denied = _require_admin(request)
    if denied is not None:
        return denied
    config = request.app["config"]
    data = {
        "config_file": config.config_file_path or "",
        "secret_file": config.secret_file_path or "",
        "fields": [_field_row(config, spec) for spec in _FIELDS],
        "prototypes": _prototypes(config),
        "credentials": _credentials(config),
    }
    return web.json_response({"result": 0, "msg": "ok", "data": data})


def _root_end(lines):
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("[") and not stripped.startswith("#"):
            return index
    return len(lines)


def _file_has_root_key(path, key):
    if not path or not os.path.isfile(path):
        return False
    lines = Path(path).read_text(encoding="utf-8").splitlines(keepends=True)
    pattern = re.compile(r"^\s*%s\s*=" % re.escape(key))
    for line in lines[: _root_end(lines)]:
        if pattern.match(line):
            return True
    return False


def upsert_root_key(path, key, value):
    from station.runner import _toml_literal

    file_path = Path(path)
    text = file_path.read_text(encoding="utf-8") if file_path.is_file() else ""
    lines = text.splitlines(keepends=True)
    end = _root_end(lines)
    pattern = re.compile(r"^\s*%s\s*=" % re.escape(key))
    literal = _toml_literal(value)
    for index in range(end):
        if pattern.match(lines[index]):
            ending = "\r\n" if lines[index].endswith("\r\n") else "\n"
            lines[index] = "%s = %s%s" % (key, literal, ending)
            file_path.write_text("".join(lines), encoding="utf-8")
            return
    insert = "%s = %s\n" % (key, literal)
    lines.insert(end, insert)
    file_path.write_text("".join(lines), encoding="utf-8")


def _destination(config, key):
    secret = config.secret_file_path
    if secret and _file_has_root_key(secret, key):
        return secret
    path = config.config_file_path
    if not path:
        raise ExternalError("Config file path not available")
    return path


def _coerce(key, kind, value):
    if kind == "bool":
        return ext_bool(key, value)
    if kind == "int":
        return ext_int(key, value)
    if kind == "float":
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            raise ExternalError("%s must be a number" % key)
        try:
            return float(value)
        except ValueError:
            raise ExternalError("%s must be a number" % key)
    if value is None:
        return ""
    return ext_str(key, value, default="")


def update_prototype_table(path, prototype_id, updates):
    from station.runner import _apply_kv_updates_to_table_lines, _find_prototypes_table_ranges

    file_path = Path(path)
    lines = file_path.read_text(encoding="utf-8").splitlines(keepends=True)
    ranges = _find_prototypes_table_ranges(lines)
    id_pattern = re.compile(r"^\s*id\s*=\s*(\d+)\s*(#.*)?$")
    for start, end in ranges:
        found = False
        for index in range(start + 1, end):
            match = id_pattern.match(lines[index])
            if match and int(match.group(1)) == prototype_id:
                found = True
                break
        if not found:
            continue
        lines, _end = _apply_kv_updates_to_table_lines(lines, start, end, updates)
        file_path.write_text("".join(lines), encoding="utf-8")
        return True
    return False


def _prototype_updates(entry):
    entry = ext_dict("prototypes", entry)
    prototype_id = ext_int("id", entry.get("id"))
    updates = {}
    if "name" in entry:
        updates["name"] = ext_str("name", entry.get("name"), default="")
    if "kind" in entry:
        kind = ext_str("kind", entry.get("kind"), default="").strip()
        if not kind:
            raise ExternalError("kind is required")
        updates["kind"] = kind
    if "config_file" in entry:
        updates["config_file"] = ext_str("config_file", entry.get("config_file"), default="")
    if "secret_file" in entry:
        updates["secret_file"] = ext_str("secret_file", entry.get("secret_file"), default="")
    if "ava" in entry:
        updates["ava"] = ext_bool("ava", entry.get("ava"))
    if "reply_to" in entry:
        updates["reply_to"] = ext_bool("reply_to", entry.get("reply_to"))
    unknown = set(entry) - set(_PROTOTYPE_KEYS) - {"id"}
    if unknown:
        raise ExternalError("Unknown prototype field")
    return prototype_id, updates


async def handle_admin_set_config(request):
    denied = _require_admin(request)
    if denied is not None:
        return denied
    body = await read_json_object(request)
    values = body.get("values") or {}
    if values is None:
        values = {}
    values = ext_dict("values", values)
    prototypes = body.get("prototypes") or []
    if prototypes:
        prototypes = ext_list("prototypes", prototypes)
    if not values and not prototypes:
        raise ExternalError("Nothing to update")

    config = request.app["config"]
    written = []
    env_keys = []
    for key, raw in values.items():
        spec = _FIELD_BY_KEY.get(key)
        if spec is None:
            raise ExternalError("Unknown config key")
        _key, _group, kind, apply, secret = spec
        if secret and (raw is None or (isinstance(raw, str) and not raw.strip())):
            continue
        value = _coerce(key, kind, raw)
        if key == "LOG_LEVEL":
            level = str(value).strip().upper()
            if level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
                raise ExternalError("Invalid log level")
            value = level
        if key == "DB_BACKEND" and str(value).strip().lower() not in ("sqlite", "postgres"):
            raise ExternalError("DB_BACKEND must be sqlite or postgres")
        path = _destination(config, key)
        upsert_root_key(path, key, value)
        written.append(key)
        if apply == "env":
            os.environ[key] = "" if value is None else str(value)
            env_keys.append(key)

    prototype_ids = []
    restart_prototypes = []
    config_path = config.config_file_path
    for entry in prototypes:
        prototype_id, updates = _prototype_updates(entry)
        if not updates:
            continue
        if not config_path:
            raise ExternalError("Config file path not available")
        if not update_prototype_table(config_path, prototype_id, updates):
            raise ExternalError("prototype_id=%s is not in the config file" % prototype_id)
        prototype_ids.append(prototype_id)
        if any(key not in _LIVE_PROTOTYPE_KEYS for key in updates):
            restart_prototypes.append(prototype_id)

    result = reload_from_disk(request.app["config"])
    apply_reloaded_config(request.app, result)
    pending = list(result["pending_restart"])
    if restart_prototypes:
        pending.append("prototypes")
    if env_keys:
        pending.extend(env_keys)
    return web.json_response(
        {
            "result": 0,
            "msg": "Config saved",
            "data": {
                "written": written,
                "prototypes": prototype_ids,
                "applied": result["applied"],
                "pending_restart": pending,
                "unchanged": len(result["applied"]) == 0 and not written and not prototype_ids,
            },
        }
    )
