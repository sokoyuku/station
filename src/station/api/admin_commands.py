import contextlib
import io
import os
from pathlib import Path

import aiohttp

from aiohttp import web

from station.api.http import read_json_object
from station.api.token import _require_admin
from station.client.retry import client_session
from station.conductor.rekey import rekey_local_secrets
from station.errors import ExternalError
from station.prototypes.boundary import ext_bool, ext_int, ext_str
from station.timeouts import http_timeout


def consul_credentials(config):
    username = str(config.data.get("CONSUL_USERNAME") or os.environ.get("CONSUL_USERNAME") or "").strip()
    password = str(config.data.get("CONSUL_PASSWORD") or os.environ.get("CONSUL_PASSWORD") or "").strip()
    if not username or not password:
        raise ExternalError("CONSUL_USERNAME and CONSUL_PASSWORD are required")
    return username, password


async def handle_admin_sync_tokens(request):
    denied = _require_admin(request)
    if denied is not None:
        return denied
    body = await read_json_object(request)
    grace_seconds = ext_int("grace_seconds", body.get("grace_seconds", 300))
    if grace_seconds < 0:
        raise ExternalError("Invalid grace_seconds")
    prototype_id = body.get("prototype_id")
    if prototype_id is not None:
        prototype_id = ext_int("prototype_id", prototype_id)

    app = request.app
    from station.runner import (
        _apply_kv_updates_to_prototypes_table_by_name,
        _collect_named_prototypes,
        _ensure_prototype,
        _payload_from_prototype_entry,
        _sign_in,
    )

    config = app["config"]
    username, password = consul_credentials(config)
    consul_url = (config.consul_url or "").strip()
    if not consul_url:
        raise ExternalError("CONSUL_URL is required")
    named = _collect_named_prototypes(config.data)
    if prototype_id is not None:
        named = [entry for entry in named if entry.get("id") == prototype_id]
    if not named:
        raise ExternalError("No named prototypes to sync")

    config_path = config.config_file_path
    if not config_path:
        raise ExternalError("Config file path not available")
    registry = app["tenants"]
    text = Path(config_path).read_text(encoding="utf-8")
    original = text
    updated = []
    cookie_jar = aiohttp.CookieJar(unsafe=True)
    async with client_session(cookie_jar=cookie_jar, timeout=http_timeout(), trust_env=False) as session:
        await _sign_in(session=session, consul_url=consul_url, username=username, password=password)
        for entry in named:
            name = ext_str("name", entry.get("name"))
            payload = {"name": name}
            payload.update(_payload_from_prototype_entry(entry))
            try:
                ensured = await _ensure_prototype(session=session, consul_url=consul_url, payload=payload)
            except RuntimeError as exc:
                raise ExternalError(str(exc)) from exc
            token = ext_str("token", ensured.get("token"), default="")
            if not token:
                raise ExternalError("ensure_prototype returned empty token")
            ensured_id = ext_int("prototype_id", ensured.get("prototype_id"))
            await registry.rotate_token(ensured_id, token, grace_seconds=grace_seconds)
            for cfg in config.hosted_prototypes:
                if cfg.id == ensured_id:
                    cfg.token = token
            text = _apply_kv_updates_to_prototypes_table_by_name(
                text,
                name,
                {"id": ensured_id, "token": token, "name": name},
            )
            updated.append({"prototype_id": ensured_id, "name": name})
    if text != original:
        Path(config_path).write_text(text, encoding="utf-8")
    return web.json_response(
        {
            "result": 0,
            "msg": "Tokens synced",
            "data": {"prototypes": updated, "grace_seconds": grace_seconds},
        }
    )


async def handle_admin_rekey(request):
    denied = _require_admin(request)
    if denied is not None:
        return denied
    body = await read_json_object(request)
    list_only = ext_bool("list_only", body.get("list_only", False))
    old_private_key = ext_str("old_private_key", body.get("old_private_key"), default="")
    config = request.app["config"]
    username, password = consul_credentials(config)
    buffer = io.StringIO()
    try:
        with contextlib.redirect_stdout(buffer):
            await rekey_local_secrets(
                config=config,
                username=username,
                password=password,
                old_private_key_path=old_private_key,
                list_only=list_only,
            )
    except ExternalError:
        raise
    except RuntimeError as exc:
        raise ExternalError(str(exc)) from exc
    report = buffer.getvalue().strip()
    return web.json_response(
        {
            "result": 0,
            "msg": "Listed local secrets" if list_only else "Rekey finished",
            "data": {"report": report, "list_only": list_only},
        }
    )
