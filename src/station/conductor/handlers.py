import asyncio
from contextlib import asynccontextmanager
import json
import os
from typing import Any
import uuid

import aiohttp
from aiohttp import web
from aiohttp.web_request import FileField
from cryptography.hazmat.primitives.asymmetric import rsa

from station.api.http import ext_path_int, read_json_object
from station.client.consul import CONSUL_TIMEOUT, fetch_prototype
from station.client.retry import RetryableError, is_retryable_error, is_transient_status, retry_transient
from station.database.consul_state import apply_consul_model, apply_consul_prototype, version_is_stale
from station.api.reception import is_slash_command
from station.conductor.account_status import AccountUnreachable, check_local_account
from station.conductor.crypto import default_private_key_path, decrypt_if_encrypted, load_private_key, public_key_token, write_private_key
from station.conductor.platform_types import (
    is_qr_account_type,
    strip_qr_prefix,
    validate_local_platform_type,
)
from station.conductor.platforms import LocalPlatformAdapter, build_builtin_platforms
from station.conductor.platforms.chat_type import station_chat_type
from station.conductor.platforms.group_settings import controls_group_messages, ensure_group_notice
from station.conductor.otp import classify_start_arg, start_otp_token
from station.conductor.pair import PairManager
from station.conductor.util import constant_time_equal, err, ext_bool, ext_int, ext_str, normalize_http_url, ok, catch_external
from station.errors import ExternalError
from station import logger
from station.prototypes.boundary import ext_dict, ext_list

def _chat_keys(chats):
    out = set()
    for chat in chats:
        if not isinstance(chat, dict):
            continue
        out.add((str(chat.get("acct_id") or ""), str(chat.get("chat_id") or "")))
    return out

def _norm_acct(value):
    return str(value or "").replace("-", "").strip().lower()

_MEDIA_METHODS = {
    "send_photo",
    "send_video",
    "send_audio",
    "send_document",
    "send_voice",
    "send_animation",
    "send_sticker",
}

_SUPPORT_NOTE = " You can reach this prototype's support for support. It is shown in the prototype details."
_MODEL_UNAVAILABLE = "This model isn't available. Please check the model and try again."
_COULD_NOT_REACH = "Couldn't reach the model. Please send your message again to retry."
_MODEL_INTERNAL = "The model had an internal error. Please send your message again to retry." + _SUPPORT_NOTE
_MODEL_BUSY = "This model is busy right now. Please send your message again to retry."
_MAX_CHATS = "Max chats reached."
_ADD_CHAT_ERROR = "Error adding chat."


class ChatMapping:
    def __init__(self, model_id: str = "", settled: bool = False):
        self.model_id = model_id or ""
        self.settled = settled

class LocalConductor:
    def __init__(self, app: web.Application):
        self.app = app
        self.db = app["db"]
        self.config = app["config"]
        self.session: aiohttp.ClientSession = app["session"]

        lc = self.config.local_conductor
        self.private_key_path = lc.private_key_path.strip() if lc.private_key_path else ""
        if not self.private_key_path:
            self.private_key_path = default_private_key_path(self.config.database.path)
        self.public_key_path = lc.public_key_path.strip() if lc.public_key_path else ""
        if not self.public_key_path:
            self.public_key_path = self.private_key_path + ".pub"

        self.private_key: rsa.RSAPrivateKey | None = None
        self.public_key_token: str = ""

        self.poller_stop = asyncio.Event()
        self._poller_tasks: dict[str, asyncio.Task] = {}
        self._poller_task_types: dict[str, str] = {}
        self._poller_reconciler: asyncio.Task | None = None

        self.platforms: dict[str, LocalPlatformAdapter] = build_builtin_platforms(self)
        self.pair_manager = PairManager(self)
        self._user_sync_lock = asyncio.Lock()
        self._consul_write_holds: dict[str, int] = {}

    def require_own_prototype(self, prototype_id: int) -> web.Response | None:
        registry = self.app["tenants"]
        if not registry.hosts(prototype_id):
            return err("Prototype mismatch", status=404)
        return None

    def conductor_address(self) -> str:
        configured = self.config.local_conductor.address.strip().rstrip("/") if self.config.local_conductor.address else ""
        if configured:
            if not configured.lower().startswith("http://") and not configured.lower().startswith("https://"):
                configured = "http://" + configured
            return configured
        host = self.config.host.strip() if self.config.host else "127.0.0.1"
        port = self.config.port if self.config.port is not None else 0
        return f"http://{host}:{port}".rstrip("/")

    def _consul_write_held(self, key) -> bool:
        return _norm_acct(key) in self._consul_write_holds

    @asynccontextmanager
    async def consul_write(self, *keys):
        held = []
        for key in keys:
            norm = _norm_acct(key)
            if not norm:
                continue
            self._consul_write_holds[norm] = self._consul_write_holds.get(norm, 0) + 1
            held.append(norm)
        try:
            yield
        finally:
            for norm in held:
                left = self._consul_write_holds.get(norm, 0) - 1
                if left <= 0:
                    self._consul_write_holds.pop(norm, None)
                else:
                    self._consul_write_holds[norm] = left

    def _model_assigned_here(self, remote: dict) -> bool:
        addr = normalize_http_url(str(remote.get("conductor_address") or ""))
        if not addr:
            return True
        return addr == normalize_http_url(self.conductor_address())

    def _stop_account_poller(self, acct_id: str) -> None:
        raw = str(acct_id or "").strip()
        if not raw:
            return
        for key in {raw, raw.replace("-", "")}:
            task = self._poller_tasks.pop(key, None)
            self._poller_task_types.pop(key, None)
            if task is not None:
                task.cancel()

    async def refresh_suspect_account(self, acct_id: str) -> None:
        acct_id = str(acct_id or "").strip()
        if not acct_id or self._consul_write_held(acct_id):
            return
        acct = await self.db.get_local_account(acct_id)
        if acct is None:
            compact = acct_id.replace("-", "")
            if compact != acct_id:
                acct = await self.db.get_local_account(compact)
        if not acct:
            return
        model_id = str(acct.get("model_id") or "").strip()
        if not model_id or self._consul_write_held(model_id):
            return
        try:
            await self._reconcile_model_from_consul(
                model_id=model_id,
                prototype_id=acct.get("prototype_id"),
                respect_write_hold=True,
            )
        except ExternalError as exc:
            if exc.status == 404 and not self._consul_write_held(acct_id) and not self._consul_write_held(model_id):
                logger.info("consul model missing model_id=%s; dropping local", model_id)
                await self._remove_local_model(model_id)
                return
            logger.error("consul refresh failed acct_id=%s model_id=%s error=%s", acct_id, model_id, exc)

    def get_platform(self, acct_type: str | None) -> LocalPlatformAdapter | None:
        normalized = strip_qr_prefix(acct_type)
        if not normalized:
            return None
        return self.platforms.get(normalized)

    def _is_session_bound_account(self, acct: dict | None) -> bool:
        if not acct:
            return False
        if is_qr_account_type(acct.get("acct_type")):
            return True
        return strip_qr_prefix(acct.get("acct_type")) == "whatsapp"

    async def _resolve_local_account_context(
        self,
        acct_id: str,
    ) -> tuple[dict | None, str | None, LocalPlatformAdapter | None]:
        acct = await self.db.get_local_account(acct_id)
        if not acct:
            return None, None, None
        adapter = self.get_platform(acct.get("acct_type"))
        server_raw = acct.get("server")
        if server_raw is None:
            server_raw = ""
        server_raw = server_raw.strip()
        if server_raw:
            server_plain, _ = decrypt_if_encrypted(self.private_key, server_raw)
            plain = server_plain.strip() if server_plain else ""
            acct = {**acct, "server": plain}
        if self._is_session_bound_account(acct):
            tok = acct.get("encrypted_token")
            if tok is None:
                tok = ""
            tok = tok.strip()
            return acct, tok if tok else "session", adapter
        token = await self._decrypt_acct_token(acct)
        return acct, token, adapter

    async def _start_platform_poller(
        self,
        *,
        acct_id: str,
        adapter: LocalPlatformAdapter,
    ) -> None:
        await adapter.ensure_account_ready(acct_id)
        await adapter.poller_loop(acct_id)

    async def _dispatch_platform_method(
        self,
        *,
        adapter: LocalPlatformAdapter | None,
        acct: dict,
        token: str,
        method: str,
        chat_id: str,
        payload: dict,
        file_bytes: bytes | None,
    ) -> web.Response:
        acct_type = acct.get("acct_type")
        if acct_type is None:
            acct_type = ""
        acct_type = acct_type.strip().lower()
        if adapter is None:
            return err(f"No adapter registered for {acct_type}", status=501)
        payload.pop("parse_mode", None)
        payload.pop("format", None)

        try:
            if method == "send_typing":
                if not adapter.supports("send_typing"):
                    return err("Platform does not support send_typing", status=501)
                typing_kwargs = {"acct": acct, "token": token, "chat_id": chat_id}
                ok_ = await adapter.send_typing(**typing_kwargs)
                return ok({"ok": ok_}) if ok_ else err("send_typing failed", status=502)

            if method == "download_file":
                file_id = ext_str(payload.get("file_id"), "file_id").strip()
                if not file_id:
                    return err("Missing file_id", status=400)
                if not adapter.supports("download_file"):
                    return err("Platform does not support download_file", status=501)
                blob = await adapter.download_file(acct=acct, token=token, file_id=file_id)
                if blob is None:
                    return err("download failed", status=502)
                return web.Response(body=blob, status=200)

            if method == "send_message":
                if not adapter.supports("send_message"):
                    return err("Platform does not support send_message", status=501)
                text = ext_str(payload.get("text"), "text").strip()
                reply_to = ext_str(payload.get("reply_to"), "reply_to").strip() or None
                send_kwargs = {
                    "acct": acct,
                    "token": token,
                    "chat_id": chat_id,
                    "text": text,
                    "reply_to": reply_to,
                    "keyboard": payload.get("keyboard"),
                }
                ok_ = await adapter.send_message(**send_kwargs)
                return ok({"ok": ok_}) if ok_ else err("send_message failed", status=502)

            if method == "send_webrtc":
                if not adapter.supports("send_webrtc"):
                    return err("Platform does not support send_webrtc", status=501)
                webrtc_type = ext_str(payload.get("webrtc_type"), "webrtc_type").strip()
                raw_content = payload.get("webrtc_content")
                if raw_content is None:
                    content_obj = {}
                elif type(raw_content) is dict:
                    content_obj = ext_dict("webrtc_content", raw_content)
                elif type(raw_content) is str:
                    try:
                        parsed = json.loads(raw_content)
                    except (json.JSONDecodeError, ValueError):
                        raise ExternalError("Invalid webrtc_content", code="conductor.local.gateway.webrtc_content_json")
                    content_obj = ext_dict("webrtc_content", parsed)
                else:
                    raise ExternalError("Invalid webrtc_content", code="conductor.local.gateway.webrtc_content_type")
                if not webrtc_type:
                    return err("Missing webrtc_type", status=400)
                ok_ = await adapter.send_webrtc(
                    acct=acct,
                    token=token,
                    chat_id=chat_id,
                    webrtc_type=webrtc_type,
                    content=content_obj,
                )
                return ok({"ok": ok_}) if ok_ else err("send_webrtc failed", status=502)

            if method == "send_discord_voice":
                if not adapter.supports("send_discord_voice"):
                    return err("Platform does not support send_discord_voice", status=501)
                action = ext_str(payload.get("action"), "action", default="join").strip().lower() or "join"
                channel_id = ext_str(payload.get("channel_id"), "channel_id").strip() or None
                guild_id = ext_str(payload.get("guild_id"), "guild_id").strip() or None
                self_mute = ext_bool(payload.get("self_mute"), "self_mute")
                self_deaf = ext_bool(payload.get("self_deaf"), "self_deaf")
                ok_ = await adapter.send_discord_voice(
                    acct=acct,
                    token=token,
                    chat_id=chat_id,
                    action=action,
                    channel_id=channel_id,
                    guild_id=guild_id,
                    self_mute=self_mute,
                    self_deaf=self_deaf,
                )
                return ok({"ok": ok_}) if ok_ else err("send_discord_voice failed", status=502)

            if method in _MEDIA_METHODS:
                if not adapter.supports("send_media"):
                    return err("Platform does not support media send", status=501)
                caption = ext_str(payload.get("caption"), "caption").strip()
                reply_to = ext_str(payload.get("reply_to"), "reply_to").strip() or None
                ok_ = await adapter.send_media(
                    acct=acct,
                    token=token,
                    chat_id=chat_id,
                    method=method,
                    payload=payload,
                    file_bytes=file_bytes,
                    caption=caption,
                    reply_to=reply_to,
                )
                return ok({"ok": ok_}) if ok_ else err("send failed", status=502)
        except ExternalError as exc:
            return err(str(exc), status=400)

        return err("Unsupported gateway method", status=501)

    def ensure_keys(self) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(self.private_key_path)), exist_ok=True)
        try:
            os.chmod(os.path.dirname(os.path.abspath(self.private_key_path)), 0o700)
        except OSError as e:
            logger.error("unexpected where=chmod_private_key_dir error=%s", e, exc_info=e)

        if os.path.exists(self.private_key_path):
            key = load_private_key(self.private_key_path)
        else:
            key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            write_private_key(self.private_key_path, key)

        token = public_key_token(key.public_key())
        self.private_key = key
        self.public_key_token = token

        os.makedirs(os.path.dirname(os.path.abspath(self.public_key_path)), exist_ok=True)
        with open(self.public_key_path, "w", encoding="utf-8") as f:
            f.write(token + "\n")
        try:
            os.chmod(self.public_key_path, 0o644)
        except OSError as e:
            logger.error("unexpected where=chmod_public_key error=%s", e, exc_info=e)

    def require_consul_token(self, request: web.Request) -> web.Response | None:
        token = request.headers.get("X-Consul-Token")
        if token is None:
            token = ""
        token = token.strip()
        if not self.public_key_token or not constant_time_equal(token, self.public_key_token):
            return err("Unauthorized", status=401)
        return None

    def require_prototype_token(self, request: web.Request) -> web.Response | None:
        from station.api.tenant_auth import authorize_request
        if authorize_request(request) is None:
            return err("Unauthorized", status=401)
        return None

    async def _prototype_version_ok(self, request: web.Request, prototype_id: int) -> bool:
        raw = request.headers.get("X-Prototype-Version")
        if raw is None or raw == "":
            logger.error("prototype version missing prototype_id=%s code=conductor.local.prototype.version_header_missing", prototype_id)
            raise ExternalError(
                "Synchronize from consul",
                status=490,
                retryable=True,
                code="conductor.local.prototype.version_header_missing",
            )
        try:
            incoming = ext_path_int("X-Prototype-Version", raw)
        except ExternalError as e:
            logger.error(
                "prototype version invalid prototype_id=%s code=conductor.local.prototype.version_header_invalid error=%s",
                prototype_id,
                e,
            )
            raise ExternalError(
                "Synchronize from consul",
                status=490,
                retryable=True,
                code="conductor.local.prototype.version_header_invalid",
            ) from e
        stored = (await self.db.get_prototype_info(prototype_id)) if prototype_id else None
        local = 0
        if stored is not None:
            v = stored.get("version")
            if v is not None:
                local = v
        registry = self.app["tenants"]
        tenant = registry.get(prototype_id)
        ctx = tenant.client_context if tenant is not None else None
        if ctx is not None and ctx.prototype_version is not None:
            local = max(local, ctx.prototype_version)
        if local == incoming:
            return True
        if tenant is None:
            raise ExternalError(
                "Synchronize from consul",
                status=490,
                retryable=True,
                code="conductor.local.prototype.tenant_missing",
            )
        token = tenant.token_state.token
        fetched = await fetch_prototype(
            session=self.session,
            consul_url=self.config.consul_url,
            token=token,
        )
        fetched = await apply_consul_prototype(self.db, fetched, ctx)
        if ctx is not None and ctx.prototype_version is not None:
            local = ctx.prototype_version
        else:
            local = ext_int(fetched.get("version"), "version")
        if local != incoming:
            raise ExternalError(
                "Synchronize from consul",
                status=490,
                retryable=True,
                code="conductor.local.prototype.version_mismatch",
            )
        return True

    async def _model_versions_ok(self, request: web.Request, model_id: str) -> bool:
        raw_model = request.headers.get("X-Model-Version")
        raw_chat = request.headers.get("X-Chat-Version")
        if not raw_model or not raw_chat:
            logger.error("model version missing model_id=%s code=conductor.local.model.version_header_missing", model_id)
            raise ExternalError(
                "Synchronize from consul",
                status=490,
                retryable=True,
                code="conductor.local.model.version_header_missing",
            )
        try:
            incoming_model = ext_path_int("X-Model-Version", raw_model)
            incoming_chat = ext_path_int("X-Chat-Version", raw_chat)
        except ExternalError as e:
            logger.error("model version invalid model_id=%s code=conductor.local.model.version_header_invalid error=%s", model_id, e)
            raise ExternalError(
                "Synchronize from consul",
                status=490,
                retryable=True,
                code="conductor.local.model.version_header_invalid",
            ) from e
        local = await self.db.get_model(model_id)
        local_model = int((local or {}).get("version") or 0)
        local_chat = int((local or {}).get("chat_version") or 0)
        if local_model >= incoming_model and local_chat >= incoming_chat:
            return True
        if local_model < incoming_model or local_chat < incoming_chat:
            remote = await self.fetch_model_from_consul(model_id=model_id)
            remote_settings = self._decrypt_model_settings(remote.get("settings"))
            if remote_settings is None:
                raise ExternalError("Model settings decrypt failed", status=500)
            remote = dict(remote)
            remote["settings"] = remote_settings
            if not self._model_version_stale(local, remote):
                await apply_consul_model(self.db, model_id, remote)
            chats, chat_version = await self.fetch_model_chats_from_consul(model_id=model_id)
            prototype_id = remote.get("prototype_id")
            if prototype_id is not None:
                prototype_id = ext_int(prototype_id, "prototype_id")
            await self._apply_chats_if_current(
                model_id=model_id,
                prototype_id=prototype_id,
                chats=chats,
                chat_version=chat_version,
            )
            local = await self.db.get_model(model_id)
            local_model = int((local or {}).get("version") or 0)
            local_chat = int((local or {}).get("chat_version") or 0)
        if local_model < incoming_model or local_chat < incoming_chat:
            raise ExternalError(
                "Synchronize from consul",
                status=490,
                retryable=True,
                code="conductor.local.model.version_mismatch",
            )
        return True

    async def _consul_request(self, *, method: str, path: str, payload: dict | None = None, prototype_token: str | None = None) -> dict:
        consul_url = self.config.consul_url.rstrip("/") if self.config.consul_url else ""
        if not consul_url:
            logger.error("consul request missing url path=%s code=conductor.local.consul.url_missing", path)
            raise ExternalError(
                "CONSUL_URL is required",
                status=500,
                reason="consul_unavailable",
                code="conductor.local.consul.url_missing",
            )
        url = "%s%s" % (consul_url, path)
        headers = {"Content-Type": "application/json"}
        if self.public_key_token:
            headers["X-Conductor-Token"] = self.public_key_token
        if prototype_token:
            headers["X-Prototype-Token"] = prototype_token
        async def _once():
            async with self.session.request(method, url, json=payload, headers=headers, timeout=CONSUL_TIMEOUT) as resp:
                try:
                    body = await resp.json(content_type=None)
                except (aiohttp.ContentTypeError, json.JSONDecodeError, ValueError) as e:
                    raise ExternalError(
                        "consul invalid json path=%s status=%s" % (path, resp.status),
                        status=502,
                        reason="consul_invalid_json",
                        code="conductor.local.consul.invalid_json",
                        retryable=False,
                    ) from e
                if is_transient_status(resp.status):
                    raise RetryableError("consul status=%s path=%s" % (resp.status, path))
                if resp.status != 200:
                    upstream_code = ""
                    upstream_msg = ""
                    if isinstance(body, dict):
                        upstream_code = str(body.get("code") or "").strip()
                        upstream_msg = str(body.get("msg") or "")
                    logger.error(
                        "consul request failed path=%s status=%s code=%s body=%s",
                        path,
                        resp.status,
                        upstream_code,
                        body,
                    )
                    if upstream_code:
                        raise ExternalError(
                            upstream_msg or ("consul request failed path=%s status=%s" % (path, resp.status)),
                            status=resp.status,
                            reason=upstream_code,
                            code=upstream_code,
                            retryable=False,
                        )
                    raise ExternalError(
                        "consul request failed path=%s status=%s" % (path, resp.status),
                        status=resp.status,
                        reason="consul_http_%s" % resp.status,
                        code="conductor.local.consul.http_%s" % resp.status,
                        retryable=False,
                    )
                return ext_dict("consul body", body)

        try:
            return await retry_transient(_once, circuit_name="consul")
        except ExternalError:
            raise
        except Exception as e:
            if not is_retryable_error(e):
                raise
            logger.error("consul request retry exhausted path=%s code=conductor.local.consul.retry_exhausted error=%s", path, e)
            raise ExternalError(
                "consul request failed path=%s error=%s" % (path, e),
                status=502,
                reason="consul_request",
                code="conductor.local.consul.retry_exhausted",
                retryable=True,
            ) from e

    async def _consul_post_body(self, *, path: str, payload: dict, prototype_token: str | None = None) -> dict:
        return await self._consul_request(method="POST", path=path, payload=payload, prototype_token=prototype_token)

    async def _consul_post(self, *, path: str, payload: dict, prototype_token: str | None = None) -> dict:
        body = await self._consul_post_body(path=path, payload=payload, prototype_token=prototype_token)
        result = body.get("result")
        if result != 0:
            msg = body.get("msg")
            text = "consul %s result=%s msg=%s" % (path, result, msg)
            code = str(body.get("code") or "").strip()
            if not code:
                from station.errors import slug
                code = "consul.unspecified.%s.%s" % (slug(path), slug(msg or "error"))
            logger.error("%s code=%s", text, code)
            lowered = str(msg or "").lower()
            if code.endswith(".model_not_found") or "model not found" in lowered:
                status = 404
            elif code.endswith(".unauthorized") or "unauthorized" in lowered:
                status = 401
            else:
                status = 502
            raise ExternalError(text, status=status, reason=code, code=code)
        data = body.get("data")
        if data is None:
            logger.error("consul %s empty data code=conductor.local.consul.empty_data", path)
            raise ExternalError(
                "consul %s empty data" % path,
                status=502,
                reason="consul_empty",
                code="conductor.local.consul.empty_data",
            )
        return ext_dict("consul data", data)

    async def _consul_post_full(self, *, path: str, payload: dict) -> dict:
        return await self._consul_post_body(path=path, payload=payload)

    async def fetch_model_from_consul(self, *, model_id: str) -> dict:
        return await self._consul_post(path="/api/local_conductor/get_model", payload={"model_id": model_id})

    async def fetch_model_chats_from_consul(self, *, model_id: str):
        data = await self._consul_post(path="/api/local_conductor/get_model_chats", payload={"model_id": model_id})
        chat_version = ext_int(data.get("chat_version"), "chat_version")
        if chat_version <= 0:
            logger.error("consul chat_version invalid model_id=%s", model_id)
            raise ExternalError("invalid chat_version", status=502)
        chats = data.get("chats")
        if chats is None:
            logger.error("consul chats missing model_id=%s", model_id)
            raise ExternalError("consul chats missing", status=502, reason="consul_empty")
        chats = ext_list('chats', chats)
        out = []
        for c in chats:
            c = ext_dict('chat', c)
            out.append(c)
        return out, chat_version

    async def _apply_chats_if_current(self, *, model_id, prototype_id, chats, chat_version):
        local = await self.db.get_model(model_id)
        if version_is_stale(local, chat_version, "chat_version"):
            logger.error(
                "stale chat snapshot model_id=%s remote=%s local=%s",
                model_id,
                chat_version,
                local.get("chat_version"),
            )
            return False
        await self.db.replace_model_chats(model_id=model_id, prototype_id=prototype_id, chats=chats)
        await self.db.set_model_chat_version(model_id, chat_version)
        return True

    def _model_version_stale(self, local, remote) -> bool:
        return version_is_stale(local, ext_int(remote.get("version"), "version"), "version")

    def _consul_chat_op_may_be_desync(self, body) -> bool:
        if not body:
            return False
        if body.get("result") == 0:
            return False
        msg = str(body.get("msg") or "").lower()
        if "max chats" in msg or "already exists" in msg or "unauthorized" in msg:
            return False
        return (
            "account not assigned" in msg
            or "model not found" in msg
            or "chat insert conflict" in msg
        )

    async def _upsert_consul_accounts(self, *, model_id, prototype_id, remote):
        accts = remote.get("accts")
        if accts is None:
            accts = []
        accts = ext_list("accts", accts)
        stored = []
        for acct in accts:
            acct = ext_dict("acct", acct)
            acct_id = ext_str(acct.get("acct_id"), "acct_id", default="").strip()
            if not acct_id:
                raise ExternalError("acct_id must be non-empty str")
            acct_type = acct.get("acct_type")
            if acct_type is None:
                acct_type = acct.get("type")
            acct_type = ext_str(acct_type, "acct_type").strip()
            username = ext_str(acct.get("acct_username"), "acct_username").strip()
            server = ext_str(acct.get("server"), "server").strip()
            encrypted_token = ext_str(acct.get("acct_token"), "acct_token").strip()
            await self.db.upsert_local_account(
                acct_id=acct_id,
                model_id=model_id,
                prototype_id=prototype_id,
                acct_type=acct_type,
                username=username,
                server=server,
                encrypted_token=encrypted_token,
                is_local=True,
            )
            stored.append(
                {
                    "acct_id": acct_id,
                    "acct_type": acct_type,
                    "acct_username": username,
                    "server": server,
                    "acct_token": encrypted_token,
                }
            )
        return stored

    async def _drop_accounts_not_in(self, *, model_id, keep):
        keep_ids = set()
        for acct in keep:
            acct_id = _norm_acct(acct.get("acct_id"))
            if acct_id:
                keep_ids.add(acct_id)
        model_key = _norm_acct(model_id)
        for existing in await self.db.list_local_accounts():
            if _norm_acct(existing.get("model_id")) != model_key:
                continue
            existing_id = existing.get("acct_id")
            if existing_id is None:
                existing_id = ""
            if _norm_acct(existing_id) not in keep_ids:
                self._stop_account_poller(str(existing_id))
                await self.db.delete_local_account(str(existing_id))

    async def _reconcile_model_from_consul(self, *, model_id: str, prototype_id, respect_write_hold: bool = False) -> None:
        remote = await self.fetch_model_from_consul(model_id=model_id)
        if respect_write_hold and self._consul_write_held(model_id):
            return
        if not self._model_assigned_here(remote):
            logger.info("consul model moved model_id=%s; dropping local", model_id)
            await self._remove_local_model(model_id)
            return
        remote_settings = self._decrypt_model_settings(remote.get("settings"))
        if remote_settings is None:
            logger.error("model settings decrypt failed model_id=%s", model_id)
            raise ExternalError("Model settings decrypt failed", status=500)
        remote = dict(remote)
        remote["settings"] = remote_settings
        if remote.get("prototype_id") is not None:
            prototype_id = ext_int(remote.get("prototype_id"), "prototype_id")
        remote["prototype_id"] = prototype_id
        from station.api.instances import precheck_prototype
        if prototype_id is None or not await precheck_prototype(self.app, prototype_id):
            return
        if respect_write_hold and self._consul_write_held(model_id):
            return
        remote = await self.fetch_model_from_consul(model_id=model_id)
        if respect_write_hold and self._consul_write_held(model_id):
            return
        if not self._model_assigned_here(remote):
            logger.info("consul model moved model_id=%s; dropping local", model_id)
            await self._remove_local_model(model_id)
            return
        remote_settings = self._decrypt_model_settings(remote.get("settings"))
        if remote_settings is None:
            logger.error("model settings decrypt failed model_id=%s", model_id)
            raise ExternalError("Model settings decrypt failed", status=500)
        remote = dict(remote)
        remote["settings"] = remote_settings
        if remote.get("prototype_id") is not None:
            prototype_id = ext_int(remote.get("prototype_id"), "prototype_id")
        remote["prototype_id"] = prototype_id
        local_now = await self.db.get_model(model_id)
        if self._model_version_stale(local_now, remote):
            logger.error(
                "stale model snapshot model_id=%s remote=%s local=%s",
                model_id,
                remote.get("version"),
                local_now.get("version"),
            )
        else:
            await apply_consul_model(self.db, model_id, remote)
            accts = remote.get("accts")
            if accts is None:
                accts = []
            accts = ext_list('accts', accts)
            remote_ids = set()
            model_key = str(model_id).replace("-", "").strip().lower()
            for a in accts:
                a = ext_dict('acct', a)
                acct_id = a.get("acct_id")
                acct_id = ext_str(acct_id, 'acct_id', default="")
                if not acct_id:
                    raise ExternalError("acct_id must be non-empty str")
                acct_id = acct_id.replace("-", "").strip().lower()
                remote_ids.add(acct_id)
                acct_type = a.get("acct_type")
                if acct_type is None:
                    acct_type = a.get("type")
                acct_type = ext_str(acct_type, 'acct_type')
                username = ext_str(a.get("acct_username"), "acct_username")
                server = ext_str(a.get("server"), "server")
                encrypted_token = ext_str(a.get("acct_token"), "acct_token")
                await self.db.upsert_local_account(
                    acct_id=acct_id,
                    model_id=model_id,
                    prototype_id=prototype_id,
                    acct_type=acct_type.strip(),
                    username=username.strip(),
                    server=server.strip(),
                    encrypted_token=encrypted_token.strip(),
                    is_local=True,
                )
            for la in await self.db.list_local_accounts():
                local_model = str(la.get("model_id") or "").replace("-", "").strip().lower()
                if local_model != model_key:
                    continue
                stored = str(la.get("acct_id") or "")
                lid = stored.replace("-", "").strip().lower()
                if lid in remote_ids and stored.replace("-", "").strip().lower() == stored.strip().lower():
                    continue
                self._stop_account_poller(stored)
                await self.db.delete_local_account(stored)
        chats, chat_version = await self.fetch_model_chats_from_consul(model_id=model_id)
        await self._apply_chats_if_current(
            model_id=model_id,
            prototype_id=prototype_id,
            chats=chats,
            chat_version=chat_version,
        )

    async def ensure_chat_mapping(self, *, acct_id: str, chat_id: str, chat_type: str, carrier_user_id: str | None, force: bool = False) -> ChatMapping:
        if acct_id is None:
            acct_id = ""
        if chat_id is None:
            chat_id = ""
        acct_id = acct_id.strip()
        chat_id = chat_id.strip()
        if not (acct_id and chat_id):
            return ChatMapping()
        if not force:
            existing = await self.db.lookup_model_for_chat(acct_id=acct_id, chat_id=chat_id)
            if existing:
                return ChatMapping(model_id=existing)
        acct = await self.db.get_local_account(acct_id)
        if not acct:
            return ChatMapping()
        try:
            base_type = chat_type if chat_type else acct.get("acct_type")
            chat_type = validate_local_platform_type(
                strip_qr_prefix(base_type),
                field_name="chat_type",
            )
        except ExternalError as e:
            logger.error("chat type invalid acct_id=%s chat_id=%s error=%s", acct_id, chat_id, e)
            return ChatMapping()
        model_id = acct.get("model_id")
        if model_id is None:
            model_id = ""
        model_id = model_id.strip()
        if not model_id:
            return ChatMapping()
        if carrier_user_id is None:
            carrier = "unknown"
        else:
            carrier = carrier_user_id.strip() or "unknown"
        payload = {
            "chat_id": chat_id,
            "model_id": model_id,
            "acct_id": acct_id,
            "chat_type": chat_type,
            "carrier_user_id": carrier,
        }
        async with self.consul_write(model_id, acct_id):
            return await self._ensure_chat_mapping_in_consul(
                acct=acct,
                acct_id=acct_id,
                chat_id=chat_id,
                model_id=model_id,
                payload=payload,
            )

    async def _refuse_chat_mapping(self, *, acct_id, chat_id, text) -> ChatMapping:
        await self._reply_local(acct_id=acct_id, chat_id=chat_id, text=text)
        return ChatMapping(settled=True)

    def _add_chat_notice(self, body) -> str:
        code = str(body.get("code") or "")
        msg = str(body.get("msg") or "")
        if code.endswith(".max_chats") or "max chats" in msg.lower():
            return _MAX_CHATS
        return _ADD_CHAT_ERROR

    async def _ensure_chat_mapping_in_consul(self, *, acct, acct_id, chat_id, model_id, payload) -> ChatMapping:
        try:
            body = await self._consul_post_full(
                path="/api/local_conductor/add_chat_to_model",
                payload=payload,
            )
            if self._consul_chat_op_may_be_desync(body):
                prototype_id = acct.get("prototype_id")
                stored_model = await self.db.get_model(model_id)
                if stored_model and stored_model.get("prototype_id") is not None:
                    prototype_id = stored_model.get("prototype_id")
                try:
                    await self._reconcile_model_from_consul(model_id=model_id, prototype_id=prototype_id)
                except ExternalError as exc:
                    logger.error("chat reconcile failed model_id=%s error=%s", model_id, exc)
                    return await self._refuse_chat_mapping(acct_id=acct_id, chat_id=chat_id, text=_ADD_CHAT_ERROR)
                except Exception as e:
                    logger.error("unexpected where=chat_reconcile model_id=%s error=%s", model_id, e, exc_info=e)
                    return await self._refuse_chat_mapping(acct_id=acct_id, chat_id=chat_id, text=_ADD_CHAT_ERROR)
                body = await self._consul_post_full(
                    path="/api/local_conductor/add_chat_to_model",
                    payload=payload,
                )
        except ExternalError as e:
            logger.error("add_chat failed model_id=%s acct_id=%s chat_id=%s error=%s", model_id, acct_id, chat_id, e)
            return await self._refuse_chat_mapping(acct_id=acct_id, chat_id=chat_id, text=_ADD_CHAT_ERROR)
        except Exception as e:
            logger.error("unexpected where=add_chat model_id=%s acct_id=%s chat_id=%s error=%s", model_id, acct_id, chat_id, e, exc_info=e)
            return await self._refuse_chat_mapping(acct_id=acct_id, chat_id=chat_id, text=_ADD_CHAT_ERROR)
        if body.get("result") != 0:
            logger.error(
                "add_chat failed model_id=%s acct_id=%s chat_id=%s result=%s msg=%s",
                model_id,
                acct_id,
                chat_id,
                body.get("result"),
                body.get("msg"),
            )
            return await self._refuse_chat_mapping(
                acct_id=acct_id,
                chat_id=chat_id,
                text=self._add_chat_notice(body),
            )
        stored_model = await self.db.get_model(model_id)
        prototype_id = stored_model.get("prototype_id") if stored_model and stored_model.get("prototype_id") is not None else None
        await self.db.upsert_local_chat(
            acct_id=acct_id,
            chat_id=chat_id,
            model_id=model_id,
            prototype_id=prototype_id,
            chat_type=payload["chat_type"],
            carrier_user_id=payload["carrier_user_id"],
        )
        return ChatMapping(model_id=model_id)

    async def _note_start_failed(self, model_id, instance, error):
        from station.api.instances import instance_kind, isolate_instance
        kind = instance_kind(self.app, instance)
        logger.error(
            "instance start failed prototype_id=%s model_id=%s kind=%s error=%s",
            instance.prototype_id,
            model_id,
            kind,
            error,
        )
        await isolate_instance(self.app, model_id, instance)

    async def _start_created_instance(self, model_id, instance):
        lock = self.app.setdefault("_instance_start_lock", asyncio.Lock())
        async with lock:
            if self.app["instances"].get(model_id) is not instance:
                return
            try:
                await instance.start()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                await self._note_start_failed(model_id, instance, e)

    async def _ensure_instance(self, *, model_id: str, prototype_id: int, settings: dict, wait_for_start: bool = True) -> Any | None:
        from station.api.instances import precheck_prototype
        if not await precheck_prototype(self.app, prototype_id):
            return None
        instances = self.app["instances"]
        instance_lock = self.app["instance_lock"]
        created = False
        async with instance_lock:
            instance = instances.get(model_id)
            if instance is None:
                registry = self.app["tenants"]
                tenant = registry.get(prototype_id)
                if tenant is None:
                    return None
                from station.api.instances import create_tenant_instance
                instance = create_tenant_instance(
                    self.app,
                    tenant,
                    prototype_id=prototype_id,
                    model_id=model_id,
                    model_settings=settings,
                )
                instances[model_id] = instance
                created = True
        if created:
            if wait_for_start:
                try:
                    await instance.start()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    await self._note_start_failed(model_id, instance, e)
                    from station.api.instances import ModelStartError
                    raise ModelStartError() from e
            else:
                task = asyncio.create_task(self._start_created_instance(model_id, instance))
                tasks = self.app.setdefault("_instance_start_tasks", set())
                tasks.add(task)
                task.add_done_callback(tasks.discard)
        return instance

    async def _reply_local(self, *, acct_id, chat_id, text) -> bool:
        acct_id = "" if acct_id is None else str(acct_id).strip()
        chat_id = "" if chat_id is None else str(chat_id).strip()
        text = "" if text is None else str(text).strip()
        if not acct_id or not chat_id or not text:
            return False
        try:
            acct, token, adapter = await self._resolve_local_account_context(acct_id)
        except ExternalError as e:
            logger.error("local reply account failed acct_id=%s chat_id=%s error=%s", acct_id, chat_id, e)
            return False
        if not acct or not token or adapter is None:
            return False
        if not adapter.supports("send_message"):
            return False
        try:
            sent = await adapter.send_message(
                acct=acct,
                token=token,
                chat_id=chat_id,
                text=text,
            )
        except asyncio.CancelledError:
            raise
        except ExternalError as e:
            logger.error("local reply failed acct_id=%s chat_id=%s error=%s", acct_id, chat_id, e)
            return False
        except Exception as e:
            logger.error("unexpected where=local_reply acct_id=%s chat_id=%s error=%s", acct_id, chat_id, e, exc_info=e)
            return False
        return bool(sent)

    async def _settle_notice(self, *, model_id, request_id, acct_id, chat_id, text, terminal: bool) -> bool:
        sent = await self._reply_local(acct_id=acct_id, chat_id=chat_id, text=text)
        if sent or terminal:
            if request_id:
                await self.db.remember_request("%s:%s" % (model_id, request_id))
            return True
        return False

    def _decrypt_settings_or_none(self, settings, *, model_id):
        try:
            return self._decrypt_model_settings(settings)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("model settings decrypt failed model_id=%s error=%s", model_id, e, exc_info=e)
            return None

    async def _send_chat_opened(self, *, acct_id: str, chat_id: str, model_id: str | None = None, instance=None, chat_type: str = "") -> bool:
        if acct_id is None:
            acct_id = ""
        if chat_id is None:
            chat_id = ""
        acct_id = acct_id.strip()
        chat_id = chat_id.strip()
        if not acct_id or not chat_id:
            return False
        acct, token, adapter = await self._resolve_local_account_context(acct_id)
        if not acct or not token or adapter is None:
            return False
        if not adapter.supports("send_message"):
            return False
        ok = await adapter.send_message(
            acct=acct,
            token=token,
            chat_id=chat_id,
            text="Chat opened.",
        )
        if ok and model_id:
            await self.db.mark_local_chat_opened(acct_id=acct_id, chat_id=chat_id, model_id=model_id)
        if ok:
            if chat_type == "group":
                await self._notify_group_message_settings(
                    acct=acct,
                    token=token,
                    adapter=adapter,
                    chat_id=chat_id,
                    acct_id=acct_id,
                )
            await self._notify_discord_chat_opened(
                acct=acct,
                token=token,
                adapter=adapter,
                chat_id=chat_id,
                model_id=model_id,
                instance=instance,
                acct_id=acct_id,
            )
        return ok

    async def _notify_group_message_settings(self, *, acct: dict, token: str, adapter, chat_id: str, acct_id: str) -> None:
        if adapter is None or not adapter.supports("send_message"):
            return
        platform = strip_qr_prefix(adapter.acct_type)
        if not controls_group_messages(platform):
            return
        notice = ""
        if adapter.checks_group_message_settings:
            try:
                notice = await adapter.group_message_notice(acct=acct, token=token, chat_id=chat_id)
            except ExternalError as e:
                if e.reason == "group_settings_unavailable":
                    logger.info("group message settings unavailable acct_id=%s chat_id=%s error=%s", acct_id, chat_id, e)
                else:
                    logger.warning("group message settings check failed acct_id=%s chat_id=%s error=%s", acct_id, chat_id, e)
                notice = ensure_group_notice(platform)
            except Exception as e:
                logger.error("unexpected where=group_message_settings acct_id=%s chat_id=%s error=%s", acct_id, chat_id, e, exc_info=e)
                notice = ensure_group_notice(platform)
        else:
            notice = ensure_group_notice(platform)
        if not notice:
            return
        await adapter.send_message(
            acct=acct,
            token=token,
            chat_id=chat_id,
            text=notice,
        )

    async def _notify_discord_chat_opened(self, *, acct: dict, token: str, adapter, chat_id: str, model_id: str | None, instance, acct_id: str) -> None:
        if adapter is None or not adapter.supports("send_discord_voice"):
            return
        channel_type = 0
        guild_id = ""
        is_voice = False
        info = await adapter.io.get_channel_info(token=token, channel_id=chat_id)
        if info is not None:
            channel_type = ext_int(info.get("type"), "type")
            guild_id = ext_str(info.get("guild_id"), "guild_id").strip()
            is_voice = channel_type in (2, 13)
        call_support = None
        own_id = 0
        if model_id:
            model = await self.db.get_model(model_id)
            if model and model.get("prototype_id") is not None:
                own_id = model.get("prototype_id")
        if not own_id and instance is not None:
            raw_id = getattr(instance, "prototype_id", None)
            if raw_id:
                own_id = raw_id
        proto = await self.db.get_prototype_info(own_id) if own_id else None
        if own_id and not proto:
            registry = self.app["tenants"]
            tenant = registry.get(own_id)
            if tenant is None:
                return
            proto_token = tenant.token_state.token
            try:
                fetched = await fetch_prototype(
                    session=self.session,
                    consul_url=self.config.consul_url,
                    token=proto_token,
                )
            except ExternalError as e:
                logger.error("fetch_prototype failed prototype_id=%s error=%s", own_id, e)
                return
            proto = await apply_consul_prototype(self.db, fetched, tenant.client_context)
        if proto is not None and "call_support" in proto:
            call_support = proto.get("call_support")
        if is_voice and call_support is not True:
            is_voice = False
            if call_support is False and adapter.supports("send_message"):
                await adapter.send_message(
                    acct=acct,
                    token=token,
                    chat_id=chat_id,
                    text="This model does not support voice calls. Continuing as a text channel.",
                )
        if instance is None or not model_id:
            if is_voice:
                await self._maybe_auto_join_discord_voice(
                    acct=acct, token=token, adapter=adapter, chat_id=chat_id, prototype_id=own_id
                )
            return
        await instance.handle_event(
            {
                "chat_opened": {
                    "platform": "discord",
                    "channel_id": chat_id,
                    "guild_id": guild_id,
                    "channel_type": channel_type,
                    "is_voice": is_voice,
                }
            },
            model_id,
            None,
            chat_id=chat_id,
            acct_id=acct_id,
            request_id=None,
        )

    async def _maybe_auto_join_discord_voice(
        self, *, acct: dict, token: str, adapter, chat_id: str, prototype_id: int | None = None
    ) -> None:
        if adapter is None or not adapter.supports("send_discord_voice"):
            return
        own_id = prototype_id or 0
        if not own_id:
            return
        info = await self.db.get_prototype_info(own_id) if own_id else None
        if not info or info.get("call_support") is not True:
            if info is not None and info.get("call_support") is False and adapter.supports("send_message"):
                await adapter.send_message(
                    acct=acct,
                    token=token,
                    chat_id=chat_id,
                    text="This model does not support voice calls. Continuing as a text channel.",
                )
            return
        info = await adapter.io.get_channel_info(token=token, channel_id=chat_id)
        if info is None:
            return
        channel_type = ext_int(info.get("type"), "type")
        if channel_type not in (2, 13):
            return
        guild_raw = ext_str(info.get("guild_id"), "guild_id").strip()
        guild_id = guild_raw or None
        id_raw = info.get("id")
        if id_raw is None:
            channel_id = chat_id
        else:
            id_raw = ext_str(id_raw, 'id')
            channel_id = id_raw.strip() or chat_id
        await adapter.send_discord_voice(
            acct=acct,
            token=token,
            chat_id=chat_id,
            action="join",
            channel_id=channel_id,
            guild_id=guild_id,
        )

    async def deliver_inbound(self, *, model_id: str, acct_id: str, chat_id: str, request_id: str, body: dict) -> bool:
        if request_id is None:
            request_id = ""
        request_id = request_id.strip()
        acct_id = "" if acct_id is None else acct_id.strip()
        chat_id = "" if chat_id is None else chat_id.strip()
        if not acct_id or not chat_id:
            logger.error("acct_id and chat_id are required model_id=%s", model_id)
            return False
        if request_id:
            dedupe_key = "%s:%s" % (model_id, request_id)
            if await self.db.is_duplicate_request(dedupe_key):
                return True
        model = await self.db.get_model(model_id)
        if not model:
            try:
                remote = await self.fetch_model_from_consul(model_id=model_id)
            except asyncio.CancelledError:
                raise
            except ExternalError as e:
                if e.status == 404:
                    logger.error("model not in consul model_id=%s error=%s", model_id, e)
                    return await self._settle_notice(
                        model_id=model_id,
                        request_id=request_id,
                        acct_id=acct_id,
                        chat_id=chat_id,
                        text=_MODEL_UNAVAILABLE,
                        terminal=True,
                    )
                logger.error("fetch_model failed model_id=%s error=%s", model_id, e)
                return await self._settle_notice(
                    model_id=model_id,
                    request_id=request_id,
                    acct_id=acct_id,
                    chat_id=chat_id,
                    text=_COULD_NOT_REACH,
                    terminal=False,
                )
            except Exception as e:
                logger.error("unexpected where=fetch_model model_id=%s error=%s", model_id, e, exc_info=e)
                return await self._settle_notice(
                    model_id=model_id,
                    request_id=request_id,
                    acct_id=acct_id,
                    chat_id=chat_id,
                    text=_COULD_NOT_REACH,
                    terminal=False,
                )
            remote_settings = self._decrypt_settings_or_none(remote.get("settings"), model_id=model_id)
            if remote_settings is None:
                return await self._settle_notice(
                    model_id=model_id,
                    request_id=request_id,
                    acct_id=acct_id,
                    chat_id=chat_id,
                    text=_MODEL_INTERNAL,
                    terminal=True,
                )
            remote = dict(remote)
            remote["settings"] = remote_settings
            remote_pid = remote.get("prototype_id")
            if remote_pid is not None:
                remote_pid = ext_int(remote_pid, "prototype_id")
            from station.api.instances import PROTOTYPE_FAILED_REPLY, precheck_prototype
            if remote_pid is None or not await precheck_prototype(self.app, remote_pid):
                return await self._settle_notice(
                    model_id=model_id,
                    request_id=request_id,
                    acct_id=acct_id,
                    chat_id=chat_id,
                    text=PROTOTYPE_FAILED_REPLY,
                    terminal=True,
                )
            model = await apply_consul_model(self.db, model_id, remote)
        prototype_id = model.get("prototype_id")
        settings = self._decrypt_settings_or_none(model.get("settings"), model_id=model_id)
        if settings is None:
            return await self._settle_notice(
                model_id=model_id,
                request_id=request_id,
                acct_id=acct_id,
                chat_id=chat_id,
                text=_MODEL_INTERNAL,
                terminal=True,
            )
        from station.api.instances import ModelStartError
        try:
            instance = await self._ensure_instance(model_id=model_id, prototype_id=prototype_id, settings=settings)
        except ModelStartError:
            from station.api.instances import MODEL_START_FAILED_REPLY
            return await self._settle_notice(
                model_id=model_id,
                request_id=request_id,
                acct_id=acct_id,
                chat_id=chat_id,
                text=MODEL_START_FAILED_REPLY,
                terminal=True,
            )
        if instance is None:
            from station.api.instances import PROTOTYPE_FAILED_REPLY
            return await self._settle_notice(
                model_id=model_id,
                request_id=request_id,
                acct_id=acct_id,
                chat_id=chat_id,
                text=PROTOTYPE_FAILED_REPLY,
                terminal=True,
            )
        text = ext_str(body.get("text"), "text").strip()
        platform = body.get("platform")
        if platform is None:
            platform = ""
        else:
            platform = ext_str(platform, 'platform')
        if not platform.strip() and acct_id:
            acct_lookup = acct_id.strip() if acct_id else ""
            acct = await self.db.get_local_account(acct_lookup)
            if acct:
                plat = strip_qr_prefix(acct.get("acct_type"))
                if plat:
                    platform = plat
                    body["platform"] = plat
        native_chat_type = body.get("chat_type") or ""
        if native_chat_type:
            unified = station_chat_type(platform, native_chat_type)
            if not unified:
                logger.info(
                    "drop unsupported chat type platform=%s chat_type=%s chat_id=%s",
                    platform,
                    native_chat_type,
                    chat_id,
                )
                return True
            body["chat_type"] = unified
        body = instance.rewrite_inbound_attachments(body, chat_id=chat_id, acct_id=acct_id)
        if is_slash_command(text):
            cmd = text.split(None, 1)[0].split("@", 1)[0].lower() if text else ""
            if cmd == "/start":
                opened_chat_type = body.get("chat_type") or ""
                if opened_chat_type not in ("group", "private"):
                    opened_chat_type = ""
                fields = text.split()
                arg = fields[1] if len(fields) > 1 else ""
                if arg:
                    reply = await self._local_start_reply(
                        model_id=model_id,
                        acct_id=acct_id,
                        chat_id=chat_id,
                        arg=arg,
                        chat_type=opened_chat_type,
                    )
                    if reply is not None:
                        if reply:
                            await self._reply_local(acct_id=acct_id, chat_id=chat_id, text=reply)
                        if request_id:
                            await self.db.remember_request("%s:%s" % (model_id, request_id))
                        return True
                await self._send_chat_opened(
                    acct_id=acct_id,
                    chat_id=chat_id,
                    model_id=model_id,
                    instance=instance,
                    chat_type=opened_chat_type,
                )
            accepted = await instance.handle_command(
                body,
                model_id=model_id,
                model_settings=settings,
                chat_id=chat_id,
                acct_id=acct_id,
                request_id=request_id,
            )
        else:
            accepted = await instance.handle_message(
                body,
                model_id=model_id,
                model_settings=settings,
                chat_id=chat_id,
                acct_id=acct_id,
                request_id=request_id,
            )
        if accepted is False:
            if not getattr(instance, "_started", True):
                return await self._settle_notice(
                    model_id=model_id,
                    request_id=request_id,
                    acct_id=acct_id,
                    chat_id=chat_id,
                    text=_MODEL_BUSY,
                    terminal=False,
                )
            return False
        if request_id:
            await self.db.remember_request("%s:%s" % (model_id, request_id))
        return True

    async def _local_start_reply(self, *, model_id: str, acct_id: str, chat_id: str, arg: str, chat_type: str) -> str | None:
        kind = classify_start_arg(model_id, arg, chat_type)
        if kind == "format":
            logger.warning("otp token invalid model_id=%s", model_id)
            return "Invalid token format."
        if kind == "mismatch_group":
            logger.debug(
                "otp model mismatch expected=%s got=%s chat_type=%s",
                (model_id or "").replace("-", ""),
                arg[:32],
                chat_type,
            )
            return ""
        if kind == "mismatch_private":
            logger.warning(
                "otp model mismatch expected=%s got=%s chat_type=%s",
                (model_id or "").replace("-", ""),
                arg[:32],
                chat_type,
            )
            return "Invalid token. Please retry."
        if await self.db.local_chat_opened(acct_id=acct_id, chat_id=chat_id, model_id=model_id):
            return "Chat already opened."
        try:
            valid = await self.db.verify_local_otp(model_id, acct_id, start_otp_token(arg))
        except Exception as e:
            logger.error("otp verify failed model_id=%s error=%s", model_id, e, exc_info=e)
            return "Error verifying token."
        if not valid:
            logger.warning("otp invalid model_id=%s", model_id)
            return "Invalid token. Please retry."
        return None

    async def _parse_body_and_file(self, request: web.Request) -> tuple[dict, bytes | None]:
        if request.content_type and request.content_type.startswith("multipart/"):
            form = await request.post()
            payload: dict[str, Any] = {}
            file_bytes: bytes | None = None
            for k, v in form.items():
                if isinstance(v, FileField):
                    v.file.seek(0)
                    file_bytes = v.file.read()
                else:
                    payload[k] = v
            return payload, file_bytes
        return await read_json_object(request), None

    async def _poller_reconcile_loop(self) -> None:
        while not self.poller_stop.is_set():
            try:
                self.ensure_keys()
                desired: dict[str, LocalPlatformAdapter] = {}
                grouped_accounts: dict[str, list[dict]] = {}
                accounts = await self.db.list_local_accounts()
                for acct in accounts:
                    acct_type = strip_qr_prefix(acct.get("acct_type"))
                    if not acct_type:
                        continue
                    model_id = acct.get("model_id")
                    if model_id is None:
                        model_id = ""
                    model_id = model_id.strip()
                    if not model_id:
                        continue
                    if not await self.db.get_model(model_id):
                        continue
                    grouped_accounts.setdefault(acct_type, []).append(acct)

                for acct_type, grouped in grouped_accounts.items():
                    adapter = self.get_platform(acct_type)
                    if adapter is None:
                        continue
                    for acct in grouped:
                        acct_id = acct.get("acct_id")
                        if acct_id is None:
                            acct_id = ""
                        acct_id = acct_id.strip()
                        if acct_id:
                            desired[acct_id] = adapter

                desired_ids = set(desired.keys())
                current_ids = set(self._poller_tasks.keys())

                for acct_id in sorted(current_ids & desired_ids):
                    desired_type = desired[acct_id].acct_type
                    if self._poller_task_types.get(acct_id) == desired_type:
                        continue
                    t = self._poller_tasks.pop(acct_id, None)
                    if t:
                        t.cancel()
                    self._poller_task_types.pop(acct_id, None)

                for acct_id in sorted(desired_ids - set(self._poller_tasks.keys())):
                    adapter = desired.get(acct_id)
                    if adapter is None:
                        continue
                    self._poller_tasks[acct_id] = asyncio.create_task(
                        self._start_platform_poller(acct_id=acct_id, adapter=adapter)
                    )
                    self._poller_task_types[acct_id] = adapter.acct_type

                for acct_id in sorted(set(self._poller_tasks.keys()) - desired_ids):
                    t = self._poller_tasks.pop(acct_id, None)
                    self._poller_task_types.pop(acct_id, None)
                    if t:
                        t.cancel()
            except Exception as e:
                logger.error("unexpected where=local poller reconcile error=%s", e, exc_info=e)
            await asyncio.sleep(5)

    async def poller_ctx(self, app: web.Application):
        self.poller_stop = asyncio.Event()
        self._poller_reconciler = asyncio.create_task(self._poller_reconcile_loop())
        yield
        self.poller_stop.set()
        for t in list(self._poller_tasks.values()):
            t.cancel()
        self._poller_tasks.clear()
        self._poller_task_types.clear()
        if self._poller_reconciler is not None:
            self._poller_reconciler.cancel()
            self._poller_reconciler = None
        for adapter in self.platforms.values():
            try:
                await adapter.close()
            except Exception as e:
                logger.error("unexpected where=platform_close acct_type=%s error=%s", adapter.acct_type, e, exc_info=e)

    async def handle_consul_ping(self, request: web.Request) -> web.Response:
        unauthorized = self.require_consul_token(request)
        if unauthorized is not None:
            return unauthorized
        return ok({"ok": True})

    async def handle_consul_info(self, request: web.Request) -> web.Response:
        unauthorized = self.require_consul_token(request)
        if unauthorized is not None:
            return unauthorized
        model_count = await self.db.count_models()
        return ok({"conductor_address": self.conductor_address(), "model_count": model_count})

    async def handle_model_create(self, request: web.Request) -> web.Response:
        unauthorized = self.require_consul_token(request)
        if unauthorized is not None:
            return unauthorized
        prototype_id_raw = request.match_info.get("prototype_id")
        if prototype_id_raw is None:
            prototype_id_raw = ""
        model_id_raw = request.match_info.get("model_id")
        model_id = model_id_raw.strip() if model_id_raw is not None else ""
        prototype_id = ext_path_int("prototype_id", prototype_id_raw)
        mismatch = self.require_own_prototype(prototype_id)
        if mismatch is not None:
            return mismatch
        if not model_id:
            return err("Missing model_id", status=400)

        remote = await self.fetch_model_from_consul(model_id=model_id)
        chats, chat_version = await self.fetch_model_chats_from_consul(model_id=model_id)
        remote_pid = remote.get("prototype_id")
        if remote_pid is None:
            remote_pid = prototype_id
        else:
            remote_pid = ext_int(remote_pid, "prototype_id")
        if remote_pid != prototype_id:
            logger.error(
                "model prototype conflict model_id=%s path=%s consul=%s; using consul",
                model_id,
                prototype_id,
                remote_pid,
            )
            prototype_id = remote_pid
            mismatch = self.require_own_prototype(prototype_id)
            if mismatch is not None:
                return mismatch
        remote_settings = self._decrypt_model_settings(remote.get("settings"))
        if remote_settings is None:
            logger.error("model settings decrypt failed model_id=%s", model_id)
            return err("Model settings decrypt failed", status=500)
        remote = dict(remote)
        remote["settings"] = remote_settings
        remote["prototype_id"] = prototype_id
        from station.api.instances import precheck_prototype
        if not await precheck_prototype(self.app, prototype_id):
            return ok({"ok": True})
        if not self._model_assigned_here(remote):
            logger.info("consul model moved model_id=%s; dropping local", model_id)
            await self._remove_local_model(model_id)
            return ok({"ok": True})
        local_now = await self.db.get_model(model_id)
        try:
            if self._model_version_stale(local_now, remote):
                logger.error(
                    "stale model snapshot model_id=%s remote=%s local=%s",
                    model_id,
                    remote.get("version"),
                    local_now.get("version"),
                )
            else:
                await apply_consul_model(self.db, model_id, remote)
                stored_accounts = await self._upsert_consul_accounts(model_id=model_id, prototype_id=prototype_id, remote=remote)
                await self._drop_accounts_not_in(model_id=model_id, keep=stored_accounts)
            await self._apply_chats_if_current(
                model_id=model_id,
                prototype_id=prototype_id,
                chats=chats,
                chat_version=chat_version,
            )
        except ExternalError as exc:
            return err(str(exc), status=exc.status or 400)

        from station.api.instances import ModelStartError
        try:
            await self._ensure_instance(model_id=model_id, prototype_id=prototype_id, settings=remote_settings)
        except ModelStartError:
            return ok({"ok": True})
        return ok({"ok": True})

    async def _remove_local_model(self, model_id: str) -> None:
        instances = self.app["instances"]
        instance_lock = self.app["instance_lock"]
        async with instance_lock:
            instance = instances.pop(model_id, None)
            self.app["instance_runtime"].pop(model_id, None)
        if instance is not None:
            try:
                await instance.stop()
            except Exception as e:
                logger.error("unexpected where=instance_stop model_id=%s error=%s", model_id, e, exc_info=e)
        model_key = _norm_acct(model_id)
        for existing in await self.db.list_local_accounts():
            if _norm_acct(existing.get("model_id")) != model_key:
                continue
            existing_id = str(existing.get("acct_id") or "")
            self._stop_account_poller(existing_id)
            await self.db.delete_local_account(existing_id)
        await self.db.delete_local_conductor_state_for_model(model_id)
        await self.db.delete_model(model_id)

    async def _sync_one_model(self, model_id: str, prototype_id: int) -> None:
        if self.require_own_prototype(prototype_id) is not None:
            raise ExternalError("Prototype mismatch", status=404)
        remote_proto = await self._consul_post(
            path="/api/local_conductor/get_prototype",
            payload={"prototype_id": prototype_id},
        )
        registry = self.app["tenants"]
        tenant = registry.get(prototype_id)
        ctx = tenant.client_context if tenant is not None else None
        await apply_consul_prototype(self.db, remote_proto, ctx)
        new_token = remote_proto.get("token")
        if tenant is not None and new_token and new_token != tenant.token_state.token:
            logger.error("prototype token conflict prototype_id=%s; consul replaces local", prototype_id)
            await tenant.token_state.rotate_token(new_token, grace_seconds=0)
            if ctx is not None:
                ctx.token = tenant.token_state.token
        remote = await self.fetch_model_from_consul(model_id=model_id)
        if not self._model_assigned_here(remote):
            logger.info("consul model moved model_id=%s; dropping local", model_id)
            await self._remove_local_model(model_id)
            return
        remote_pid = remote.get("prototype_id")
        if remote_pid is None:
            remote_pid = prototype_id
        else:
            remote_pid = ext_int(remote_pid, "prototype_id")
        if remote_pid != prototype_id:
            logger.error(
                "model prototype conflict model_id=%s path=%s consul=%s; using consul",
                model_id,
                prototype_id,
                remote_pid,
            )
            prototype_id = remote_pid
            if self.require_own_prototype(prototype_id) is not None:
                raise ExternalError("Prototype mismatch", status=404)
        remote_settings = self._decrypt_model_settings(remote.get("settings"))
        if remote_settings is None:
            logger.error("model settings decrypt failed model_id=%s", model_id)
            raise ExternalError("Model settings decrypt failed", status=500)
        remote = dict(remote)
        remote["settings"] = remote_settings
        remote["prototype_id"] = prototype_id
        from station.api.instances import precheck_prototype
        if not await precheck_prototype(self.app, prototype_id):
            return
        remote = await self.fetch_model_from_consul(model_id=model_id)
        if not self._model_assigned_here(remote):
            logger.info("consul model moved model_id=%s; dropping local", model_id)
            await self._remove_local_model(model_id)
            return
        remote_settings = self._decrypt_model_settings(remote.get("settings"))
        if remote_settings is None:
            logger.error("model settings decrypt failed model_id=%s", model_id)
            raise ExternalError("Model settings decrypt failed", status=500)
        remote = dict(remote)
        remote["settings"] = remote_settings
        fresh_pid = remote.get("prototype_id")
        if fresh_pid is not None:
            fresh_pid = ext_int(fresh_pid, "prototype_id")
            if self.require_own_prototype(fresh_pid) is not None:
                raise ExternalError("Prototype mismatch", status=404)
            prototype_id = fresh_pid
        remote["prototype_id"] = prototype_id
        local_now = await self.db.get_model(model_id)
        if self._model_version_stale(local_now, remote):
            logger.error(
                "stale model snapshot model_id=%s remote=%s local=%s",
                model_id,
                remote.get("version"),
                local_now.get("version"),
            )
        else:
            await apply_consul_model(self.db, model_id, remote)
            stored_accounts = await self._upsert_consul_accounts(model_id=model_id, prototype_id=prototype_id, remote=remote)
            await self._drop_accounts_not_in(model_id=model_id, keep=stored_accounts)
        chats, chat_version = await self.fetch_model_chats_from_consul(model_id=model_id)
        await self._apply_chats_if_current(
            model_id=model_id,
            prototype_id=prototype_id,
            chats=chats,
            chat_version=chat_version,
        )
        await self._ensure_instance(
            model_id=model_id,
            prototype_id=prototype_id,
            settings=remote_settings,
            wait_for_start=False,
        )

    async def handle_user_sync(self, request: web.Request) -> web.Response:
        unauthorized = self.require_consul_token(request)
        if unauthorized is not None:
            return unauthorized
        body = await read_json_object(request)
        user_id = ext_str(body.get("user_id"), "user_id").strip()
        if not user_id:
            return err("Missing user_id", status=400)
        try:
            uuid.UUID(user_id)
        except ValueError:
            return err("user_id must be a uuid", status=400)
        async with self._user_sync_lock:
            stored = await self.db.list_models()
            local_ids = []
            local_by_id = {}
            for model in stored:
                model_id = model.get("model_id")
                if model_id is None:
                    continue
                model_id = str(model_id).strip()
                if not model_id:
                    continue
                local_ids.append(model_id)
                local_by_id[model_id.replace("-", "").lower()] = model_id
            data = await self._consul_post(
                path="/api/local_conductor/user_state",
                payload={
                    "user_id": user_id,
                    "conductor_address": self.conductor_address(),
                    "local_model_ids": local_ids,
                },
            )
            assigned = data.get("models")
            if assigned is None:
                assigned = []
            assigned = ext_list("models", assigned)
            remove_ids = data.get("remove_model_ids")
            if remove_ids is None:
                remove_ids = []
            remove_ids = ext_list("remove_model_ids", remove_ids)
            self_addr = normalize_http_url(self.conductor_address())
            synced = 0
            for item in assigned:
                item = ext_dict("model", item)
                model_id = ext_str(item.get("model_id"), "model_id").strip()
                if not model_id:
                    raise ExternalError("consul model missing id", status=502)
                if "prototype_id" not in item or item.get("prototype_id") is None:
                    raise ExternalError("prototype_id is required", status=502)
                if "conductor_address" not in item or item.get("conductor_address") is None:
                    raise ExternalError("conductor_address is required", status=502)
                owner = normalize_http_url(item.get("conductor_address") or "")
                prototype_id = ext_int(item.get("prototype_id"), "prototype_id")
                if owner != self_addr:
                    key = model_id.replace("-", "").lower()
                    if key in local_by_id:
                        await self._remove_local_model(local_by_id[key])
                    continue
                await self._sync_one_model(model_id, prototype_id)
                synced += 1
            removed = 0
            for raw in remove_ids:
                model_id = ext_str(raw, "model_id").strip()
                key = model_id.replace("-", "").lower()
                stored_id = local_by_id.get(key)
                if stored_id is None:
                    continue
                await self._remove_local_model(stored_id)
                removed += 1
        return ok({"synced": synced, "removed": removed})

    async def handle_model_remove(self, request: web.Request) -> web.Response:
        unauthorized = self.require_consul_token(request)
        if unauthorized is not None:
            return unauthorized
        prototype_id_raw = request.match_info.get("prototype_id")
        if prototype_id_raw is None:
            prototype_id_raw = ""
        model_id_raw = request.match_info.get("model_id")
        model_id = model_id_raw.strip() if model_id_raw is not None else ""
        prototype_id = ext_path_int("prototype_id", prototype_id_raw)
        mismatch = self.require_own_prototype(prototype_id)
        if mismatch is not None:
            return mismatch
        if not model_id:
            return err("Missing model_id", status=400)
        await self._remove_local_model(model_id)
        return ok({"ok": True})

    async def handle_model_update_acct(self, request: web.Request) -> web.Response:
        unauthorized = self.require_consul_token(request)
        if unauthorized is not None:
            return unauthorized
        acct_id_raw = request.match_info.get("acct_id")
        acct_id = acct_id_raw.strip() if acct_id_raw is not None else ""
        model_id_raw = request.match_info.get("model_id")
        model_id = model_id_raw.strip() if model_id_raw is not None else ""
        if not acct_id:
            return err("Missing acct_id", status=400)
        if not model_id:
            return err("Missing model_id", status=400)
        body = await read_json_object(request)
        stored = await self.db.get_model(model_id)
        remote = await self.fetch_model_from_consul(model_id=model_id)
        remote_settings = self._decrypt_model_settings(remote.get("settings"))
        if remote_settings is None:
            logger.error("model settings decrypt failed model_id=%s", model_id)
            return err("Model settings decrypt failed", status=500)
        remote = dict(remote)
        remote["settings"] = remote_settings
        prototype_id = remote.get("prototype_id")
        if prototype_id is not None:
            prototype_id = ext_int(prototype_id, "prototype_id")
            mismatch = self.require_own_prototype(prototype_id)
            if mismatch is not None:
                return mismatch
        remote["prototype_id"] = prototype_id
        from station.api.instances import precheck_prototype
        if not stored and (prototype_id is None or not await precheck_prototype(self.app, prototype_id)):
            return ok({"ok": True})
        local_now = await self.db.get_model(model_id)
        if self._model_version_stale(local_now, remote):
            logger.error(
                "stale model snapshot model_id=%s remote=%s local=%s",
                model_id,
                remote.get("version"),
                local_now.get("version"),
            )
            return ok({"ok": True})
        await apply_consul_model(self.db, model_id, remote)
        if not stored and prototype_id is not None:
            from station.api.instances import ModelStartError
            try:
                await self._ensure_instance(
                    model_id=model_id,
                    prototype_id=prototype_id,
                    settings=remote_settings,
                )
            except ModelStartError:
                return ok({"ok": True})
        try:
            matched = await self._upsert_consul_accounts(model_id=model_id, prototype_id=prototype_id, remote=remote)
        except ExternalError as exc:
            return err(str(exc), status=exc.status or 400)
        acct_key = acct_id.replace("-", "").strip().lower()
        acct_type = ""
        found = None
        for acct in matched:
            rid = ext_str(acct.get("acct_id"), "acct_id").replace("-", "").strip().lower()
            if rid == acct_key:
                found = acct
                acct_type = ext_str(acct.get("acct_type"), "acct_type").strip()
                break
        await self._drop_accounts_not_in(model_id=model_id, keep=matched)
        if found is None:
            logger.error("account conflict acct_id=%s model_id=%s; consul has no such account", acct_id, model_id)
            return err("Account not in consul", status=404)
        for field in ("acct_type", "acct_username", "server", "acct_token"):
            if body.get(field) is None:
                continue
            body_value = ext_str(body.get(field), field).strip()
            consul_value = ext_str(found.get(field), field).strip()
            if body_value != consul_value:
                logger.error(
                    "account field conflict acct_id=%s field=%s; using consul",
                    acct_id,
                    field,
                )
        adapter = self.get_platform(acct_type)
        if adapter is not None:
            await adapter.ensure_account_ready(acct_id)
        try:
            want_otp = ext_bool(body.get("otp"), "otp")
        except ExternalError as exc:
            return err(str(exc), status=400)
        if want_otp:
            otp = await self.db.generate_local_otp(model_id, acct_id)
            return ok({"otp": otp})
        return ok({"ok": True})

    async def handle_model_delete_acct(self, request: web.Request) -> web.Response:
        unauthorized = self.require_consul_token(request)
        if unauthorized is not None:
            return unauthorized
        acct_id_raw = request.match_info.get("acct_id")
        acct_id = acct_id_raw.strip() if acct_id_raw is not None else ""
        if not acct_id:
            return err("Missing acct_id", status=400)
        await self.db.delete_local_account(acct_id)
        return ok({"ok": True})

    async def handle_model_remove_acct(self, request: web.Request) -> web.Response:
        unauthorized = self.require_consul_token(request)
        if unauthorized is not None:
            return unauthorized
        acct_id_raw = request.match_info.get("acct_id")
        acct_id = acct_id_raw.strip() if acct_id_raw is not None else ""
        model_id_raw = request.match_info.get("model_id")
        model_id = model_id_raw.strip() if model_id_raw is not None else ""
        if not acct_id or not model_id:
            return err("Invalid path", status=400)
        await self.db.delete_local_account(acct_id)
        return ok({"ok": True})

    async def handle_model_sync_chats(self, request: web.Request) -> web.Response:
        unauthorized = self.require_consul_token(request)
        if unauthorized is not None:
            return unauthorized
        prototype_id_raw = request.match_info.get("prototype_id")
        if prototype_id_raw is None:
            prototype_id_raw = ""
        model_id_raw = request.match_info.get("model_id")
        model_id = model_id_raw.strip() if model_id_raw is not None else ""
        prototype_id = ext_path_int("prototype_id", prototype_id_raw)
        mismatch = self.require_own_prototype(prototype_id)
        if mismatch is not None:
            return mismatch
        if not model_id:
            return err("Missing model_id", status=400)
        body = await read_json_object(request)
        remote = await self.fetch_model_from_consul(model_id=model_id)
        remote_pid = remote.get("prototype_id")
        if remote_pid is not None:
            remote_pid = ext_int(remote_pid, "prototype_id")
            if remote_pid != prototype_id:
                logger.error(
                    "model prototype conflict model_id=%s path=%s consul=%s; using consul",
                    model_id,
                    prototype_id,
                    remote_pid,
                )
                prototype_id = remote_pid
                mismatch = self.require_own_prototype(prototype_id)
                if mismatch is not None:
                    return mismatch
        chats, chat_version = await self.fetch_model_chats_from_consul(model_id=model_id)
        body_chats = body.get("chats")
        if body_chats is None:
            body_chats = []
        try:
            body_chats = ext_list("chats", body_chats)
        except ExternalError as e:
            logger.error("sync chats body invalid model_id=%s error=%s; using consul", model_id, e)
            body_chats = []
        if _chat_keys(body_chats) != _chat_keys(chats):
            logger.error("chats conflict model_id=%s; consul replaces request", model_id)
        try:
            await self._apply_chats_if_current(
                model_id=model_id,
                prototype_id=prototype_id,
                chats=chats,
                chat_version=chat_version,
            )
        except ExternalError as exc:
            return err(str(exc), status=exc.status or 400)
        return ok({"ok": True})

    async def handle_model_remove_chat(self, request: web.Request) -> web.Response:
        unauthorized = self.require_consul_token(request)
        if unauthorized is not None:
            return unauthorized
        chat_id_raw = request.match_info.get("chat_id")
        chat_id = chat_id_raw.strip() if chat_id_raw is not None else ""
        model_id_raw = request.match_info.get("model_id")
        model_id = model_id_raw.strip() if model_id_raw is not None else ""
        if not chat_id or not model_id:
            return err("Invalid path", status=400)
        body = await read_json_object(request)
        try:
            acct_id = ext_str(body.get("acct_id"), "acct_id").strip()
        except ExternalError as exc:
            return err(str(exc), status=400)
        if not acct_id:
            return err("Missing acct_id", status=400)
        await self.db.remove_chat(model_id=model_id, chat_id=chat_id, acct_id=acct_id)
        return ok({"ok": True})

    async def handle_model_update_period(self, request: web.Request) -> web.Response:
        unauthorized = self.require_consul_token(request)
        if unauthorized is not None:
            return unauthorized
        return ok({"ok": True})

    async def handle_prototype_update(self, request: web.Request) -> web.Response:
        unauthorized = self.require_consul_token(request)
        if unauthorized is not None:
            return unauthorized
        prototype_id_raw = request.match_info.get("prototype_id")
        if prototype_id_raw is None:
            prototype_id_raw = ""
        prototype_id = ext_path_int("prototype_id", prototype_id_raw)
        mismatch = self.require_own_prototype(prototype_id)
        if mismatch is not None:
            return mismatch
        body = await read_json_object(request)
        remote = await self._consul_post(
            path="/api/local_conductor/get_prototype",
            payload={"prototype_id": prototype_id},
        )
        for key in ("token", "type", "access_point", "max_chats", "version", "call_support"):
            if body.get(key) is None:
                continue
            if body.get(key) != remote.get(key):
                logger.error(
                    "prototype update conflict prototype_id=%s field=%s; using consul",
                    prototype_id,
                    key,
                )
        registry = self.app["tenants"]
        tenant = registry.get(prototype_id)
        ctx = tenant.client_context if tenant is not None else None
        await apply_consul_prototype(self.db, remote, ctx)
        new_token = remote.get("token")
        if tenant is not None and new_token and new_token != tenant.token_state.token:
            logger.error("prototype token conflict prototype_id=%s; consul replaces local", prototype_id)
            await tenant.token_state.rotate_token(new_token, grace_seconds=0)
            if ctx is not None:
                ctx.token = tenant.token_state.token
        return ok({"ok": True})

    async def handle_prototype_delete(self, request: web.Request) -> web.Response:
        unauthorized = self.require_consul_token(request)
        if unauthorized is not None:
            return unauthorized
        return ok({"ok": True})

    async def handle_gateway_list_chats(self, request: web.Request) -> web.Response:
        unauthorized = self.require_prototype_token(request)
        if unauthorized is not None:
            return unauthorized
        prototype_id_raw = request.match_info.get("prototype_id")
        if prototype_id_raw is None:
            prototype_id_raw = ""
        model_id_raw = request.match_info.get("model_id")
        model_id = model_id_raw.strip() if model_id_raw is not None else ""
        acct_id_raw = request.match_info.get("acct_id")
        acct_id = acct_id_raw.strip() if acct_id_raw is not None else ""
        prototype_id = ext_path_int("prototype_id", prototype_id_raw)
        mismatch = self.require_own_prototype(prototype_id)
        if mismatch is not None:
            return mismatch
        if not model_id:
            return err("Missing model_id", status=400)
        if not (await self._prototype_version_ok(request, prototype_id)):
            return err("Synchronize from consul", status=490)
        if not (await self._model_versions_ok(request, model_id)):
            return err("Synchronize from consul", status=490)
        chats = await self.db.list_model_chats(model_id=model_id, acct_id=acct_id if acct_id else None)
        return ok({"chats": chats})

    async def _decrypt_acct_token(self, acct: dict) -> str | None:
        token_enc = acct.get("encrypted_token")
        if token_enc is None:
            token_enc = ""
        token_enc = token_enc.strip()
        if is_qr_account_type(acct.get("acct_type")):
            return token_enc
        token, _ = decrypt_if_encrypted(self.private_key, token_enc)
        return token.strip()

    async def handle_pair_start(self, request: web.Request) -> web.Response:
        unauthorized = self.require_consul_token(request)
        if unauthorized is not None:
            return unauthorized
        body = await read_json_object(request)
        try:
            data = await self.pair_manager.start(
                acct_id=ext_str(body.get("acct_id"), "acct_id").strip(),
                platform=ext_str(body.get("platform"), "platform").strip(),
                qr_timeout_ms=body.get("qr_timeout_ms"),
            )
        except ExternalError as exc:
            return err(str(exc), status=400)
        return ok(data)

    async def handle_pair_status(self, request: web.Request) -> web.Response:
        unauthorized = self.require_consul_token(request)
        if unauthorized is not None:
            return unauthorized
        body = await read_json_object(request)
        try:
            data = await self.pair_manager.status(
                acct_id=ext_str(body.get("acct_id"), "acct_id").strip(),
                platform=ext_str(body.get("platform"), "platform").strip(),
            )
        except ExternalError as exc:
            return err(str(exc), status=400)
        return ok(data)

    async def handle_pair_cancel(self, request: web.Request) -> web.Response:
        unauthorized = self.require_consul_token(request)
        if unauthorized is not None:
            return unauthorized
        body = await read_json_object(request)
        try:
            data = await self.pair_manager.cancel(
                acct_id=ext_str(body.get("acct_id"), "acct_id").strip(),
                platform=ext_str(body.get("platform"), "platform").strip(),
            )
        except ExternalError as exc:
            return err(str(exc), status=400)
        return ok(data)

    def _decrypt_model_settings(self, settings: Any) -> dict | None:
        if settings is None:
            return {}
        settings = ext_dict('settings', settings)
        if not settings:
            return {}
        enc = settings.get("__enc__")
        if enc is None:
            return dict(settings)
        try:
            enc = ext_str(enc, "__enc__", default="")
        except ExternalError:
            return dict(settings)
        if not enc:
            return dict(settings)
        plain, _ = decrypt_if_encrypted(self.private_key, enc)
        decoded = json.loads(plain) if plain.strip() else {}
        decoded = ext_dict('decrypted settings', decoded)
        return decoded

    async def handle_gateway(self, request: web.Request) -> web.Response:
        unauthorized = self.require_prototype_token(request)
        if unauthorized is not None:
            return unauthorized
        method_raw = request.match_info.get("method")
        method = method_raw.strip() if method_raw is not None else ""
        chat_id_raw = request.match_info.get("chat_id")
        chat_id = chat_id_raw.strip() if chat_id_raw is not None else ""
        prototype_id_raw = request.match_info.get("prototype_id")
        if prototype_id_raw is None:
            prototype_id_raw = ""
        model_id_raw = request.match_info.get("model_id")
        model_id = model_id_raw.strip() if model_id_raw is not None else ""
        acct_id_raw = request.match_info.get("acct_id")
        acct_id = acct_id_raw.strip() if acct_id_raw is not None else ""
        prototype_id = ext_path_int("prototype_id", prototype_id_raw)
        mismatch = self.require_own_prototype(prototype_id)
        if mismatch is not None:
            return mismatch
        if not (method and model_id and acct_id):
            return err("Invalid path", status=400)
        if not (await self._prototype_version_ok(request, prototype_id)):
            return err("Synchronize from consul", status=490)
        if not (await self._model_versions_ok(request, model_id)):
            return err("Synchronize from consul", status=490)

        acct, token, adapter = await self._resolve_local_account_context(acct_id)
        if not acct:
            return err("Acct not found", status=404)
        if not token:
            return err("Acct token decrypt failed", status=500)
        try:
            payload, file_bytes = await self._parse_body_and_file(request)
        except ExternalError as exc:
            return err(str(exc), status=400)
        payload.pop("acct_id", None)
        payload.pop("chat_id", None)
        payload.pop("request_id", None)
        return await self._dispatch_platform_method(
            adapter=adapter,
            acct=acct,
            token=token,
            method=method,
            chat_id=chat_id,
            payload=payload,
            file_bytes=file_bytes,
        )

    def _read_first_file_bytes(self, files):
        if not files:
            return None
        for path in files.values():
            if not path:
                continue
            with open(path, "rb") as f:
                return f.read()
        return None

    async def invoke(
        self,
        *,
        method,
        prototype_id,
        model_id,
        chat_id=None,
        acct_id=None,
        params=None,
        files=None,
    ):
        method = method.strip() if method else ""
        model_id = model_id.strip() if model_id else ""
        chat_id = chat_id.strip() if chat_id else ""
        if not (method and model_id):
            return False
        mismatch = self.require_own_prototype(prototype_id)
        if mismatch is not None:
            return False
        payload = dict(params) if params else {}
        preferred = ""
        raw_preferred = payload.get("acct_id")
        if raw_preferred is not None:
            preferred = str(raw_preferred).strip()
        if acct_id:
            preferred = acct_id.strip()
        if not preferred:
            return False
        resolved_acct = preferred
        acct, token, adapter = await self._resolve_local_account_context(resolved_acct)
        if not acct or not token:
            return False
        payload.pop("acct_id", None)
        payload.pop("chat_id", None)
        payload.pop("request_id", None)
        file_bytes = self._read_first_file_bytes(files)
        if method == "download_file":
            resp = await self._dispatch_platform_method(
                adapter=adapter,
                acct=acct,
                token=token,
                method=method,
                chat_id=chat_id,
                payload=payload,
                file_bytes=file_bytes,
            )
            if resp.status >= 400:
                return False
            body = resp.body
            if body is None:
                return b""
            if isinstance(body, (bytes, bytearray)):
                return bytes(body)
            return body
        resp = await self._dispatch_platform_method(
            adapter=adapter,
            acct=acct,
            token=token,
            method=method,
            chat_id=chat_id,
            payload=payload,
            file_bytes=file_bytes,
        )
        return resp.status < 400

    async def _resume_local_account(self, acct_id: str, acct_type: str) -> None:
        adapter = self.get_platform(acct_type)
        if adapter is None:
            return
        stored = await self.db.get_local_account(acct_id)
        if stored is None:
            compact = acct_id.replace("-", "")
            if compact != acct_id:
                stored = await self.db.get_local_account(compact)
                if stored is not None:
                    acct_id = compact
        if stored is None:
            return
        model_id = stored.get("model_id")
        if model_id is None:
            model_id = ""
        model_id = str(model_id).strip()
        if not model_id or not await self.db.get_model(model_id):
            return
        await adapter.ensure_account_ready(acct_id)
        task = self._poller_tasks.get(acct_id)
        if task is not None and not task.done():
            return
        if task is not None:
            self._poller_tasks.pop(acct_id, None)
            self._poller_task_types.pop(acct_id, None)
        self._poller_tasks[acct_id] = asyncio.create_task(
            self._start_platform_poller(acct_id=acct_id, adapter=adapter)
        )
        self._poller_task_types[acct_id] = adapter.acct_type

    async def handle_account_check_status(self, request: web.Request) -> web.Response:
        unauthorized = self.require_consul_token(request)
        if unauthorized is not None:
            return unauthorized
        body = await read_json_object(request)
        acct_id = ext_str(body.get("acct_id"), "acct_id").strip()
        acct_type = ext_str(body.get("acct_type"), "acct_type").strip().lower()
        username = ext_str(body.get("acct_username"), "acct_username").strip()
        token_raw = ext_str(body.get("acct_token"), "acct_token").strip()
        server_raw = ext_str(body.get("server"), "server").strip()
        if not acct_id or not acct_type:
            return err("acct_id and acct_type are required", status=400)
        try:
            token, _ = decrypt_if_encrypted(self.private_key, token_raw)
            server, _ = decrypt_if_encrypted(self.private_key, server_raw)
        except (ExternalError, ValueError, json.JSONDecodeError, TypeError):
            return err("Local account token could not be decrypted", status=400)
        try:
            status = await check_local_account(
                self,
                acct_id=acct_id,
                acct_type=acct_type,
                username=username,
                token=token,
                server=server,
            )
        except AccountUnreachable:
            return web.json_response({"ok": False, "msg": "Could not reach the platform"})
        if status == "active":
            await self._resume_local_account(acct_id, acct_type)
        return ok({"status": status})

    def decrypt_settings(self, settings):
        return self._decrypt_model_settings(settings)

def ensure_local_conductor(app: web.Application):
    existing = app.get("local_conductor")
    if existing is None:
        app["local_conductor"] = LocalConductor(app)
        existing = app["local_conductor"]
    existing.ensure_keys()
    if not app.get("_local_conductor_routes"):
        setup_local_conductor_routes(app)
        app["_local_conductor_routes"] = True
    return existing

def setup_local_conductor_routes(app: web.Application) -> None:
    lc: LocalConductor = app["local_conductor"]
    app.router.add_post("/consul/ping", catch_external(lc.handle_consul_ping))
    app.router.add_post("/consul/info", catch_external(lc.handle_consul_info))
    app.router.add_post("/pair/start", catch_external(lc.handle_pair_start))
    app.router.add_post("/pair/status", catch_external(lc.handle_pair_status))
    app.router.add_post("/pair/cancel", catch_external(lc.handle_pair_cancel))
    app.router.add_post("/account/check_status", catch_external(lc.handle_account_check_status))
    for adapter in lc.platforms.values():
        adapter.register_routes(app)
    app.router.add_post("/user/sync", catch_external(lc.handle_user_sync))
    app.router.add_post("/model/create/{prototype_id}/{model_id}", catch_external(lc.handle_model_create))
    app.router.add_post("/model/remove/{prototype_id}/{model_id}", catch_external(lc.handle_model_remove))
    app.router.add_post("/model/remove_chat/{chat_id}/{model_id}", catch_external(lc.handle_model_remove_chat))
    app.router.add_post("/model/remove_acct/{acct_id}/{model_id}", catch_external(lc.handle_model_remove_acct))
    app.router.add_post("/model/update_acct/{acct_id}/{model_id}", catch_external(lc.handle_model_update_acct))
    app.router.add_post("/model/update_acct/{acct_id}", catch_external(lc.handle_model_delete_acct))
    app.router.add_post("/model/sync_chats/{prototype_id}/{model_id}", catch_external(lc.handle_model_sync_chats))
    app.router.add_post("/model/update_period/{prototype_id}/{model_id}", catch_external(lc.handle_model_update_period))
    app.router.add_post("/prototype/update/{prototype_id}", catch_external(lc.handle_prototype_update))
    app.router.add_post("/prototype/delete/{prototype_id}", catch_external(lc.handle_prototype_delete))
    app.router.add_post("/gateway/list_chats/{prototype_id}/{model_id}", catch_external(lc.handle_gateway_list_chats))
    app.router.add_post("/gateway/list_chats/{prototype_id}/{model_id}/{acct_id}", catch_external(lc.handle_gateway_list_chats))
    app.router.add_post("/gateway/{method}/{chat_id}/{prototype_id}/{model_id}/{acct_id}", catch_external(lc.handle_gateway))
