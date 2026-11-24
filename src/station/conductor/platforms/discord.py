import asyncio
import base64
import io
import json
import mimetypes
import os
import shutil
import subprocess
import tempfile
import time
from urllib.parse import urlparse
from typing import Any

import aiohttp

from station.conductor.crypto import decrypt_if_encrypted
from station.conductor.platforms import guidance
from station.conductor.platforms.group_settings import discord_group_notice
from station.conductor.platforms.keyboard import discord_components, parse_keyboard
from station.conductor.util import apply_sender, attachment_type_from_meta, ext_bool, ext_id, ext_int, ext_str
from station import logger
from station.client.retry import RetryableError, is_retryable_error, is_transient_status, retry_transient
from station.errors import ExternalError
from station.prototypes.boundary import ext_dict, ext_list
from station.timeouts import http_timeout, ws_timeout

_THREAD_CHANNEL_TYPES = {10, 11, 12}
_DM_CHANNEL_TYPES = {1, 3}
_API_VERSION_PREFIXES = ("/api/v10", "/api/v9")


def discord_rest_url(base: str, path: str) -> str:
    """Join a Discord or DSB root with an /api/v9 path.

    Official config is https://discord.com/api/v9, and paths already start with
    /api/v9. Appending those paths again requests /api/v9/api/v9/... and the
    gateway fallback becomes wss://discord.com/api/v9/gateway, which Discord
    rejects. DSB roots do not include /api/vN, so the path is appended whole.
    """
    root = (base or "").strip().rstrip("/")
    suffix = path if str(path or "").startswith("/") else "/" + str(path or "")
    lower_root = root.lower()
    versioned = any(lower_root.endswith(prefix) for prefix in _API_VERSION_PREFIXES)
    if versioned:
        lower_suffix = suffix.lower()
        for prefix in _API_VERSION_PREFIXES:
            if lower_suffix.startswith(prefix):
                suffix = suffix[len(prefix):]
                break
        if not suffix.startswith("/"):
            suffix = "/" + suffix
    return root + suffix


def discord_gateway_fallback(base: str) -> str:
    root = (base or "").strip().rstrip("/")
    lower_root = root.lower()
    for prefix in _API_VERSION_PREFIXES:
        if lower_root.endswith(prefix):
            root = root[: len(root) - len(prefix)].rstrip("/")
            break
    host = root.split("://", 1)[-1].split("/", 1)[0].lower()
    if host == "discord.com" or host.endswith(".discord.com") or host == "discordapp.com" or host.endswith(".discordapp.com"):
        return "wss://gateway.discord.gg"
    if root.startswith("https://"):
        return "wss://" + root[len("https://"):] + "/gateway"
    if root.startswith("http://"):
        return "ws://" + root[len("http://"):] + "/gateway"
    return "ws://" + root + "/gateway"


def _transcode_discord_voice(data: bytes) -> bytes | None:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg or not data:
        return None
    with tempfile.TemporaryDirectory() as directory:
        src = os.path.join(directory, "audio.bin")
        dst = os.path.join(directory, "voice.ogg")
        with open(src, "wb") as handle:
            handle.write(data)
        try:
            proc = subprocess.run(
                [
                    ffmpeg, "-y", "-i", src,
                    "-ar", "48000", "-ac", "1", "-c:a", "libopus",
                    "-b:a", "32k", "-vbr", "on", "-application", "voip",
                    "-f", "ogg", dst,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=30,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        if proc.returncode != 0 or not os.path.exists(dst):
            return None
        with open(dst, "rb") as handle:
            out = handle.read()
    if not out.startswith(b"OggS"):
        return None
    return out


def _discord_cdn_host(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.startswith("cdn.") or host.startswith("media.")


def _ogg_opus_duration(data: bytes) -> float:
    i = 0
    last = 0
    n = len(data)
    while i + 27 <= n and data[i : i + 4] == b"OggS":
        granule = int.from_bytes(data[i + 6 : i + 14], "little")
        if granule > last:
            last = granule
        nseg = data[i + 26]
        if i + 27 + nseg > n:
            break
        i += 27 + nseg + sum(data[i + 27 : i + 27 + nseg])
    if last <= 0:
        return 1.0
    return last / 48000.0

def _discord_user_sender(user):
    if user is None:
        return "", ""
    username = ext_str(user.name, "username").strip()
    global_name = ext_str(user.global_name, "global_name").strip()
    return (global_name or username), username


def _discord_application_flags(payload):
    payload = ext_dict("discord application", payload)
    app_id = ext_id(payload.get("id"), "id").strip()
    if not app_id:
        raise ExternalError("discord application id missing")
    raw_new = payload.get("flags_new")
    if raw_new is not None and raw_new != "":
        text = ext_str(raw_new, "flags_new").strip()
        if text:
            try:
                return int(text)
            except ValueError as e:
                raise ExternalError("discord flags_new invalid") from e
    return ext_int(payload.get("flags"), "flags")


def discord_mentions_bot(*, bot_id="", content="", mention_ids=None, referenced_author_id=""):
    bot_id = (bot_id or "").strip()
    if not bot_id:
        return False
    for uid in mention_ids or []:
        if (uid or "").strip() == bot_id:
            return True
    content = content or ""
    if ("<@%s>" % bot_id) in content or ("<@!%s>" % bot_id) in content:
        return True
    if (referenced_author_id or "").strip() == bot_id:
        return True
    return False


def _discord_mention_ids_from_users(users):
    ids = []
    for user in users or []:
        if user is None:
            continue
        if isinstance(user, dict):
            uid = ext_id(user.get("id"), "id").strip()
        else:
            uid = ext_id(getattr(user, "id", None), "id").strip()
        if uid:
            ids.append(uid)
    return ids


def _discord_author_sender(author):
    username = ext_str(author.get("username"), "username").strip()
    global_name = ext_str(author.get("global_name"), "global_name").strip()
    return (global_name or username), username


_DISCORD_EPOCH_MS = 1420070400000


def discord_snowflake_greater(message_id: str, cursor: str) -> bool:
    message_id = (message_id or "").strip()
    cursor = (cursor or "").strip()
    if not message_id:
        return False
    if cursor == "" or cursor == "0":
        return True
    try:
        return int(message_id) > int(cursor)
    except ValueError:
        return message_id > cursor


def discord_snowflake_recent(snowflake: str, *, now: float, window: float) -> bool:
    snowflake = (snowflake or "").strip()
    if not snowflake.isdigit():
        return False
    created = ((int(snowflake) >> 22) + _DISCORD_EPOCH_MS) / 1000.0
    age = now - created
    return age >= 0 and age <= window


def dsb_inbound_ignored(message: dict, *, self_id: str) -> bool:
    if not isinstance(message, dict):
        return True
    author = message.get("author")
    if not isinstance(author, dict):
        return True
    author_id = ext_id(author.get("id"), "id").strip()
    channel_id = ext_id(message.get("channel_id"), "channel_id").strip()
    if not author_id or not channel_id:
        return True
    self_id = (self_id or "").strip()
    if self_id and author_id == self_id:
        return True
    if ext_bool(author.get("bot"), "bot") or ext_bool(author.get("system"), "system"):
        return True
    if ext_int(message.get("type"), "type") not in (0, 19):
        return True
    return False


def is_thread_channel_type(channel_type):
    return int(channel_type) in _THREAD_CHANNEL_TYPES

def _discord_chat_type_name(channel_type):
    if channel_type == 1:
        return "dm"
    if channel_type == 3:
        return "group_dm"
    if channel_type in (2, 13):
        return "voice"
    if channel_type == 15:
        return "forum"
    if is_thread_channel_type(channel_type):
        return "thread"
    return "guild_text"

def join_chat_id(chat_id, thread_id=""):
    chat_id = (chat_id or "").strip()
    thread_id = (thread_id or "").strip()
    if not chat_id or not thread_id:
        return chat_id
    return f"{chat_id}:{thread_id}"

def split_chat_id(value):
    value = (value or "").strip()
    if not value:
        return "", ""
    idx = value.rfind(":")
    if idx <= 0 or idx == len(value) - 1:
        return value, ""
    thread = value[idx + 1 :]
    if not thread.isdigit():
        return value, ""
    chat = value[:idx].strip()
    if not chat:
        return value, ""
    return chat, thread

def open_chat_id(platform_chat_id):
    chat_id, _ = split_chat_id(platform_chat_id)
    return chat_id

def send_channel_id(platform_chat_id):
    parent, thread = split_chat_id(platform_chat_id)
    if thread:
        return thread
    return parent

def resolve_chat_and_thread(channel_id, parent_id="", channel_type=0):
    channel_id = (channel_id or "").strip()
    parent_id = (parent_id or "").strip()
    if not channel_id:
        return "", "", ""
    ctype = 0 if channel_type is None else int(channel_type)
    if is_thread_channel_type(ctype) and parent_id:
        return join_chat_id(parent_id, channel_id), parent_id, "thread"
    if parent_id and parent_id != channel_id:
        return join_chat_id(parent_id, channel_id), parent_id, "thread"
    return channel_id, channel_id, _discord_chat_type_name(ctype)

def _discord_channel_parent_id(chan) -> str:
    pid = chan.parent_id
    if pid is None:
        return ""
    return ext_id(pid, "parent_id").strip()

def _discord_sticker_format(s) -> int:
    fmt = s.format
    if fmt is None:
        return 1
    return int(fmt)

class _DiscordAcctState:

    __slots__ = (
        "client",
        "client_task",
        "voice_pending",
        "gateway_ws",
        "gateway_task",
        "gateway_ready",
        "gateway_token",
        "gateway_user_id",
        "gateway_session_id",
        "gateway_last_seq",
        "gateway_resume_url",
        "gateway_hb_ack",
        "gateway_channels",
        "backlog_threads",
    )

    def __init__(self) -> None:
        self.client: Any = None
        self.client_task: asyncio.Task | None = None
        self.voice_pending: dict[str, dict] = {}
        self.gateway_ws: Any = None
        self.gateway_task: asyncio.Task | None = None
        self.gateway_ready = asyncio.Event()
        self.gateway_token: str = ""
        self.gateway_user_id: str = ""
        self.gateway_last_seq: int | None = None
        self.gateway_resume_url: str = ""
        self.gateway_hb_ack: bool = True
        self.gateway_session_id: str = ""
        self.gateway_channels: dict[str, dict] = {}
        self.backlog_threads: set[str] = set()

class DiscordIO:
    def __init__(self, conductor):
        self.conductor = conductor
        self._accounts: dict[str, _DiscordAcctState] = {}
        self._dsb_channel_cursors: dict[str, dict[str, str]] = {}
        self._dsb_msg_lock = asyncio.Lock()
        self._dsb_inflight: set[tuple[str, str, str]] = set()

    def _acct_state(self, acct_id: str) -> _DiscordAcctState:
        if not acct_id:
            raise ExternalError("discord acct_id required")
        st = self._accounts.get(acct_id)
        if st is None:
            st = _DiscordAcctState()
            self._accounts[acct_id] = st
        return st

    def _find_client(self) -> Any | None:
        for st in self._accounts.values():
            if st.client is not None:
                return st.client
        return None

    def _get_cursors(self, acct_id: str) -> dict[str, str]:
        if not acct_id:
            return {}
        cache = self._dsb_channel_cursors.get(acct_id)
        if cache is None:
            cache = {}
            self._dsb_channel_cursors[acct_id] = cache
        return cache

    async def _load_runtime_state(self, acct_id: str) -> dict:
        state = await self.conductor.db.get_local_account_runtime_state(
            acct_id=acct_id,
            acct_type="discord",
            state_key="poller",
        )
        if state is None:
            self._dsb_channel_cursors.pop(acct_id, None)
            return {}
        state = ext_dict('discord runtime state', state)
        restored: dict[str, str] = {}
        raw_cursors = state.get("channel_cursors")
        if raw_cursors is None:
            raw_cursors = {}
        else:
            raw_cursors = ext_dict('discord channel_cursors', raw_cursors)
        for channel_id, msg_id in raw_cursors.items():
            channel_key = ext_id(channel_id, "channel_id").strip()
            msg_key = ext_id(msg_id, "msg_id").strip()
            if channel_key and msg_key:
                restored[channel_key] = msg_key
        if restored:
            self._dsb_channel_cursors[acct_id] = restored
        else:
            self._dsb_channel_cursors.pop(acct_id, None)
        return state

    async def _save_runtime_state(self, acct_id: str, *, mode: str) -> None:
        cursors = self._get_cursors(acct_id)
        await self.conductor.db.set_local_account_runtime_state(
            acct_id=acct_id,
            acct_type="discord",
            state_key="poller",
            state={
                "mode": mode or "discord",
                "channel_cursors": cursors,
                "updated_at": time.time(),
            },
        )

    @property
    def session(self) -> aiohttp.ClientSession:
        return self.conductor.session

    def _event_attachments(self, atts: list[Any], stickers: list[Any] | None = None) -> tuple[list[dict], str]:
        out: list[dict] = []
        for a in atts or []:
            url_raw = a.url
            if not url_raw:
                url_raw = a.proxy_url
            url = ext_str(url_raw, "url").strip()
            file_id = ext_id(a.id, "id").strip()
            if not url and not file_id:
                continue
            filename = ext_str(a.filename, "filename").strip()
            content_type = ext_str(a.content_type, "content_type").strip()
            item: dict[str, Any] = {
                "type": attachment_type_from_meta(filename=filename, content_type=content_type),
                "file_id": file_id or url,
                "file_name": filename,
                "content_type": content_type,
            }
            if url:
                item["url"] = url
            size = a.size
            if size is not None:
                item["file_size"] = ext_int(size, "size")
            out.append(item)
        for s in stickers or []:
            sid = ext_id(s.id, "id").strip()
            name = ext_str(s.name, "name").strip()
            if not sid and not name:
                continue
            fmt = _discord_sticker_format(s)
            url_str, mime_type, animated = guidance.discord_sticker_url(
                sid, fmt, self._api_base()
            )
            item = {
                "type": "sticker",
                "file_id": sid,
                "file_name": name,
                "content_type": mime_type,
            }
            if url_str:
                item["url"] = url_str
            if animated and not url_str:
                item["is_animated"] = True
            out.append(item)
        kept, notes = guidance.filter_oversized_attachments("discord", out)
        return kept, "\n".join(notes) if notes else ""

    async def _deliver_component_interaction(
        self,
        *,
        acct_id: str,
        interaction_id: str,
        user_id: str,
        channel_id: str,
        message_id: str,
        custom_id: str,
        parent_id: str = "",
        channel_type: int = 0,
        sender_name: str = "",
        sender_username: str = "",
    ) -> None:
        custom_id = (custom_id or "").strip()
        channel_id = (channel_id or "").strip()
        user_id = (user_id or "").strip()
        if not custom_id or not channel_id or not user_id:
            return
        chat_id, open_id, chat_type = resolve_chat_and_thread(channel_id, parent_id, channel_type)
        force = custom_id.startswith("/start")
        model_id = None if force else (await self.conductor.db.lookup_model_for_chat(acct_id=acct_id, chat_id=open_id))
        if not model_id:
            mapped = await self.conductor.ensure_chat_mapping(
                acct_id=acct_id,
                chat_id=open_id,
                chat_type="discord",
                carrier_user_id=user_id,
                force=force,
            )
            model_id = mapped.model_id
        if not model_id:
            return
        msg_id = f"interaction:{(interaction_id or '').strip() or channel_id}"
        body = {
            "method": "send_message",
            "params": {"text": custom_id},
            "text": custom_id,
            "caption": "",
            "attachments": [],
            "msg_id": msg_id,
            "platform": "discord",
            "user_id": user_id,
        }
        apply_sender(body, name=sender_name, username=sender_username)
        if message_id:
            body["reply_to"] = message_id
        if chat_type:
            body["chat_type"] = chat_type
        body["mentioned"] = True
        await self.conductor.deliver_inbound(
            model_id=model_id,
            acct_id=acct_id,
            chat_id=chat_id,
            request_id=msg_id,
            body=body,
        )

    async def _ack_dsb_interaction(self, *, token: str, interaction_id: str, interaction_token: str) -> None:
        interaction_id = (interaction_id or "").strip()
        interaction_token = (interaction_token or "").strip()
        if not interaction_id or not interaction_token:
            return
        await self._dsb_request(
            method="POST",
            token=token,
            path=f"/api/v9/interactions/{interaction_id}/{interaction_token}/callback",
            json_body={"type": 6},
        )

    async def _deliver_reaction_payload(self, *, acct_id: str, data: dict, removed: bool) -> None:
        emoji = data.get("emoji")
        if emoji is None:
            emoji = {}
        else:
            emoji = ext_dict('discord reaction emoji', emoji)
        sender_name = ""
        sender_username = ""
        member = data.get("member")
        if member is not None:
            member = ext_dict('discord reaction member', member)
            member_user = member.get("user")
            if member_user is not None:
                member_user = ext_dict('discord reaction user', member_user)
                sender_name, sender_username = _discord_author_sender(member_user)
        await self._deliver_reaction_fields(
            acct_id=acct_id,
            user_id=ext_id(data.get("user_id"), "user_id").strip(),
            channel_id=ext_id(data.get("channel_id"), "channel_id").strip(),
            message_id=ext_id(data.get("message_id"), "message_id").strip(),
            emoji_name=ext_str(emoji.get("name"), "name").strip(),
            emoji_id=ext_id(emoji.get("id"), "id").strip() if emoji.get("id") not in (None, "") else "",
            removed=removed,
            sender_name=sender_name,
            sender_username=sender_username,
        )

    async def _deliver_reaction(self, *, acct_id: str, payload, removed: bool) -> None:
        if payload is None:
            return
        emoji = payload.emoji
        emoji_name = ""
        emoji_id = ""
        if emoji is not None:
            emoji_name = ext_str(emoji.name, "name").strip()
            if emoji.id is not None:
                emoji_id = ext_id(emoji.id, "id").strip()
        sender_name, sender_username = _discord_user_sender(payload.member)
        await self._deliver_reaction_fields(
            acct_id=acct_id,
            user_id=ext_id(payload.user_id, "user_id").strip(),
            channel_id=ext_id(payload.channel_id, "channel_id").strip(),
            message_id=ext_id(payload.message_id, "message_id").strip(),
            emoji_name=emoji_name,
            emoji_id=emoji_id,
            removed=removed,
            sender_name=sender_name,
            sender_username=sender_username,
        )

    async def _deliver_reaction_fields(
        self,
        *,
        acct_id: str,
        user_id: str,
        channel_id: str,
        message_id: str,
        emoji_name: str,
        emoji_id: str,
        removed: bool,
        sender_name: str = "",
        sender_username: str = "",
    ) -> None:
        if not user_id or not channel_id or not message_id:
            return
        st = self._acct_state(acct_id)
        client = st.client
        if client is not None and client.user is not None and ext_id(client.user.id, "user.id") == user_id:
            return
        if st.gateway_user_id and st.gateway_user_id == user_id:
            return
        text = guidance.format_reaction_text(None, None)
        if not removed:
            if emoji_id:
                text = guidance.format_reaction_text(None, [f"{emoji_name}:{emoji_id}" if emoji_name else emoji_id])
            elif emoji_name:
                text = guidance.format_reaction_text([emoji_name], None)
        msg_id = f"reaction:{channel_id}:{message_id}:{user_id}:{emoji_id}{emoji_name}"
        if removed:
            msg_id = msg_id + ":removed"
        acct = await self.conductor.db.get_local_account(acct_id)
        token = ""
        if acct:
            enc = (acct.get("encrypted_token") or "").strip()
            token, _ = decrypt_if_encrypted(self.conductor.private_key, enc)
        chat_id, open_id, chat_type = await self.resolve_message_chat_thread(token=token, channel_id=channel_id)
        model_id = await self.conductor.db.lookup_model_for_chat(acct_id=acct_id, chat_id=open_id)
        if not model_id:
            mapped = await self.conductor.ensure_chat_mapping(
                acct_id=acct_id,
                chat_id=open_id,
                chat_type="discord",
                carrier_user_id=user_id,
                force=False,
            )
            model_id = mapped.model_id
        if not model_id:
            return
        body = {
            "method": "send_message",
            "params": {"text": text},
            "text": text,
            "caption": "",
            "attachments": [],
            "msg_id": msg_id,
            "reply_to": message_id,
            "platform": "discord",
            "user_id": user_id,
        }
        apply_sender(body, name=sender_name, username=sender_username)
        if chat_type:
            body["chat_type"] = chat_type
        body["mentioned"] = False
        await self.conductor.deliver_inbound(
            model_id=model_id,
            acct_id=acct_id,
            chat_id=chat_id,
            request_id=msg_id,
            body=body,
        )

    def _api_base(self) -> str:
        raw = self.conductor.config.discord_api_url.strip()
        if raw:
            return raw.rstrip("/")
        for k in ("DISCORD_API_URL", "DSB_API_URL"):
            v = (os.environ.get(k) or "").strip()
            if v:
                return v.rstrip("/")
        return ""

    async def _dsb_request(self, *, method: str, token: str, path: str, params: dict | None = None, json_body: dict | None = None) -> tuple[int, Any]:
        base = self._api_base()
        if not base:
            return 0, None
        url = discord_rest_url(base, path)
        headers = {"Authorization": "Bot " + (token or "").strip()}

        async def _once():
            async with self.session.request(method.upper(), url, headers=headers, params=params, json=json_body) as resp:
                status = int(resp.status)
                try:
                    body = await resp.json(content_type=None)
                except (aiohttp.ContentTypeError, json.JSONDecodeError, ValueError) as e:
                    if is_transient_status(status):
                        raise RetryableError("discord body status=%s error=%s" % (status, e)) from e
                    body = await resp.text()
                if is_transient_status(status):
                    raise RetryableError("discord status=%s path=%s" % (status, path))
                return status, body

        try:
            return await retry_transient(_once, circuit_name="discord")
        except Exception as e:
            if not is_retryable_error(e):
                raise
            logger.error("discord request retry exhausted path=%s error=%s", path, e)
            return 0, None

    def _dsb_attachments(self, atts: list[Any], stickers: list[Any] | None = None) -> tuple[list[dict], str]:
        out: list[dict] = []
        for a in atts:
            a = ext_dict('discord attachment', a)
            url_raw = a.get("url")
            if not url_raw:
                url_raw = a.get("proxy_url")
            url = ext_str(url_raw, "url").strip()
            file_id = ext_id(a.get("id"), "id").strip()
            if not url and not file_id:
                continue
            filename = ext_str(a.get("filename"), "filename").strip()
            content_type = ext_str(a.get("content_type"), "content_type").strip()
            item: dict[str, Any] = {
                "type": attachment_type_from_meta(filename=filename, content_type=content_type),
                "file_id": file_id or url,
                "file_name": filename,
                "content_type": content_type,
            }
            if url:
                item["url"] = url
            size = a.get("size")
            if size is not None:
                item["file_size"] = ext_int(size, "size")
            out.append(item)
        for s in stickers or []:
            s = ext_dict('discord sticker', s)
            sid = ext_id(s.get("id"), "id").strip()
            name = ext_str(s.get("name"), "name").strip()
            if not sid and not name:
                continue
            fmt = s.get("format_type")
            if fmt is None:
                fmt = 1
            else:
                fmt = ext_int(fmt, "format_type")
            url_str, mime_type, animated = guidance.discord_sticker_url(
                sid, fmt, self._api_base()
            )
            item = {
                "type": "sticker",
                "file_id": sid,
                "file_name": name,
                "content_type": mime_type,
            }
            if url_str:
                item["url"] = url_str
            if animated and not url_str:
                item["is_animated"] = True
            out.append(item)
        kept, notes = guidance.filter_oversized_attachments("discord", out)
        return kept, "\n".join(notes) if notes else ""

    async def get_channel_info(self, *, token: str, channel_id: str) -> dict | None:
        channel_id = send_channel_id(channel_id)
        if not channel_id:
            return None
        api_base = self._api_base()
        if api_base:
            status, payload = await self._dsb_request(
                method="GET",
                token=token,
                path=f"/api/v9/channels/{channel_id}",
            )
            if status < 200 or status >= 300 or not isinstance(payload, dict):
                return None
            return {
                "id": (ext_id(payload.get("id"), "id") or channel_id).strip(),
                "type": ext_int(payload.get("type"), "type"),
                "guild_id": ext_id(payload.get("guild_id"), "guild_id").strip(),
                "parent_id": ext_id(payload.get("parent_id"), "parent_id").strip(),
            }
        import discord
        chan = await self._get_channel(discord=discord, token=token, channel_id=channel_id)
        if chan is None:
            return None
        channel_type = int(chan.type)
        guild = chan.guild
        guild_id = ext_id(guild.id, "id").strip() if guild is not None else ""
        parent_id = _discord_channel_parent_id(chan)
        return {
            "id": (ext_id(chan.id, "id") or channel_id).strip(),
            "type": channel_type,
            "guild_id": guild_id,
            "parent_id": parent_id,
        }

    async def group_message_notice(self, *, token: str) -> str:
        token = (token or "").strip()
        if not token:
            raise ExternalError("discord token required")
        api_base = self._api_base()
        if api_base:
            status, payload = await self._dsb_request(
                method="GET",
                token=token,
                path="/api/v9/applications/@me",
            )
            if status == 401:
                raise ExternalError("discord application unauthorized")
            if status < 200 or status >= 300 or not isinstance(payload, dict):
                raise ExternalError("discord application settings unavailable", reason="group_settings_unavailable")
            return discord_group_notice(_discord_application_flags(payload))
        import discord
        intents = discord.Intents.none()
        client = discord.Client(intents=intents)
        try:
            try:
                await client.login(token)
                info = await client.application_info()
            except (discord.LoginFailure, discord.HTTPException) as e:
                raise ExternalError("discord application settings failed: %s" % e) from e
        finally:
            try:
                await client.close()
            except Exception as e:
                logger.error("unexpected where=discord_app_info_close error=%s", e, exc_info=e)
        flags = int(info.flags.value)
        return discord_group_notice(flags)

    async def resolve_message_chat_thread(self, *, token: str, channel_id: str, channel_meta=None):
        channel_id = (channel_id or "").strip()
        if channel_meta is None:
            channel_meta = {}
        parent_id = ext_id(channel_meta.get("parent_id"), "parent_id").strip()
        channel_type = channel_meta.get("type", 0)
        if not parent_id and channel_id:
            info = await self.get_channel_info(token=token, channel_id=channel_id)
            if info is not None:
                parent_id = ext_id(info.get("parent_id"), "parent_id").strip()
                channel_type = info.get("type", channel_type)
        return resolve_chat_and_thread(channel_id, parent_id, channel_type)

    async def send_typing(self, *, token: str, channel_id: str) -> bool:
        channel_id = send_channel_id(channel_id)
        api_base = self._api_base()
        if api_base:
            if not channel_id:
                return False
            status, _ = await self._dsb_request(
                method="POST",
                token=token,
                path=f"/api/v9/channels/{channel_id}/typing",
            )
            return 200 <= int(status) < 300
        import discord
        chan = await self._get_channel(discord=discord, token=token, channel_id=channel_id)
        if chan is None:
            return False
        await chan.trigger_typing()
        return True

    async def _get_channel(self, *, discord: Any, token: str, channel_id: str) -> Any | None:
        channel_id = (channel_id or "").strip()
        if not channel_id:
            return None
        cid = int(channel_id)
        client = self._find_client()
        if client is not None:
            ch = client.get_channel(cid)
            if ch is not None:
                return ch
        intents = discord.Intents.none()
        intents.guilds = True
        temp = discord.Client(intents=intents)
        try:
            await temp.login(token)
            ch = await temp.fetch_channel(cid)
            return ch
        finally:
            try:
                await temp.close()
            except Exception as e:
                logger.error("unexpected where=discord_temp_client_close error=%s", e, exc_info=e)

    def _discord_allowed_mentions_obj(self, discord: Any) -> Any:
        return discord.AllowedMentions(
            everyone=False,
            users=True,
            roles=False,
            replied_user=True,
        )

    async def send_message(
        self,
        *,
        token: str,
        channel_id: str,
        content: str,
        file_bytes: bytes | None = None,
        filename: str | None = None,
        reply_to: str | None = None,
        keyboard=None,
        voice: bool = False,
    ) -> bool:
        channel_id = send_channel_id(channel_id)
        api_base = self._api_base()
        if file_bytes is not None and len(file_bytes) > 20 * 1024 * 1024:
            return False
        if voice and file_bytes is not None:
            converted = _transcode_discord_voice(file_bytes)
            if converted is None:
                voice = False
            else:
                file_bytes = converted
                filename = "voice-message.ogg"
        chunks = guidance.prepare_outbound_text("discord", (content or ""))
        components = discord_components(keyboard) if parse_keyboard(keyboard) else None
        if file_bytes is None and not chunks and components is None:
            return False
        if not chunks:
            chunks = [""]
        if api_base:
            channel_id = (channel_id or "").strip()
            if not channel_id:
                return False
            last_ok = False
            for idx, chunk in enumerate(chunks):
                use_reply = reply_to if idx == 0 else None
                use_file = file_bytes if idx == 0 else None
                use_name = filename if idx == 0 else None
                ok = await self._dsb_send_message(
                    token=token,
                    channel_id=channel_id,
                    content="" if voice else chunk,
                    reply_to=use_reply,
                    file_bytes=use_file,
                    filename=use_name,
                    components=components if idx == 0 else None,
                    voice=voice and idx == 0,
                )
                if not ok:
                    return False
                last_ok = True
            return last_ok
        import discord
        chan = await self._get_channel(discord=discord, token=token, channel_id=channel_id)
        if chan is None:
            return False
        allowed = self._discord_allowed_mentions_obj(discord)
        for idx, chunk in enumerate(chunks):
            kwargs: dict[str, Any] = {"content": chunk, "allowed_mentions": allowed}
            if idx == 0 and components:
                view = discord.ui.View(timeout=None)
                for row_idx, row in enumerate(components):
                    for item in row.get("components") or []:
                        label = ext_str(item.get("label"), "label")
                        style = ext_int(item.get("style"), "style")
                        if style == 5:
                            btn = discord.ui.Button(style=discord.ButtonStyle.link, label=label, url=ext_str(item.get("url"), "url"))
                        else:
                            btn = discord.ui.Button(
                                style=discord.ButtonStyle.primary,
                                label=label,
                                custom_id=ext_str(item.get("custom_id"), "custom_id"),
                            )
                        btn.row = row_idx
                        view.add_item(btn)
                kwargs["view"] = view
            if idx == 0 and reply_to:
                ref_id = int(reply_to.strip())
                kwargs["reference"] = discord.MessageReference(
                    message_id=ref_id,
                    channel_id=int(chan.id) if chan.id else None,
                    fail_if_not_exists=False,
                )
            if idx == 0 and file_bytes is not None:
                send_name = "voice-message.ogg" if voice else (filename or "file")
                kwargs["file"] = discord.File(fp=io.BytesIO(file_bytes), filename=send_name)
                if voice:
                    kwargs["content"] = ""
                    kwargs["flags"] = discord.MessageFlags(voice=True)
            try:
                await chan.send(**kwargs)
            except discord.HTTPException as e:
                if idx == 0 and reply_to and (e.code == 10008 or "system message" in str(e).lower() or "unknown message" in str(e).lower()):
                    kwargs.pop("reference", None)
                    await chan.send(**kwargs)
                else:
                    raise
        return True

    async def _dsb_post(self, url, *, headers, json_body=None, form_fields=None):
        async def _once():
            data = None
            if form_fields is not None:
                data = aiohttp.FormData()
                for name, value, options in form_fields:
                    data.add_field(name, value, **options)
            async with self.session.post(url, headers=headers, json=json_body, data=data) as resp:
                status = int(resp.status)
                err_body = await resp.text()
                if is_transient_status(status):
                    raise RetryableError("discord send status=%s" % status)
                return status, err_body

        try:
            return await retry_transient(_once, circuit_name="discord")
        except Exception as e:
            if not is_retryable_error(e):
                raise
            logger.error("discord send retry exhausted error=%s", e)
            return 0, str(e)

    async def _dsb_send_message(
        self,
        *,
        token: str,
        channel_id: str,
        content: str,
        reply_to: str | None,
        file_bytes: bytes | None,
        filename: str | None,
        components=None,
        voice: bool = False,
    ) -> bool:
        base = self._api_base()
        if not base:
            return False
        url = discord_rest_url(base, f"/api/v9/channels/{channel_id}/messages")
        headers = {"Authorization": "Bot " + (token or "").strip()}
        allowed = guidance.default_discord_allowed_mentions()
        if file_bytes is None:
            payload: dict[str, Any] = {
                "content": (content or ""),
                "allowed_mentions": allowed,
            }
            if reply_to:
                payload["message_reference"] = {
                    "message_id": reply_to.strip(),
                    "channel_id": channel_id,
                    "fail_if_not_exists": False,
                }
            if components:
                payload["components"] = components
            status, err_body = await self._dsb_post(url, headers=headers, json_body=payload)
            if 200 <= status < 300:
                return True
            if reply_to and self._is_unknown_reply_error(status, err_body):
                payload.pop("message_reference", None)
                status, _err_body = await self._dsb_post(url, headers=headers, json_body=payload)
                return 200 <= status < 300
            return False
        payload = {
            "content": (content or ""),
            "allowed_mentions": allowed,
        }
        if reply_to:
            payload["message_reference"] = {
                "message_id": reply_to.strip(),
                "channel_id": channel_id,
                "fail_if_not_exists": False,
            }
        if components:
            payload["components"] = components
        send_name = filename or "file"
        send_type = mimetypes.guess_type(send_name)[0] or "application/octet-stream"
        if voice:
            payload["content"] = ""
            payload["flags"] = 8192
            send_name = "voice-message.ogg"
            send_type = "audio/ogg"
            payload["attachments"] = [
                {
                    "id": 0,
                    "filename": send_name,
                    "duration_secs": round(_ogg_opus_duration(file_bytes), 2),
                    "waveform": base64.b64encode(b"\x80" * 256).decode("ascii"),
                }
            ]

        def _fields():
            return (
                ("payload_json", json.dumps(payload), {"content_type": "application/json"}),
                ("files[0]", file_bytes, {"filename": send_name, "content_type": send_type}),
            )

        status, err_body = await self._dsb_post(url, headers=headers, form_fields=_fields())
        if 200 <= status < 300:
            return True
        if reply_to and self._is_unknown_reply_error(status, err_body):
            payload.pop("message_reference", None)
            status, _err_body = await self._dsb_post(url, headers=headers, form_fields=_fields())
            return 200 <= status < 300
        return False

    @staticmethod
    def _is_unknown_reply_error(status: int, body: str) -> bool:
        raw = (body or "").lower()
        if "10008" in raw or "unknown message" in raw:
            return True
        return "50035" in raw and "system message" in raw

    async def download(self, *, token: str, url: str) -> bytes | None:
        url = (url or "").strip()
        if not url:
            return None
        limit = guidance.max_media_bytes("discord", "document")
        max_redirects = 10
        current = url
        for _ in range(max_redirects + 1):
            headers = {}
            if not _discord_cdn_host(current):
                tok = (token or "").strip()
                if tok:
                    if tok.startswith("Bot ") or tok.startswith("Bearer "):
                        headers["Authorization"] = tok
                    else:
                        headers["Authorization"] = "Bot " + tok
            async with self.session.get(current, headers=headers, allow_redirects=False) as resp:
                status = int(resp.status)
                if status in (301, 302, 303, 307, 308):
                    loc = ext_str(resp.headers.get("Location"), "Location").strip()
                    if not loc:
                        return None
                    from urllib.parse import urljoin

                    current = urljoin(current, loc)
                    continue
                if status != 200:
                    return None
                cl = resp.headers.get("Content-Length")
                if cl:
                    try:
                        guidance.check_media_size("discord", int(cl), "document")
                    except guidance.MediaTooLargeError:
                        return None
                data = await resp.content.read(limit + 1)
                if len(data) > limit:
                    return None
                return data
        return None

    async def _relay_discord_voice(
        self, *, acct_id: str, relay_chat_id: str, user_id: str, event_type: str, content: dict
    ) -> None:
        if not (acct_id and relay_chat_id):
            return
        model_id = await self.conductor.db.lookup_model_for_chat(acct_id=acct_id, chat_id=open_chat_id(relay_chat_id))
        if not model_id:
            mapped = await self.conductor.ensure_chat_mapping(
                acct_id=acct_id,
                chat_id=open_chat_id(relay_chat_id),
                chat_type="discord",
                carrier_user_id=user_id or None,
            )
            model_id = mapped.model_id
        if not model_id:
            return
        update_id = f"{event_type}:{relay_chat_id}:{int(time.time() * 1000)}"
        body = {
            "discord_voice": {
                "type": event_type,
                "content": content,
            },
            "msg_id": update_id,
            "platform": "discord",
            "user_id": user_id or "",
            "chat_type": "voice",
        }
        await self.conductor.deliver_inbound(
            model_id=model_id,
            acct_id=acct_id,
            chat_id=relay_chat_id,
            request_id=update_id,
            body=body,
        )

    async def _resolve_guild_id(self, *, acct_id: str, token: str, channel_id: str, relay_chat_id: str) -> str:
        channel_id = (channel_id or "").strip()
        relay_chat_id = (relay_chat_id or "").strip()
        if channel_id:
            info = await self.get_channel_info(token=token, channel_id=channel_id)
            if info is not None:
                guild_id = ext_id(info.get("guild_id"), "guild_id").strip()
                if guild_id:
                    return guild_id
        st = self._acct_state(acct_id) if acct_id else None
        client = st.client if st is not None else self._find_client()
        if client is not None and channel_id:
            ch = client.get_channel(int(channel_id))
            if ch is None:
                ch = await client.fetch_channel(int(channel_id))
            guild = ch.guild
            guild_id = ext_id(guild.id, "id").strip() if guild is not None else ""
            if guild_id:
                return guild_id
        if st is not None:
            for gid, pending in st.voice_pending.items():
                if ext_str(pending.get("relay_chat_id"), "relay_chat_id").strip() == relay_chat_id:
                    return ext_id(gid, "guild_id")
        return ""

    async def _ensure_dsb_gateway(self, *, acct_id: str, token: str) -> bool:
        if not (acct_id and token):
            return False
        st = self._acct_state(acct_id)
        if (
            st.gateway_ws is not None
            and not st.gateway_ws.closed
            and st.gateway_ready.is_set()
            and st.gateway_token == token
        ):
            return True
        if st.gateway_task is not None and not st.gateway_task.done():
            if st.gateway_token and st.gateway_token != token:
                st.gateway_task.cancel()
                try:
                    await st.gateway_task
                except Exception as e:
                    logger.error("unexpected where=discord_gateway_task_cancel acct_id=%s error=%s", acct_id, e, exc_info=e)
                st.gateway_task = None
            else:
                try:
                    await asyncio.wait_for(st.gateway_ready.wait(), timeout=10)
                    return (
                        st.gateway_ws is not None
                        and not st.gateway_ws.closed
                        and st.gateway_token == token
                    )
                except asyncio.TimeoutError:
                    return False
        st.gateway_token = token
        st.gateway_ready.clear()
        st.gateway_task = asyncio.create_task(self._dsb_gateway_loop(acct_id=acct_id, token=token))
        try:
            await asyncio.wait_for(st.gateway_ready.wait(), timeout=10)
        except asyncio.TimeoutError:
            return False
        return st.gateway_ws is not None and not st.gateway_ws.closed and st.gateway_token == token

    async def _handle_dsb_dispatch(self, st, *, acct_id: str, token: str, event_type: str, data: dict) -> None:
        if event_type in ("CHANNEL_CREATE", "CHANNEL_UPDATE", "THREAD_CREATE", "THREAD_UPDATE"):
            self._note_gateway_channel(st, data)
            if event_type == "THREAD_CREATE":
                channel_id = ext_id(data.get("id"), "id").strip()
                if ext_bool(data.get("newly_created"), "newly_created"):
                    st.backlog_threads.discard(channel_id)
                    async with self._dsb_msg_lock:
                        cursors = self._get_cursors(acct_id)
                        if channel_id and not (cursors.get(channel_id) or "").strip():
                            cursors[channel_id] = "0"
                            save_cursor = True
                        else:
                            save_cursor = False
                    if save_cursor:
                        await self._save_runtime_state(acct_id, mode="dsb")
                elif channel_id:
                    st.backlog_threads.add(channel_id)
            return
        if event_type == "GUILD_CREATE":
            if not ext_bool(data.get("unavailable"), "unavailable"):
                self._note_gateway_threads(st, data.get("channels"))
                self._note_gateway_threads(st, data.get("threads"))
            return
        if event_type == "THREAD_LIST_SYNC":
            self._note_gateway_threads(st, data.get("threads"))
            return
        if event_type == "MESSAGE_CREATE":
            if ext_id(data.get("guild_id"), "guild_id").strip():
                await self._deliver_dsb_guild_thread_message(st, acct_id=acct_id, token=token, data=data)
            else:
                await self._deliver_dsb_direct_message(st, acct_id=acct_id, token=token, data=data)
            return
        if event_type == "MESSAGE_REACTION_ADD":
            await self._deliver_reaction_payload(acct_id=acct_id, data=data, removed=False)
            return
        if event_type == "MESSAGE_REACTION_REMOVE":
            await self._deliver_reaction_payload(acct_id=acct_id, data=data, removed=True)
            return
        if event_type == "INTERACTION_CREATE":
            if ext_int(data.get("type"), "type") != 3:
                return
            interaction_id = ext_id(data.get("id"), "id").strip()
            interaction_token = ext_str(data.get("token"), "token").strip()
            await self._ack_dsb_interaction(
                token=token,
                interaction_id=interaction_id,
                interaction_token=interaction_token,
            )
            inner = data.get("data")
            inner = ext_dict("discord interaction data", inner) if inner is not None else {}
            custom_id = ext_str(inner.get("custom_id"), "custom_id").strip()
            member = data.get("member")
            user = data.get("user")
            user_id = ""
            sender_name = ""
            sender_username = ""
            if member is not None:
                member = ext_dict("discord interaction member", member)
                muser = member.get("user")
                if muser is not None:
                    muser = ext_dict("discord interaction member user", muser)
                    user_id = ext_id(muser.get("id"), "id").strip()
                    sender_name, sender_username = _discord_author_sender(muser)
            if not user_id and user is not None:
                user = ext_dict("discord interaction user", user)
                user_id = ext_id(user.get("id"), "id").strip()
                sender_name, sender_username = _discord_author_sender(user)
            message = data.get("message")
            message_id = ""
            parent_id = ""
            channel_type = 0
            if message is not None:
                message = ext_dict("discord interaction message", message)
                message_id = ext_id(message.get("id"), "id").strip()
            await self._deliver_component_interaction(
                acct_id=acct_id,
                interaction_id=interaction_id,
                user_id=user_id,
                channel_id=ext_id(data.get("channel_id"), "channel_id").strip(),
                message_id=message_id,
                custom_id=custom_id,
                parent_id=parent_id,
                channel_type=channel_type,
                sender_name=sender_name,
                sender_username=sender_username,
            )
            return
        if event_type == "READY":
            user = data.get("user")
            user = ext_dict('discord READY user', user)
            st.gateway_user_id = ext_id(user.get("id"), "id").strip()
            session_id = ext_str(data.get("session_id"), "session_id").strip()
            if session_id:
                st.gateway_session_id = session_id
            resume_url = ext_str(data.get("resume_gateway_url"), "resume_gateway_url").strip()
            if resume_url:
                st.gateway_resume_url = resume_url
            st.gateway_ready.set()
            return
        if event_type == "RESUMED":
            st.gateway_ready.set()
            return
        if event_type == "VOICE_STATE_UPDATE":
            user_id = ext_id(data.get("user_id"), "user_id").strip()
            if st.gateway_user_id and user_id and user_id != st.gateway_user_id:
                return
            guild_id = ext_id(data.get("guild_id"), "guild_id").strip()
            if not guild_id:
                return
            channel_id = ext_id(data.get("channel_id"), "channel_id").strip()
            session_id = ext_str(data.get("session_id") or st.gateway_session_id, "session_id").strip()
            pending = st.voice_pending.get(guild_id)
            if pending is None:
                pending = {"guild_id": guild_id}
            pending["user_id"] = user_id or st.gateway_user_id
            pending["session_id"] = session_id
            pending["channel_id"] = channel_id
            if not pending.get("relay_chat_id") and channel_id:
                pending["relay_chat_id"] = channel_id
            st.voice_pending[guild_id] = pending
            if not channel_id:
                await self._relay_discord_voice(
                    acct_id=acct_id,
                    relay_chat_id=(ext_str(pending.get("relay_chat_id"), "relay_chat_id") or guild_id),
                    user_id=ext_id(pending.get("user_id"), "user_id"),
                    event_type="discord.voice.left",
                    content={"guild_id": guild_id},
                )
                return
            self._maybe_emit_credentials(acct_id, guild_id)
            return
        if event_type == "VOICE_SERVER_UPDATE":
            guild_id = ext_id(data.get("guild_id"), "guild_id").strip()
            endpoint = ext_str(data.get("endpoint"), "endpoint").strip()
            token_v = ext_str(data.get("token"), "token").strip()
            if not (guild_id and endpoint and token_v):
                return
            pending = st.voice_pending.get(guild_id)
            if pending is None:
                pending = {"guild_id": guild_id}
            pending["endpoint"] = endpoint
            pending["token"] = token_v
            if not pending.get("user_id"):
                pending["user_id"] = st.gateway_user_id
            if not pending.get("session_id"):
                pending["session_id"] = st.gateway_session_id or "dsb_session"
            st.voice_pending[guild_id] = pending
            self._maybe_emit_credentials(acct_id, guild_id)

    async def _dsb_gateway_url(self, *, token: str) -> str:
        base = self._api_base()
        status, payload = await self._dsb_request(method="GET", token=token, path="/api/v9/gateway/bot")
        gateway_url = ""
        if status == 200 and isinstance(payload, dict):
            gateway_url = ext_str(payload.get("url"), "url").strip()
        if not gateway_url:
            gateway_url = discord_gateway_fallback(base)
        return gateway_url

    async def _dsb_gateway_loop(self, *, acct_id: str, token: str) -> None:
        st = self._acct_state(acct_id)
        base = self._api_base()
        if not base:
            return
        dispatch_q: asyncio.Queue = asyncio.Queue()

        async def _drain_dispatch() -> None:
            while True:
                item = await dispatch_q.get()
                if item is None:
                    return
                event_type, data = item
                try:
                    await self._handle_dsb_dispatch(
                        st, acct_id=acct_id, token=token, event_type=event_type, data=data,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error("unexpected where=discord_gateway_dispatch acct_id=%s error=%s", acct_id, e, exc_info=e)

        worker = asyncio.create_task(_drain_dispatch())
        try:
            connect_mode = "identify"
            while st.gateway_token == token and not self.conductor.poller_stop.is_set():
                gateway_url = await self._dsb_gateway_url(token=token)
                connect_mode = await self._dsb_gateway_session(
                    st,
                    acct_id=acct_id,
                    token=token,
                    gateway_url=gateway_url,
                    dispatch_q=dispatch_q,
                    connect_mode=connect_mode,
                )
                if connect_mode == "stop" or st.gateway_token != token or self.conductor.poller_stop.is_set():
                    return
                await asyncio.sleep(1)
        finally:
            dispatch_q.put_nowait(None)
            worker.cancel()
            try:
                await worker
            except (asyncio.CancelledError, Exception):
                pass

    async def _dsb_gateway_session(self, st, *, acct_id: str, token: str, gateway_url: str, dispatch_q: asyncio.Queue, connect_mode: str) -> str:
        ws = None
        heartbeat_task = None
        next_mode = "resume" if connect_mode == "resume" else "identify"
        try:
            url = gateway_url
            if connect_mode == "resume" and st.gateway_resume_url:
                url = st.gateway_resume_url
            try:
                ws = await self.session.ws_connect(url, heartbeat=None, timeout=ws_timeout())
            except (aiohttp.ClientError, OSError) as e:
                logger.error("unexpected where=discord_gateway_connect acct_id=%s error=%s", acct_id, e, exc_info=e)
                return next_mode
            st.gateway_ws = ws
            async for msg in ws:
                if msg.type != aiohttp.WSMsgType.TEXT:
                    if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
                    continue
                try:
                    frame = json.loads(msg.data)
                except json.JSONDecodeError as exc:
                    logger.error("discord gateway malformed JSON acct_id=%s error=%s", acct_id, exc)
                    raise ExternalError("discord gateway malformed JSON") from exc
                frame = ext_dict('discord gateway frame', frame)
                op = frame.get("op")
                if op == 10:
                    d = frame.get("d")
                    d = ext_dict('discord hello d', d)
                    interval = float(d.get("heartbeat_interval") or 0) / 1000.0
                    st.gateway_hb_ack = True
                    if connect_mode == "resume" and st.gateway_session_id and st.gateway_last_seq is not None:
                        await ws.send_json({
                            "op": 6,
                            "d": {
                                "token": token,
                                "session_id": st.gateway_session_id,
                                "seq": st.gateway_last_seq,
                            },
                        })
                    else:
                        await ws.send_json({
                            "op": 2,
                            "d": {
                                "token": token,
                                "properties": {"os": "linux", "browser": "station", "device": "station"},
                                "intents": 1 | 128 | 512 | 1024 | 4096 | 8192 | 32768,
                            },
                        })
                    if interval > 0:
                        if heartbeat_task is not None:
                            heartbeat_task.cancel()
                        async def _hb(ws=ws, interval=interval):
                            while True:
                                await asyncio.sleep(interval)
                                if not st.gateway_hb_ack:
                                    try:
                                        await ws.close(code=4000)
                                    except Exception:
                                        return
                                    return
                                st.gateway_hb_ack = False
                                await ws.send_json({"op": 1, "d": st.gateway_last_seq})
                        heartbeat_task = asyncio.create_task(_hb())
                    continue
                if op == 11:
                    st.gateway_hb_ack = True
                    continue
                if op == 7:
                    break
                if op == 9:
                    if frame.get("d") is not True:
                        st.gateway_session_id = ""
                        st.gateway_last_seq = None
                        st.gateway_resume_url = ""
                    break
                if op == 1:
                    st.gateway_hb_ack = False
                    await ws.send_json({"op": 1, "d": st.gateway_last_seq})
                    continue
                if op != 0:
                    continue
                raw_seq = frame.get("s")
                if isinstance(raw_seq, bool):
                    raw_seq = None
                if isinstance(raw_seq, int):
                    st.gateway_last_seq = raw_seq
                elif isinstance(raw_seq, float) and raw_seq.is_integer():
                    st.gateway_last_seq = int(raw_seq)
                event_type = ext_str(frame.get("t"), "t").strip()
                data = frame.get("d")
                data = ext_dict('discord dispatch d', data)
                if event_type in ("READY", "RESUMED"):
                    await self._handle_dsb_dispatch(
                        st, acct_id=acct_id, token=token, event_type=event_type, data=data,
                    )
                    continue
                dispatch_q.put_nowait((event_type, data))
            code = ws.close_code if ws is not None else None
            if code in (4004, 4010, 4011, 4012, 4013, 4014):
                next_mode = "stop"
            elif code in (4007, 4009) or not st.gateway_session_id or st.gateway_last_seq is None:
                st.gateway_session_id = ""
                st.gateway_last_seq = None
                st.gateway_resume_url = ""
                next_mode = "identify"
            else:
                next_mode = "resume"
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("unexpected where=discord_gateway acct_id=%s error=%s", acct_id, e, exc_info=e)
        finally:
            if heartbeat_task is not None:
                heartbeat_task.cancel()
            st.gateway_ready.clear()
            if ws is not None and not ws.closed:
                try:
                    await ws.close(code=4000)
                except Exception as e:
                    logger.error("unexpected where=discord_gateway_close acct_id=%s error=%s", acct_id, e, exc_info=e)
            if st.gateway_ws is ws:
                st.gateway_ws = None
        return next_mode

    async def voice_state_update(
        self,
        *,
        token: str,
        acct_id: str,
        relay_chat_id: str,
        guild_id: str | None,
        channel_id: str | None,
        self_mute: bool = False,
        self_deaf: bool = False,
    ) -> bool:
        guild_id = (guild_id or "").strip()
        channel_id = (channel_id or "").strip()
        relay_chat_id = (relay_chat_id or "").strip()
        token = (token or "").strip()
        if not acct_id:
            return False
        st = self._acct_state(acct_id)
        if not guild_id:
            guild_id = await self._resolve_guild_id(
                acct_id=acct_id, token=token, channel_id=channel_id, relay_chat_id=relay_chat_id
            )
        if not guild_id:
            return False
        if not relay_chat_id:
            relay_chat_id = channel_id or guild_id
        pending = st.voice_pending.get(guild_id)
        if pending is None:
            pending = {}
        pending.update({
            "relay_chat_id": relay_chat_id,
            "guild_id": guild_id,
            "channel_id": channel_id,
        })
        if not channel_id:
            pending["session_id"] = ""
            pending["endpoint"] = ""
            pending["token"] = ""
        st.voice_pending[guild_id] = pending

        client = st.client
        if client is not None and client.ws is not None:
            await client.ws.voice_state(
                int(guild_id),
                int(channel_id) if channel_id else None,
                self_mute=self_mute,
                self_deaf=self_deaf,
            )
            return True

        if self._api_base():
            if not await self._ensure_dsb_gateway(acct_id=acct_id, token=token):
                return False
            ws = st.gateway_ws
            if ws is None or ws.closed:
                return False
            await ws.send_json({
                "op": 4,
                "d": {
                    "guild_id": guild_id,
                    "channel_id": channel_id or None,
                    "self_mute": self_mute,
                    "self_deaf": self_deaf,
                },
            })
            return True
        return False

    def _maybe_emit_credentials(self, acct_id: str, guild_id: str) -> None:
        if not acct_id:
            return
        st = self._acct_state(acct_id)
        pending = st.voice_pending.get(guild_id)
        if pending is None:
            return
        relay_chat_id = ext_str(pending.get("relay_chat_id"), "relay_chat_id").strip()
        channel_id = ext_id(pending.get("channel_id"), "channel_id").strip()
        session_id = ext_str(pending.get("session_id"), "session_id").strip()
        endpoint = ext_str(pending.get("endpoint"), "endpoint").strip()
        token = ext_str(pending.get("token"), "token").strip()
        user_id = ext_id(pending.get("user_id"), "user_id").strip()
        if not (relay_chat_id and channel_id and session_id and endpoint and token and user_id):
            return
        asyncio.create_task(
            self._relay_discord_voice(
                acct_id=acct_id,
                relay_chat_id=relay_chat_id,
                user_id=user_id,
                event_type="discord.voice.credentials",
                content={
                    "guild_id": guild_id,
                    "channel_id": channel_id,
                    "endpoint": endpoint,
                    "token": token,
                    "session_id": session_id,
                    "user_id": user_id,
                },
            )
        )

    async def poller_loop(self, acct_id: str) -> None:
        acct = await self.conductor.db.get_local_account(acct_id)
        if not acct:
            return
        await self._load_runtime_state(acct_id)
        enc = (acct.get("encrypted_token") or "").strip()
        token, _ = decrypt_if_encrypted(self.conductor.private_key, enc)
        token = (token or "").strip()
        if not token:
            return

        st = self._acct_state(acct_id)
        api_base = self._api_base()
        if api_base:
            await self._save_runtime_state(acct_id, mode="dsb")
            await self._ensure_dsb_gateway(acct_id=acct_id, token=token)
            try:
                await self._dsb_poller_loop(acct_id=acct_id, token=token)
            finally:
                if st.gateway_task is not None:
                    st.gateway_task.cancel()
                    try:
                        await st.gateway_task
                    except Exception as e:
                        logger.error("unexpected where=discord_poller_gateway_cancel acct_id=%s error=%s", acct_id, e, exc_info=e)
                    st.gateway_task = None
                if st.gateway_ws is not None:
                    try:
                        await st.gateway_ws.close()
                    except Exception as e:
                        logger.error("unexpected where=discord_poller_gateway_close acct_id=%s error=%s", acct_id, e, exc_info=e)
                    st.gateway_ws = None
            return
        await self._save_runtime_state(acct_id, mode="gateway")
        while not self.conductor.poller_stop.is_set():
            try:
                await self._gateway_client_session(acct_id=acct_id, token=token)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("unexpected where=discord_gateway_session acct_id=%s error=%s", acct_id, e, exc_info=e)
            if self.conductor.poller_stop.is_set():
                break
            try:
                await asyncio.wait_for(self.conductor.poller_stop.wait(), timeout=3.0)
                break
            except asyncio.TimeoutError:
                pass

    async def _gateway_client_session(self, *, acct_id: str, token: str) -> None:
        import discord

        st = self._acct_state(acct_id)
        intents = discord.Intents.none()
        intents.guilds = True
        intents.messages = True
        intents.message_content = True
        intents.reactions = True
        intents.voice_states = True
        client = discord.Client(intents=intents)
        st.client = client

        @client.event
        async def on_message(message):
            if message.author.bot or message.author.system:
                return
            if client.user is not None and message.author.id == client.user.id:
                return
            if message.type not in (discord.MessageType.default, discord.MessageType.reply):
                return
            channel_id = ext_id(message.channel.id, "channel.id").strip()
            if not channel_id:
                return
            parent_id = _discord_channel_parent_id(message.channel)
            channel_type = int(message.channel.type)
            chat_id, open_id, chat_type = resolve_chat_and_thread(channel_id, parent_id, channel_type)
            msg_id = ext_id(message.id, "id").strip()
            author_id = ext_id(message.author.id, "author.id").strip() or None
            content = ext_str(message.content, "content").strip()
            force = content.startswith("/start")
            model_id = None if force else (await self.conductor.db.lookup_model_for_chat(acct_id=acct_id, chat_id=open_id))
            if not model_id:
                mapped = await self.conductor.ensure_chat_mapping(acct_id=acct_id, chat_id=open_id, chat_type="discord", carrier_user_id=author_id, force=force)
                model_id = mapped.model_id
            if not model_id:
                return
            attachments, size_note = self._event_attachments(
                list(message.attachments or []),
                stickers=list(message.stickers or []),
            )
            if size_note:
                content = f"{content}\n{size_note}".strip() if content else size_note
            reply_to = ""
            reply_snippet = ""
            ref = message.reference
            if ref is not None:
                reply_to = ext_id(ref.message_id, "message_id").strip()
                resolved = ref.resolved
                if resolved is not None:
                    reply_snippet = ext_str(resolved.content, "content").strip()
                    if not reply_snippet:
                        atts = resolved.attachments or []
                        stickers = resolved.stickers or []
                        if atts or stickers:
                            reply_snippet = "(attachment)"
            reply_to_text = guidance.telegram_reply_snippet(reply_snippet)
            content = guidance.inject_reply_context(content, reply_to_text)
            if not content and attachments:
                for a in attachments:
                    if a.get("type") == "sticker":
                        name = ext_str(a.get("file_name"), "file_name").strip()
                        if a.get("is_animated") or not ext_str(a.get("url"), "url").strip():
                            content = guidance.animated_sticker_injection(name)
                        else:
                            content = guidance.sticker_injection(name or "a sticker", "", "")
                        break
            body = {
                "method": "send_message",
                "params": {"text": content},
                "text": content,
                "caption": "",
                "attachments": attachments,
                "msg_id": msg_id,
                "platform": "discord",
                "user_id": author_id or "",
            }
            sender_name, sender_username = _discord_user_sender(message.author)
            apply_sender(body, name=sender_name, username=sender_username)
            if reply_to:
                body["reply_to"] = reply_to
            if reply_to_text:
                body["reply_to_text"] = reply_to_text
            if chat_type:
                body["chat_type"] = chat_type
            bot_id = ""
            if client.user is not None:
                bot_id = ext_id(client.user.id, "user.id").strip()
            ref_author = ""
            if ref is not None and getattr(ref, "resolved", None) is not None:
                resolved_author = getattr(ref.resolved, "author", None)
                if resolved_author is not None:
                    ref_author = ext_id(resolved_author.id, "id").strip()
            body["mentioned"] = discord_mentions_bot(
                bot_id=bot_id,
                content=ext_str(message.content, "content"),
                mention_ids=_discord_mention_ids_from_users(message.mentions),
                referenced_author_id=ref_author,
            )
            if not content and not attachments:
                body["raw"] = {
                    "id": msg_id,
                    "channel_id": channel_id,
                    "content": ext_str(message.content, "content"),
                    "type": int(message.type),
                    "author": {"id": author_id or "", "bot": bool(message.author.bot)},
                }
            await self.conductor.deliver_inbound(
                model_id=model_id,
                acct_id=acct_id,
                chat_id=chat_id,
                request_id=msg_id,
                body=body,
            )

        @client.event
        async def on_interaction(interaction):
            try:
                itype = int(interaction.type)
            except (TypeError, ValueError):
                return
            if itype != 3:
                return
            try:
                if not interaction.response.is_done():
                    await interaction.response.defer()
            except Exception as e:
                logger.error("unexpected where=discord_interaction_ack acct_id=%s error=%s", acct_id, e, exc_info=e)
            data = interaction.data or {}
            if not isinstance(data, dict):
                data = {}
            custom_id = ext_str(data.get("custom_id"), "custom_id").strip()
            user = interaction.user
            user_id = ext_id(user.id, "user.id").strip() if user is not None else ""
            sender_name, sender_username = _discord_user_sender(user)
            channel = interaction.channel
            channel_id = ext_id(interaction.channel_id, "channel_id").strip()
            parent_id = _discord_channel_parent_id(channel) if channel is not None else ""
            channel_type = int(channel.type) if channel is not None else 0
            message = interaction.message
            message_id = ext_id(message.id, "id").strip() if message is not None else ""
            await self._deliver_component_interaction(
                acct_id=acct_id,
                interaction_id=ext_id(interaction.id, "id").strip(),
                user_id=user_id,
                channel_id=channel_id,
                message_id=message_id,
                custom_id=custom_id,
                parent_id=parent_id,
                channel_type=channel_type,
                sender_name=sender_name,
                sender_username=sender_username,
            )

        @client.event
        async def on_raw_reaction_add(payload):
            await self._deliver_reaction(acct_id=acct_id, payload=payload, removed=False)

        @client.event
        async def on_raw_reaction_remove(payload):
            await self._deliver_reaction(acct_id=acct_id, payload=payload, removed=True)

        @client.event
        async def on_message_edit(before, after):
            return

        @client.event
        async def on_voice_state_update(member, before, after):
            if client.user is None or member.id != client.user.id:
                return
            guild_id = ext_id(member.guild.id, "guild.id").strip()
            if not guild_id:
                return
            channel_id = ext_id(after.channel.id, "channel.id").strip() if after.channel is not None else ""
            session_id = ext_id(after.session_id, "session_id").strip()
            pending = st.voice_pending.get(guild_id)
            if pending is None:
                pending = {"guild_id": guild_id}
            pending["user_id"] = ext_id(member.id, "member.id")
            pending["session_id"] = session_id
            pending["channel_id"] = channel_id
            if not pending.get("relay_chat_id") and channel_id:
                pending["relay_chat_id"] = channel_id
            st.voice_pending[guild_id] = pending
            if not channel_id:
                await self._relay_discord_voice(
                    acct_id=acct_id,
                    relay_chat_id=(ext_str(pending.get("relay_chat_id"), "relay_chat_id") or guild_id),
                    user_id=ext_id(member.id, "member.id"),
                    event_type="discord.voice.left",
                    content={"guild_id": guild_id},
                )
                return
            self._maybe_emit_credentials(acct_id, guild_id)

        state = client._connection
        original_voice_server = state.parse_voice_server_update

        def parse_voice_server_update(data):
            data = ext_dict('discord voice_server_update', data)
            guild_id = ext_id(data.get("guild_id"), "guild_id").strip()
            endpoint = ext_str(data.get("endpoint"), "endpoint").strip()
            token_v = ext_str(data.get("token"), "token").strip()
            if guild_id and endpoint and token_v:
                pending = st.voice_pending.get(guild_id)
                if pending is None:
                    pending = {"guild_id": guild_id}
                pending["endpoint"] = endpoint
                pending["token"] = token_v
                if not pending.get("user_id") and client.user is not None:
                    pending["user_id"] = ext_id(client.user.id, "user.id")
                st.voice_pending[guild_id] = pending
                self._maybe_emit_credentials(acct_id, guild_id)
            return original_voice_server(data)

        state.parse_voice_server_update = parse_voice_server_update

        st.client_task = asyncio.create_task(client.start(token))
        liveness_failures = 0
        liveness_interval = 15.0
        liveness_threshold = 3
        heartbeat_ack_max_age = 90.0
        max_latency = 30.0
        try:
            ready_deadline = time.time() + 30.0
            while not self.conductor.poller_stop.is_set():
                if client.is_ready():
                    break
                if st.client_task.done():
                    exc = st.client_task.exception() if not st.client_task.cancelled() else None
                    raise RuntimeError(f"discord client exited before ready: {exc}")
                if time.time() >= ready_deadline:
                    raise RuntimeError("discord gateway ready timeout")
                await asyncio.sleep(0.25)
            while not self.conductor.poller_stop.is_set():
                if st.client_task is not None and st.client_task.done():
                    exc = st.client_task.exception() if not st.client_task.cancelled() else None
                    raise RuntimeError(f"discord session dead after ready: {exc}")
                healthy, reason = self._discord_gateway_health(
                    client,
                    heartbeat_ack_max_age=heartbeat_ack_max_age,
                    max_latency=max_latency,
                )
                if healthy:
                    liveness_failures = 0
                else:
                    liveness_failures += 1
                    logger.warning(
                        "discord gateway unhealthy acct_id=%s reason=%s failures=%s/%s",
                        acct_id,
                        reason,
                        liveness_failures,
                        liveness_threshold,
                    )
                    if liveness_failures >= liveness_threshold:
                        raise RuntimeError(f"discord gateway health failed: {reason}")
                await asyncio.sleep(liveness_interval)
        finally:
            try:
                await client.close()
            except Exception as e:
                logger.error("unexpected where=discord_client_close acct_id=%s error=%s", acct_id, e, exc_info=e)
            st.client = None
            if st.client_task is not None:
                st.client_task.cancel()
                st.client_task = None

    @staticmethod
    def _discord_gateway_health(client, *, heartbeat_ack_max_age: float, max_latency: float) -> tuple[bool, str]:
        if not client.is_ready():
            return False, "not_ready"
        if client.is_closed():
            return False, "client_closed"
        ws = client.ws
        if ws is None:
            return False, "socket_closed"
        if not ws.open:
            return False, "socket_closed"
        keep_alive = ws._keep_alive
        last_ack = None
        if keep_alive is not None:
            last_ack = keep_alive._last_ack
        if last_ack is not None:
            ack_age = time.perf_counter() - float(last_ack)
            if ack_age > heartbeat_ack_max_age:
                return False, "ack_stale"
        latency = client.latency
        if latency is not None:
            if latency < 0 or latency > max_latency:
                return False, "latency_exceeded"
        return True, "healthy"

    def _note_gateway_channel(self, st, data: dict) -> None:
        if not isinstance(data, dict):
            return
        channel_id = ext_id(data.get("id"), "id").strip()
        if not channel_id:
            return
        prev = st.gateway_channels.get(channel_id) or {}
        parent_id = ext_id(data.get("parent_id"), "parent_id").strip() or prev.get("parent_id") or ""
        guild_id = ext_id(data.get("guild_id"), "guild_id").strip() or prev.get("guild_id") or ""
        if data.get("type") is None:
            channel_type = prev.get("type") or 0
        else:
            channel_type = ext_int(data.get("type"), "type")
        st.gateway_channels[channel_id] = {
            "parent_id": parent_id,
            "type": channel_type,
            "guild_id": guild_id,
        }

    def _note_gateway_threads(self, st, threads) -> None:
        if threads is None:
            return
        for th in ext_list("discord threads", threads):
            self._note_gateway_channel(st, ext_dict("discord thread", th))

    def _channel_open_id(self, channel_id: str, meta: dict) -> str:
        _, open_id, _ = resolve_chat_and_thread(
            channel_id,
            ext_str((meta or {}).get("parent_id"), "parent_id").strip(),
            ext_int((meta or {}).get("type"), "type"),
        )
        return open_id or channel_id

    def _thread_lookup_meta(self, st, directory, meta: dict) -> dict:
        meta = meta or {}
        if not is_thread_channel_type(ext_int(meta.get("type"), "type")):
            return meta
        parent_id = ext_str(meta.get("parent_id"), "parent_id").strip()
        if not parent_id:
            return meta
        parent = {}
        if directory:
            parent = directory.get(parent_id) or {}
        if not parent:
            parent = st.gateway_channels.get(parent_id) or {}
        open_id = self._channel_open_id(parent_id, parent)
        if not open_id:
            return meta
        out = dict(meta)
        out["open_id"] = open_id
        return out

    def _dsb_gateway_live(self, st) -> bool:
        ws = st.gateway_ws
        return ws is not None and not ws.closed and st.gateway_ready.is_set()

    async def _deliver_dsb_direct_message(self, st, *, acct_id: str, token: str, data: dict) -> None:
        if not isinstance(data, dict):
            return
        if ext_id(data.get("guild_id"), "guild_id").strip():
            return
        channel_id = ext_id(data.get("channel_id"), "channel_id").strip()
        if not channel_id:
            return
        info = await self.get_channel_info(token=token, channel_id=channel_id)
        channel_type = 0
        parent_id = ""
        if isinstance(info, dict):
            channel_type = ext_int(info.get("type"), "type")
            parent_id = ext_id(info.get("parent_id"), "parent_id").strip()
        if channel_type not in _DM_CHANNEL_TYPES:
            return
        result = await self._accept_dsb_message(
            acct_id=acct_id,
            st=st,
            channel_id=channel_id,
            message=data,
            channel_meta={"parent_id": parent_id, "type": channel_type},
        )
        if result == "advance":
            await self._save_runtime_state(acct_id, mode="dsb")

    async def _deliver_dsb_guild_thread_message(self, st, *, acct_id: str, token: str, data: dict) -> None:
        if not isinstance(data, dict):
            return
        channel_id = ext_id(data.get("channel_id"), "channel_id").strip()
        if not channel_id:
            return
        known = st.gateway_channels.get(channel_id)
        if not known:
            info = await self.get_channel_info(token=token, channel_id=channel_id)
            if not isinstance(info, dict):
                return
            self._note_gateway_channel(st, info)
            known = st.gateway_channels.get(channel_id) or {}
        channel_type = ext_int(known.get("type"), "type")
        if not is_thread_channel_type(channel_type):
            return
        parent_id = ext_str(known.get("parent_id"), "parent_id").strip()
        if parent_id and parent_id not in st.gateway_channels:
            parent_info = await self.get_channel_info(token=token, channel_id=parent_id)
            if isinstance(parent_info, dict):
                self._note_gateway_channel(st, parent_info)
        msg_id = ext_id(data.get("id"), "id").strip()
        if msg_id.isdigit():
            async with self._dsb_msg_lock:
                cursors = self._get_cursors(acct_id)
                if not (cursors.get(channel_id) or "").strip():
                    cursors[channel_id] = str(int(msg_id) - 1)
        result = await self._accept_dsb_message(
            acct_id=acct_id,
            st=st,
            channel_id=channel_id,
            message=data,
            channel_meta=self._thread_lookup_meta(
                st, None, {"parent_id": parent_id, "type": channel_type},
            ),
        )
        if result == "advance":
            await self._save_runtime_state(acct_id, mode="dsb")

    async def _accept_dsb_message(self, *, acct_id: str, st, channel_id: str, message: dict, channel_meta: dict) -> str:
        message = ext_dict("discord message", message)
        channel_id = (channel_id or "").strip()
        msg_id = ext_id(message.get("id"), "id").strip()
        if not channel_id or not msg_id:
            return "skip"
        if not ext_id(message.get("channel_id"), "channel_id").strip():
            message = dict(message)
            message["channel_id"] = channel_id
        key = (acct_id, channel_id, msg_id)
        async with self._dsb_msg_lock:
            cursors = self._get_cursors(acct_id)
            if key in self._dsb_inflight:
                return "skip"
            if not discord_snowflake_greater(msg_id, cursors.get(channel_id, "")):
                return "skip"
            self._dsb_inflight.add(key)
            try:
                result = await self._consume_dsb_message(
                    acct_id=acct_id,
                    st=st,
                    channel_id=channel_id,
                    message=message,
                    channel_meta=channel_meta,
                )
                if result == "advance":
                    cursors[channel_id] = msg_id
                return result
            finally:
                self._dsb_inflight.discard(key)

    async def _consume_dsb_message(self, *, acct_id: str, st, channel_id: str, message: dict, channel_meta: dict) -> str:
        author = message.get("author")
        author = ext_dict("discord message author", author)
        author_id = ext_id(author.get("id"), "id").strip() or None
        self_id = ext_id(st.gateway_user_id, "gateway_user_id").strip()
        if dsb_inbound_ignored(message, self_id=self_id):
            return "advance"
        content = ext_str(message.get("content"), "content").strip()
        sticker_raw = message.get("sticker_items")
        if sticker_raw is None:
            sticker_raw = message.get("stickers")
        if sticker_raw is None:
            sticker_list = []
        else:
            sticker_list = ext_list("discord stickers", sticker_raw)
        atts_raw = message.get("attachments")
        if atts_raw is None:
            atts_raw = []
        else:
            atts_raw = ext_list("discord attachments", atts_raw)
        attachments, size_note = self._dsb_attachments(atts_raw, stickers=sticker_list)
        if size_note:
            content = f"{content}\n{size_note}".strip() if content else size_note
        meta = channel_meta or {}
        chat_id, open_id, chat_type = resolve_chat_and_thread(
            channel_id,
            meta.get("parent_id") or "",
            meta.get("type") or 0,
        )
        lookup_id = ext_str(meta.get("open_id"), "open_id").strip()
        if lookup_id:
            open_id = lookup_id
        force = content.startswith("/start")
        model_id = None if force else (await self.conductor.db.lookup_model_for_chat(acct_id=acct_id, chat_id=open_id))
        if not model_id:
            mapped = await self.conductor.ensure_chat_mapping(
                acct_id=acct_id,
                chat_id=open_id,
                chat_type="discord",
                carrier_user_id=author_id,
                force=force,
            )
            model_id = mapped.model_id
        if not model_id:
            return "advance" if mapped.settled else "stop"
        ref = message.get("message_reference")
        reply_to = ""
        reply_snippet = ""
        if ref is not None:
            ref = ext_dict("discord message_reference", ref)
            reply_to = ext_id(ref.get("message_id"), "message_id").strip()
        referenced = message.get("referenced_message")
        if referenced is not None:
            referenced = ext_dict("discord referenced_message", referenced)
            reply_snippet = ext_str(referenced.get("content"), "content").strip()
            if not reply_to:
                reply_to = ext_id(referenced.get("id"), "id").strip()
            if not reply_snippet:
                ref_atts = referenced.get("attachments")
                ref_stickers = referenced.get("sticker_items")
                if ref_stickers is None:
                    ref_stickers = referenced.get("stickers")
                if ref_atts is not None:
                    ref_atts = ext_list("discord referenced attachments", ref_atts)
                if ref_stickers is not None:
                    ref_stickers = ext_list("discord referenced stickers", ref_stickers)
                if (isinstance(ref_atts, list) and ref_atts) or (isinstance(ref_stickers, list) and ref_stickers):
                    reply_snippet = "(attachment)"
        reply_to_text = guidance.telegram_reply_snippet(reply_snippet)
        content = guidance.inject_reply_context(content, reply_to_text)
        if not content and attachments:
            for a in attachments:
                if a.get("type") == "sticker":
                    name = ext_str(a.get("file_name"), "file_name").strip()
                    if a.get("is_animated") or not ext_str(a.get("url"), "url").strip():
                        content = guidance.animated_sticker_injection(name)
                    else:
                        content = guidance.sticker_injection(name or "a sticker", "", "")
                    break
        msg_id = ext_id(message.get("id"), "id").strip()
        body = {
            "method": "send_message",
            "params": {"text": content},
            "text": content,
            "caption": "",
            "attachments": attachments,
            "msg_id": msg_id,
            "platform": "discord",
            "user_id": author_id or "",
        }
        sender_name, sender_username = _discord_author_sender(author)
        apply_sender(body, name=sender_name, username=sender_username)
        if reply_to:
            body["reply_to"] = reply_to
        if reply_to_text:
            body["reply_to_text"] = reply_to_text
        if chat_type:
            body["chat_type"] = chat_type
        ref_author = ""
        referenced = message.get("referenced_message")
        if referenced is not None:
            referenced = ext_dict("discord referenced_message", referenced)
            ref_user = referenced.get("author")
            if ref_user is not None:
                ref_user = ext_dict("discord referenced author", ref_user)
                ref_author = ext_id(ref_user.get("id"), "id").strip()
        body["mentioned"] = discord_mentions_bot(
            bot_id=self_id,
            content=ext_str(message.get("content"), "content"),
            mention_ids=_discord_mention_ids_from_users(message.get("mentions")),
            referenced_author_id=ref_author,
        )
        if not content and not attachments:
            body["raw"] = message
        ok = await self.conductor.deliver_inbound(
            model_id=model_id,
            acct_id=acct_id,
            chat_id=chat_id,
            request_id=msg_id,
            body=body,
        )
        if not ok:
            return "stop"
        return "advance"

    async def _dsb_poller_loop(self, *, acct_id: str, token: str) -> None:
        # Discord allows 10 guild channel-list calls per 60 seconds. Stay under that,
        # keep the last good list when a call fails, and poll channels learned from
        # gateway create events without waiting for the next list.
        directory_refresh_seconds = 8.0
        message_poll_seconds = 2.0
        refresh_at = 0.0
        guild_channels: dict[str, list[str]] = {}
        dm_ids: list[str] = []
        channel_meta: dict[str, dict] = {}
        next_message_at: dict[str, float] = {}
        await self._load_runtime_state(acct_id)
        st = self._acct_state(acct_id)
        if not st.gateway_user_id:
            status, me = await self._dsb_request(method="GET", token=token, path="/api/v9/users/@me")
            if status == 200 and isinstance(me, dict):
                st.gateway_user_id = ext_id(me.get("id"), "id").strip()

        def listed_channel_ids() -> list[str]:
            out: list[str] = []
            seen: set[str] = set()
            for ids in guild_channels.values():
                for cid in ids:
                    if cid not in seen:
                        out.append(cid)
                        seen.add(cid)
            for cid in dm_ids:
                if cid not in seen:
                    out.append(cid)
                    seen.add(cid)
            for cid, meta in st.gateway_channels.items():
                if cid not in channel_meta:
                    channel_meta[cid] = {
                        "parent_id": ext_str(meta.get("parent_id"), "parent_id").strip(),
                        "type": ext_int(meta.get("type"), "type"),
                    }
                if cid not in seen:
                    out.append(cid)
                    seen.add(cid)
            return out

        while not self.conductor.poller_stop.is_set():
            try:
                now = time.time()
                state_dirty = False
                cursors = self._get_cursors(acct_id)
                if now >= refresh_at:
                    refresh_at = now + directory_refresh_seconds
                    status, guilds = await self._dsb_request(method="GET", token=token, path="/api/v9/users/@me/guilds")
                    if status == 200 and isinstance(guilds, list):
                        seen_gids: set[str] = set()
                        for g in guilds:
                            g = ext_dict('discord guild', g)
                            gid = ext_id(g.get("id"), "id").strip()
                            if not gid:
                                continue
                            seen_gids.add(gid)
                            cStatus, channels = await self._dsb_request(method="GET", token=token, path=f"/api/v9/guilds/{gid}/channels")
                            if cStatus != 200 or not isinstance(channels, list):
                                continue
                            ids: list[str] = []
                            for ch in channels:
                                ch = ext_dict('discord channel', ch)
                                cid = ext_id(ch.get("id"), "id").strip()
                                if not cid:
                                    continue
                                ids.append(cid)
                                channel_meta[cid] = {
                                    "parent_id": ext_id(ch.get("parent_id"), "parent_id").strip(),
                                    "type": ext_int(ch.get("type"), "type"),
                                }
                            t_status, active = await self._dsb_request(
                                method="GET",
                                token=token,
                                path=f"/api/v9/guilds/{gid}/threads/active",
                            )
                            seen_ids = set(ids)
                            if t_status == 200 and isinstance(active, dict):
                                raw_threads = active.get("threads")
                                if raw_threads is None:
                                    raw_threads = []
                                else:
                                    raw_threads = ext_list("discord active threads", raw_threads)
                                for th in raw_threads:
                                    th = ext_dict("discord thread", th)
                                    cid = ext_id(th.get("id"), "id").strip()
                                    if not cid or cid in seen_ids:
                                        continue
                                    ids.append(cid)
                                    seen_ids.add(cid)
                                    channel_meta[cid] = {
                                        "parent_id": ext_id(th.get("parent_id"), "parent_id").strip(),
                                        "type": ext_int(th.get("type"), "type"),
                                    }
                            else:
                                for cid in guild_channels.get(gid) or []:
                                    meta = channel_meta.get(cid) or {}
                                    if cid in seen_ids or not is_thread_channel_type(meta.get("type") or 0):
                                        continue
                                    ids.append(cid)
                                    seen_ids.add(cid)
                            old = set(guild_channels.get(gid) or [])
                            guild_channels[gid] = ids
                            for gone in old - set(ids):
                                if gone not in dm_ids:
                                    channel_meta.pop(gone, None)
                        for gone_gid in set(guild_channels) - seen_gids:
                            for cid in guild_channels.pop(gone_gid):
                                if cid not in dm_ids:
                                    channel_meta.pop(cid, None)
                    dm_status, dm_channels = await self._dsb_request(
                        method="GET", token=token, path="/api/v9/users/@me/channels",
                    )
                    if dm_status == 200 and isinstance(dm_channels, list):
                        next_dms: list[str] = []
                        for ch in dm_channels:
                            ch = ext_dict("discord dm channel", ch)
                            cid = ext_id(ch.get("id"), "id").strip()
                            if not cid:
                                continue
                            next_dms.append(cid)
                            channel_meta[cid] = {
                                "parent_id": "",
                                "type": ext_int(ch.get("type"), "type"),
                            }
                        still_in_guild = {cid for ids in guild_channels.values() for cid in ids}
                        for gone in set(dm_ids) - set(next_dms):
                            if gone not in still_in_guild:
                                channel_meta.pop(gone, None)
                        dm_ids = next_dms

                for channel_id in listed_channel_ids():
                    meta = channel_meta.get(channel_id) or {}
                    if ext_int(meta.get("type"), "type") in _DM_CHANNEL_TYPES and self._dsb_gateway_live(st):
                        continue
                    if now < next_message_at.get(channel_id, 0):
                        continue
                    next_message_at[channel_id] = now + message_poll_seconds
                    cursor = ext_str(cursors.get(channel_id), "cursor").strip()
                    msgs: list[Any] = []
                    if not cursor:
                        status, seed_msgs = await self._dsb_request(
                            method="GET",
                            token=token,
                            path=f"/api/v9/channels/{channel_id}/messages",
                            params={"limit": "1"},
                        )
                        async with self._dsb_msg_lock:
                            cursor = (cursors.get(channel_id) or "").strip()
                            if not cursor:
                                if status != 200 or not isinstance(seed_msgs, list):
                                    cursor = ""
                                elif not seed_msgs:
                                    cursors[channel_id] = "0"
                                    state_dirty = True
                                    cursor = "0"
                                else:
                                    seed = seed_msgs[0]
                                    seed = ext_dict('discord seed message', seed)
                                    seed_id = ext_id(seed.get("id"), "id").strip()
                                    author = seed.get("author") if seed_id else None
                                    is_bot = isinstance(author, dict) and bool(author.get("bot"))
                                    content = ext_str(seed.get("content"), "content").strip() if seed_id else ""
                                    if seed_id and (not is_bot) and content.startswith("/start"):
                                        msgs = [seed]
                                    elif seed_id and is_thread_channel_type(ext_int(meta.get("type"), "type")) and channel_id not in st.backlog_threads and discord_snowflake_recent(channel_id, now=now, window=180):
                                        cursors[channel_id] = "0"
                                        cursor = "0"
                                    elif seed_id:
                                        cursors[channel_id] = seed_id
                                        state_dirty = True
                                        cursor = "skip"
                                    else:
                                        cursor = ""
                        if cursor == "skip":
                            continue
                    if cursor and not msgs:
                        status, fetched = await self._dsb_request(
                            method="GET",
                            token=token,
                            path=f"/api/v9/channels/{channel_id}/messages",
                            params={"after": cursor, "limit": "100"},
                        )
                        if status != 200 or not isinstance(fetched, list):
                            continue
                        msgs = list(reversed(fetched))
                    for m in msgs:
                        meta = self._thread_lookup_meta(st, channel_meta, channel_meta.get(channel_id) or {})
                        result = await self._accept_dsb_message(
                            acct_id=acct_id,
                            st=st,
                            channel_id=channel_id,
                            message=m,
                            channel_meta=meta,
                        )
                        if result == "stop":
                            break
                        if result == "advance":
                            state_dirty = True

                if state_dirty:
                    await self._save_runtime_state(acct_id, mode="dsb")

                await asyncio.sleep(0.25)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("unexpected where=discord_dsb_poller acct_id=%s error=%s", acct_id, e, exc_info=e)
                await asyncio.sleep(2)
                continue
