import json
import os

import aiohttp

from station.conductor.platform_types import is_qr_account_type, strip_qr_prefix
from station.errors import ExternalError

OFFICIAL_TELEGRAM_BASE = "https://api.telegram.org"
OFFICIAL_DISCORD_BASE = "https://discord.com/api/v9"
OFFICIAL_QQ_API = "https://api.sgroup.qq.com"
OFFICIAL_QQ_TOKEN = "https://bots.qq.com/app/getAppAccessToken"
OFFICIAL_WHATSAPP_BASE = "https://graph.facebook.com"
WHATSAPP_API_VERSION = "v21.0"


class AccountUnreachable(ExternalError):
    def __init__(self):
        super().__init__("Could not reach the platform", status=503, reason="unreachable")


def _session_timeout():
    return aiohttp.ClientTimeout(total=20)


def parse_qq_credentials(token):
    raw = (token or "").strip()
    if raw.startswith("lctest1-"):
        raw = raw[len("lctest1-"):]
    if not raw:
        return "", ""
    if raw.startswith("{"):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ExternalError("invalid qq credentials JSON") from exc
        app_id = str(data.get("app_id") or data.get("appId") or "").strip()
        secret = str(data.get("client_secret") or data.get("clientSecret") or "").strip()
        return app_id, secret
    if ":" in raw:
        app_id, secret = raw.split(":", 1)
        return app_id.strip(), secret.strip()
    return "", ""


def whatsapp_creds_path(conductor, acct_id):
    base = (os.environ.get("LOCAL_CONDUCTOR_WHATSAPP_SESSION_DIR") or "").strip()
    if not base:
        db_path = conductor.config.database.path or "."
        parent = db_path if os.path.isdir(db_path) else os.path.dirname(os.path.abspath(db_path)) or "."
        base = os.path.join(parent, "whatsapp_sessions")
    return os.path.join(base, "whatsapp", (acct_id or "").replace("-", "").strip(), "creds.json")


def _cfg_str(conductor, name):
    config = getattr(conductor, "config", None)
    if config is None:
        return ""
    return str(getattr(config, name, "") or "").strip().rstrip("/")


def _env_url(*names):
    for name in names:
        raw = str(os.environ.get(name) or "").strip().rstrip("/")
        if raw:
            return raw
    return ""


def _telegram_api_base(conductor):
    return (
        _cfg_str(conductor, "telegram_api_url")
        or _env_url("TELEGRAM_API_URL", "TGB_API_URL", "TELEGRAM_API_BASE")
        or OFFICIAL_TELEGRAM_BASE
    )


def _discord_api_base(conductor):
    return _cfg_str(conductor, "discord_api_url") or _env_url("DISCORD_API_URL", "DSB_API_URL") or OFFICIAL_DISCORD_BASE


def _qq_api_base(conductor):
    return _cfg_str(conductor, "qq_api_url") or _env_url("QQ_API_URL") or OFFICIAL_QQ_API


def _qq_token_url(conductor):
    return _cfg_str(conductor, "qq_token_url") or _env_url("QQ_TOKEN_URL") or OFFICIAL_QQ_TOKEN


def _whatsapp_api_base(conductor):
    return _cfg_str(conductor, "whatsapp_cloud_api_url") or _env_url("WHATSAPP_CLOUD_API_URL") or OFFICIAL_WHATSAPP_BASE


def _classify_http(status):
    if status == 429 or status >= 500:
        raise AccountUnreachable()
    return "outdated"


async def _read_json(resp):
    try:
        body = await resp.json(content_type=None)
    except (aiohttp.ContentTypeError, json.JSONDecodeError, ValueError):
        body = None
    return body if isinstance(body, dict) else {}


async def _check_telegram(session, *, token, username, api_base):
    expected = (username or "").strip().lstrip("@")
    secret = (token or "").strip()
    if not secret or not expected:
        return "outdated"
    root = (api_base or "").strip().rstrip("/") or OFFICIAL_TELEGRAM_BASE
    url = "%s/bot%s/getMe" % (root, secret)
    try:
        async with session.get(url) as resp:
            status = int(resp.status)
            body = await _read_json(resp)
    except (aiohttp.ClientError, TimeoutError) as exc:
        raise AccountUnreachable() from exc
    if status != 200:
        return _classify_http(status)
    if not body.get("ok"):
        return "outdated"
    me = body.get("result") if isinstance(body.get("result"), dict) else {}
    got_name = str(me.get("username") or "").strip()
    got_id = str(me.get("id") or "").strip()
    if got_name and got_name.casefold() == expected.casefold():
        return "active"
    if got_id and got_id == expected:
        return "active"
    return "outdated"


async def _check_discord(session, *, token, username, api_base):
    expected = (username or "").strip()
    secret = (token or "").strip()
    if not secret or not expected:
        return "outdated"
    auth = secret if secret.lower().startswith("bot ") else "Bot %s" % secret
    root = (api_base or "").strip().rstrip("/") or OFFICIAL_DISCORD_BASE
    if root == OFFICIAL_DISCORD_BASE or root.endswith("/api/v9"):
        url = "%s/users/@me" % root
    else:
        url = "%s/api/v9/users/@me" % root
    try:
        async with session.get(
            url,
            headers={"Authorization": auth},
        ) as resp:
            status = int(resp.status)
            body = await _read_json(resp)
    except (aiohttp.ClientError, TimeoutError) as exc:
        raise AccountUnreachable() from exc
    if status != 200:
        return _classify_http(status)
    got_id = str(body.get("id") or "").strip()
    if got_id and got_id == expected:
        return "active"
    return "outdated"


async def _check_qq(session, *, token, username, api_base, token_url):
    expected = (username or "").strip()
    app_id, client_secret = parse_qq_credentials(token)
    if not expected or not app_id or not client_secret or app_id != expected:
        return "outdated"
    token_endpoint = (token_url or "").strip() or OFFICIAL_QQ_TOKEN
    api_root = (api_base or "").strip().rstrip("/") or OFFICIAL_QQ_API
    try:
        async with session.post(
            token_endpoint,
            json={"appId": app_id, "clientSecret": client_secret},
        ) as resp:
            status = int(resp.status)
            body = await _read_json(resp)
        if status != 200:
            return _classify_http(status)
        access_token = str(body.get("access_token") or "").strip()
        if not access_token:
            return "outdated"
        async with session.get(
            "%s/gateway" % api_root,
            headers={"Authorization": "QQBot %s" % access_token},
        ) as resp:
            status = int(resp.status)
            body = await _read_json(resp)
    except (aiohttp.ClientError, TimeoutError) as exc:
        raise AccountUnreachable() from exc
    if status != 200:
        return _classify_http(status)
    if str(body.get("url") or "").strip():
        return "active"
    return "outdated"


async def _check_whatsapp_cloud(session, *, token, username, server, api_base):
    access_token = (token or "").strip()
    phone_number_id = (username or "").strip()
    app_secret = (server or "").strip()
    if not access_token or not phone_number_id or not app_secret:
        return "outdated"
    root = (api_base or "").strip().rstrip("/") or OFFICIAL_WHATSAPP_BASE
    url = "%s/%s/%s" % (root, WHATSAPP_API_VERSION, phone_number_id)
    try:
        async with session.get(
            url,
            params={"fields": "id"},
            headers={"Authorization": "Bearer %s" % access_token},
        ) as resp:
            status = int(resp.status)
            body = await _read_json(resp)
    except (aiohttp.ClientError, TimeoutError) as exc:
        raise AccountUnreachable() from exc
    if status != 200:
        return _classify_http(status)
    got_id = str(body.get("id") or "").strip()
    if got_id and got_id == phone_number_id:
        return "active"
    return "outdated"


def _check_whatsapp_qr(conductor, acct_id):
    if os.path.isfile(whatsapp_creds_path(conductor, acct_id)):
        return "active"
    return "outdated"


async def check_local_account(conductor, *, acct_id, acct_type, username, token, server):
    raw_type = (acct_type or "").strip().lower()
    bare = strip_qr_prefix(raw_type)
    if is_qr_account_type(raw_type) or bare == "whatsapp":
        if bare != "whatsapp":
            raise ExternalError("Account cannot be checked")
        return _check_whatsapp_qr(conductor, acct_id)
    async with aiohttp.ClientSession(timeout=_session_timeout()) as session:
        if bare == "telegram":
            return await _check_telegram(
                session,
                token=token,
                username=username,
                api_base=_telegram_api_base(conductor),
            )
        if bare == "discord":
            return await _check_discord(
                session,
                token=token,
                username=username,
                api_base=_discord_api_base(conductor),
            )
        if bare == "qq":
            return await _check_qq(
                session,
                token=token,
                username=username,
                api_base=_qq_api_base(conductor),
                token_url=_qq_token_url(conductor),
            )
        if bare == "whatsapp_cloud":
            return await _check_whatsapp_cloud(
                session,
                token=token,
                username=username,
                server=server,
                api_base=_whatsapp_api_base(conductor),
            )
    raise ExternalError("Account cannot be checked")
