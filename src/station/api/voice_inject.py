import base64

from aiohttp import web

from station.api.http import read_json_object
from station.api.token import _require_admin
from station.errors import ExternalError
from station.prototypes.boundary import ext_int, ext_str


def _pcm_from_body(body):
    raw_b64 = ext_str("pcm_base64", body.get("pcm_base64"), default="")
    if not raw_b64:
        raise ExternalError("pcm_base64 is required")
    try:
        pcm = base64.b64decode(raw_b64, validate=True)
    except (ValueError, TypeError) as e:
        raise ExternalError("Invalid pcm_base64") from e
    if not pcm:
        raise ExternalError("pcm_base64 is empty")
    if len(pcm) % 2 != 0:
        raise ExternalError("pcm_base64 must be s16le PCM")
    return pcm


def _silence_pcm(*, silence_ms):
    silence_ms = max(0, int(silence_ms or 0))
    if silence_ms <= 0:
        return b""
    samples = int(48000 * silence_ms / 1000)
    return b"\x00\x00" * samples


async def handle_admin_inject_voice_pcm(request):
    denied = _require_admin(request)
    if denied is not None:
        return denied

    body = await read_json_object(request)
    kind = ext_str("kind", body.get("kind"), default="").strip().lower()
    pcm = _pcm_from_body(body)
    silence_ms = 0
    if "silence_ms" in body and body.get("silence_ms") is not None:
        silence_ms = ext_int("silence_ms", body.get("silence_ms"))
    if silence_ms < 0:
        raise ExternalError("silence_ms must be >= 0")
    payload = pcm + _silence_pcm(silence_ms=silence_ms)

    app = request.app
    ok = False
    if kind == "webrtc":
        call_id = ext_str("call_id", body.get("call_id"), default="").strip()
        if not call_id:
            raise ExternalError("call_id is required for webrtc")
        svc = app.get("webrtc")
        if svc is None:
            raise ExternalError("webrtc service unavailable", status=503)
        ok = await svc.inject_audio_pcm(call_id=call_id, pcm_s16le_mono_48k=payload)
    elif kind == "discord":
        acct_id = ext_str("acct_id", body.get("acct_id"), default="").strip()
        guild_id = ext_str("guild_id", body.get("guild_id"), default="").strip()
        channel_id = ext_str("channel_id", body.get("channel_id"), default="").strip()
        if not acct_id or not guild_id:
            raise ExternalError("acct_id and guild_id are required for discord")
        svc = app.get("discord_voice")
        if svc is None:
            raise ExternalError("discord_voice service unavailable", status=503)
        ok = await svc.inject_audio_pcm(
            acct_id=acct_id,
            guild_id=guild_id,
            channel_id=channel_id or None,
            pcm_s16le_mono_48k=payload,
        )
    else:
        raise ExternalError("kind must be webrtc or discord")

    if not ok:
        raise ExternalError("No active voice session for inject", status=404)
    return web.json_response(
        {
            "result": 0,
            "msg": "ok",
            "data": {
                "kind": kind,
                "bytes": len(payload),
                "silence_ms": silence_ms,
            },
        },
        status=200,
    )
