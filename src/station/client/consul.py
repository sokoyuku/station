import asyncio
import json

import aiohttp

from station.client.retry import CircuitOpen, RetryableError, is_transient_status, retry_transient
from station.errors import ExternalError
from station.prototypes.boundary import ext_dict, ext_require
from station import logger

CONSUL_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10, sock_connect=10, sock_read=30)

_REMOTE = (CircuitOpen, aiohttp.ClientError, asyncio.TimeoutError, RetryableError, ExternalError)

def _consul_error(message, *, status=502, reason="consul_request", retryable=None):
    return ExternalError(message, status=status, reason=reason, retryable=retryable)

def _raise_remote(where, error):
    logger.error("%s failed error=%s", where, error)
    if isinstance(error, ExternalError):
        raise error
    status = 503 if isinstance(error, CircuitOpen) else 502
    raise _consul_error(
        "%s failed error=%s" % (where, error),
        status=status,
        reason="consul_request",
        retryable=True,
    ) from error

async def _request_json(*, session, method, url, headers, json_payload=None):
    kwargs = {"headers": headers, "timeout": CONSUL_TIMEOUT}
    if json_payload is not None:
        kwargs["json"] = json_payload
    async with session.request(method, url, **kwargs) as resp:
        if is_transient_status(resp.status):
            raise RetryableError("status=%s url=%s" % (resp.status, url))
        try:
            body = await resp.json(content_type=None)
        except (aiohttp.ContentTypeError, json.JSONDecodeError, ValueError) as e:
            raise _consul_error(
                "%s invalid json status=%s error=%s" % (url, resp.status, e),
                status=502,
                reason="consul_body",
                retryable=True,
            ) from e
        if resp.status != 200:
            msg = body.get("msg") if isinstance(body, dict) else None
            if resp.status == 404:
                raise _consul_error(
                    "%s status=404 msg=%s" % (url, msg),
                    status=404,
                    reason="not_found",
                    retryable=False,
                )
            if resp.status == 401:
                raise _consul_error(
                    "%s status=401 msg=%s" % (url, msg),
                    status=401,
                    reason="unauthorized",
                    retryable=False,
                )
            raise _consul_error(
                "%s status=%s msg=%s" % (url, resp.status, msg),
                status=502,
                reason="consul_http",
                retryable=resp.status >= 500,
            )
        return body

def _require_result_data(body, *, where):
    body = ext_dict("%s body" % where, body)
    result = ext_require("%s result" % where, body.get("result"), (int,))
    if result != 0:
        msg = body.get("msg")
        text = "%s result=%s msg=%s" % (where, result, msg)
        lowered = str(msg or "").lower()
        if "not found" in lowered:
            raise _consul_error(text, status=404, reason="not_found", retryable=False)
        if "unauthorized" in lowered:
            raise _consul_error(text, status=401, reason="unauthorized", retryable=False)
        raise _consul_error(text, status=502, reason="consul_result", retryable=False)
    data = body.get("data")
    if data is None:
        raise _consul_error("%s empty data" % where, status=502, reason="consul_empty", retryable=False)
    return ext_dict("%s data" % where, data)

async def fetch_model(*, session, consul_url, token, model_id):
    consul_url = consul_url.rstrip("/")
    url = "%s/api/station/get_model" % consul_url
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Prototype-Token"] = token
    payload = {"model_id": model_id}
    try:
        body = await retry_transient(
            _request_json,
            session=session,
            method="POST",
            url=url,
            headers=headers,
            json_payload=payload,
            circuit_name="consul",
        )
        return _require_result_data(body, where="fetch_model")
    except _REMOTE as e:
        _raise_remote("fetch_model model_id=%s" % model_id, e)

async def push_webrtc(*, session, consul_url, token, chat_id, webrtc_type, webrtc_content, session_id=""):
    consul_url = consul_url.rstrip("/")
    url = "%s/api/station/push_webrtc" % consul_url
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Prototype-Token"] = token
    payload = {
        "chat_id": chat_id,
        "session_id": session_id,
        "webrtc_type": webrtc_type,
        "content": webrtc_content if webrtc_content is not None else {},
    }
    try:
        body = await retry_transient(
            _request_json,
            session=session,
            method="POST",
            url=url,
            headers=headers,
            json_payload=payload,
            circuit_name="consul",
        )
    except _REMOTE as e:
        _raise_remote("push_webrtc", e)
    body = ext_dict("push_webrtc body", body)
    result = body.get("result")
    if result != 0:
        text = "push_webrtc result=%s msg=%s" % (result, body.get("msg"))
        logger.error("%s", text)
        raise _consul_error(text, status=502, reason="consul_result", retryable=False)
    logger.info("push_webrtc ok chat_id=%s webrtc_type=%s", chat_id, webrtc_type)
    return True

async def fetch_prototype(*, session, consul_url, token):
    consul_url = consul_url.rstrip("/")
    url = "%s/api/station/get_prototype" % consul_url
    headers = {}
    if token:
        headers["X-Prototype-Token"] = token
    try:
        body = await retry_transient(
            _request_json,
            session=session,
            method="GET",
            url=url,
            headers=headers,
            circuit_name="consul",
        )
        return _require_result_data(body, where="fetch_prototype")
    except _REMOTE as e:
        _raise_remote("fetch_prototype", e)
