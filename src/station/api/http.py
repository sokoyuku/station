import json

from aiohttp import web

from station.errors import ExternalError, InternalError
from station.prototypes.boundary import ext_dict, ext_str


async def read_json_object(request, name="body"):
    try:
        data = await request.json()
    except (json.JSONDecodeError, ValueError):
        raise ExternalError("Invalid JSON body")
    return ext_dict(name, data)


def ext_path_int(name, raw):
    text = ext_str(name, raw if raw is not None else "")
    if not text:
        raise ExternalError("Invalid %s" % name)
    try:
        return int(text)
    except ValueError:
        raise ExternalError("Invalid %s" % name)


def external_response(error):
    payload = {
        "result": 1,
        "msg": str(error),
        "data": None,
        "code": error.code,
    }
    if error.cause_code:
        payload["cause"] = error.cause_code
    if error.reason:
        payload["reason"] = error.reason
    if error.retryable is not None:
        payload["retryable"] = error.retryable
    return web.json_response(payload, status=error.status)


def log_caught(logger, error, *, where):
    code = getattr(error, "code", "") or ""
    if type(error) is ExternalError:
        logger.error("external where=%s code=%s error=%s", where, code, error)
        return
    if type(error) is InternalError:
        logger.error("internal where=%s code=%s error=%s", where, code, error, exc_info=error)
        return
    logger.error("unexpected where=%s code=%s error=%s", where, code, error, exc_info=error)
