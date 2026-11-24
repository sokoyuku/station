import re
import sys

_EDGE_CODE = re.compile(r"error code:\s*\d+", re.IGNORECASE)
_SLUG = re.compile(r"[^a-z0-9]+")
_COMMAND_FAILED = re.compile(r"^Command failed with status (\d+):", re.IGNORECASE)
PUBLIC_UNAVAILABLE = "The service is temporarily unavailable."


def looks_like_edge_body(text: str) -> bool:
    sample = (text or "").strip().lower()
    if not sample:
        return False
    if len(sample) > 2000:
        sample = sample[:2000]
    if "<!doctype" in sample or "<html" in sample:
        return True
    if "cf-error" in sample or "cf-wrapper" in sample or "cloudflare ray id" in sample:
        return True
    return _EDGE_CODE.search(sample) is not None


def public_chat_error(text: str) -> str:
    cleaned = " ".join((text or "").split())
    if not cleaned:
        return cleaned
    if looks_like_edge_body(cleaned):
        return PUBLIC_UNAVAILABLE
    match = _COMMAND_FAILED.match(cleaned)
    if match is not None and int(match.group(1)) >= 500:
        return PUBLIC_UNAVAILABLE
    return cleaned


def slug(value) -> str:
    text = _SLUG.sub("_", str(value or "").strip().lower()).strip("_")
    if not text:
        return "error"
    if len(text) > 80:
        return text[:80]
    return text


def auto_code(message, *, depth=1) -> str:
    frame = sys._getframe(depth)
    module = frame.f_globals.get("__name__", "")
    func = slug(frame.f_code.co_name)
    line = frame.f_lineno
    prefix = "conductor.local" if "station.conductor" in module else "station"
    return "%s.%s.L%s.%s" % (prefix, func, line, slug(message))


class ExternalError(Exception):
    def __init__(self, message, *, status=400, reason="", retryable=None, code="", cause_code=""):
        super().__init__(message)
        self.status = status
        self.reason = reason
        self.retryable = retryable
        self.code = str(code or "").strip() or auto_code(message, depth=2)
        self.cause_code = str(cause_code or "")


class InternalError(Exception):
    pass
