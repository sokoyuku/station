import re

_EDGE_CODE = re.compile(r"error code:\s*\d+", re.IGNORECASE)
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


class ExternalError(Exception):
    def __init__(self, message, *, status=400, reason="", retryable=None):
        super().__init__(message)
        self.status = status
        self.reason = reason
        self.retryable = retryable


class InternalError(Exception):
    pass
