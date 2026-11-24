import secrets
from datetime import timedelta

from station import instant

OTP_TTL = timedelta(minutes=6)
OTP_MAX_ATTEMPTS = 5


def fresh_otp() -> str:
    return "%06d" % secrets.randbelow(1000000)


def expiry_text() -> str:
    return instant.format(instant.now() + OTP_TTL)


def reusable(row: dict | None) -> str | None:
    if not row:
        return None
    otp = row.get("otp") or ""
    if not otp:
        return None
    try:
        exp = instant.parse(row.get("expires_at") or "")
        attempts = int(row.get("attempts") or 0)
    except (TypeError, ValueError):
        return None
    if instant.now() < exp and attempts < OTP_MAX_ATTEMPTS:
        return otp
    return None


def apply_verify(row: dict | None, token: str) -> tuple[bool, dict | None]:
    """Return whether the token matched, and the row to keep.

    A None replacement means the stored row should be deleted. A missing row
    stays missing.
    """
    if not row:
        return False, None
    try:
        exp = instant.parse(row.get("expires_at") or "")
        attempts = int(row.get("attempts") or 0)
    except (TypeError, ValueError):
        return False, None
    if not instant.now() < exp or attempts >= OTP_MAX_ATTEMPTS:
        return False, None
    if (row.get("otp") or "") == token:
        return True, None
    attempts += 1
    if attempts >= OTP_MAX_ATTEMPTS:
        return False, None
    updated = dict(row)
    updated["attempts"] = attempts
    return False, updated


def classify_start_arg(model_id: str, arg: str, chat_type: str) -> str:
    """format, mismatch_group, mismatch_private, or check.

    A group can contain several bots, so a token for another model is left
    alone. A direct message has only this bot, so that mismatch is a wrong token.
    """
    if len(arg) <= 32:
        return "format"
    expected = (model_id or "").replace("-", "").lower()
    if arg[:32].lower() != expected:
        if (chat_type or "").strip().lower() == "group":
            return "mismatch_group"
        return "mismatch_private"
    return "check"


def start_otp_token(arg: str) -> str:
    return arg[32:]
