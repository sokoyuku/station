from station.prototypes.boundary import ext_require
from station.prototypes.chat_history import attachment_history_line


def inbound_history_text(text, *, attachments=None, photo=False, audio=False):
    parts = []
    body = (text or "").strip()
    if body:
        parts.append(body)
    kinds = set()
    for record in attachments or []:
        parts.append(attachment_history_line(record))
        kinds.add(record.get("type") or "")
    if photo and "photo" not in kinds and "image" not in kinds:
        parts.append("[photo]")
    if audio and "audio" not in kinds and "voice" not in kinds:
        parts.append("[audio]")
    return "\n".join(parts).strip()


def inbound_mentions_model(data):
    raw = data.get("mentioned")
    if raw is None:
        return True
    return ext_require("mentioned", raw, (bool,))


def group_starts_turn(ctx):
    if ctx.chat_type != "group":
        return True
    return inbound_mentions_model(ctx.data)
