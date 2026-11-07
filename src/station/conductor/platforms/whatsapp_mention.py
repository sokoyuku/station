import re

from station.conductor.util import ext_str

_AT_ALL_RE = re.compile(r"(?i)(?:^|\s)@all\b")


def normalize_whatsapp_id(value):
    text = ext_str(value, "whatsapp_id").strip()
    if ":" in text and "@" in text:
        text = text.replace(":", "@", 1)
    return text


def whatsapp_id_keys(value):
    normalized = normalize_whatsapp_id(value)
    if not normalized:
        return set()
    local = normalized.split("@", 1)[0].split(":", 1)[0]
    keys = {normalized, normalized.lower()}
    if local:
        keys.add(local)
        keys.add(local.lower())
    return {key for key in keys if key}


def whatsapp_ids_overlap(left, right):
    return bool(whatsapp_id_keys(left) & whatsapp_id_keys(right))


def is_whatsapp_broadcast_chat(chat_id):
    cid = ext_str(chat_id, "chat_id").strip().lower()
    if not cid:
        return False
    return cid == "status@broadcast" or cid.endswith("@broadcast") or cid.endswith("@newsletter")


def collect_whatsapp_mentioned_ids(*sources):
    ids = []
    for source in sources:
        if source is None:
            continue
        if isinstance(source, list):
            values = source
        elif isinstance(source, dict):
            values = []
            for key in ("mentioned_ids", "mentionedJid", "mentioned"):
                raw = source.get(key)
                if isinstance(raw, list):
                    values.extend(raw)
        else:
            continue
        for item in values:
            if isinstance(item, dict):
                item = item.get("wa_id") or item.get("id") or ""
            text = normalize_whatsapp_id(item)
            if text:
                ids.append(text)
    return ids


def whatsapp_mentions_bot(
    *,
    text="",
    caption="",
    mentioned_ids=None,
    quoted_participant="",
    bot_ids=None,
    broadcast=False,
):
    if broadcast:
        return False
    body = "%s\n%s" % (ext_str(text, "text"), ext_str(caption, "caption"))
    if _AT_ALL_RE.search(body):
        return True
    bots = [normalize_whatsapp_id(item) for item in (bot_ids or []) if normalize_whatsapp_id(item)]
    if not bots:
        return False
    for mentioned in mentioned_ids or []:
        for bot_id in bots:
            if whatsapp_ids_overlap(mentioned, bot_id):
                return True
    quoted = normalize_whatsapp_id(quoted_participant)
    if quoted:
        for bot_id in bots:
            if whatsapp_ids_overlap(quoted, bot_id):
                return True
    lower = body.lower()
    for bot_id in bots:
        bare = bot_id.split("@", 1)[0].split(":", 1)[0].lower()
        if bare and ("@" + bare) in lower:
            return True
    return False
