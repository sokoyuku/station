from datetime import datetime, timezone

_LEN = 24


def now():
    current = datetime.now(timezone.utc)
    return current.replace(microsecond=(current.microsecond // 1000) * 1000)


def format(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        raise ValueError("instant requires a timezone")
    utc = dt.astimezone(timezone.utc)
    ms = utc.microsecond // 1000
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


def parse(text):
    if type(text) is not str or len(text) != _LEN or not text.endswith("Z") or text[19] != ".":
        raise ValueError("invalid instant")
    frac = text[20:23]
    if not frac.isdigit():
        raise ValueError("invalid instant")
    parsed = datetime.strptime(text[:19], "%Y-%m-%dT%H:%M:%S")
    return parsed.replace(tzinfo=timezone.utc, microsecond=int(frac) * 1000)


def rewrite_sqlite(conn, table, column):
    rows = conn.execute(f"SELECT rowid, {column} FROM {table}").fetchall()
    for rowid, value in rows:
        kind = type(value)
        if kind is bool or kind not in (int, float):
            continue
        conn.execute(
            f"UPDATE {table} SET {column} = ? WHERE rowid = ?",
            (format(from_unix_seconds(value)), rowid),
        )


def from_unix_seconds(seconds):
    whole = int(seconds)
    ms = int(round((float(seconds) - whole) * 1000))
    if ms >= 1000:
        whole += 1
        ms -= 1000
    if ms < 0:
        whole -= 1
        ms += 1000
    return datetime.fromtimestamp(whole, tz=timezone.utc).replace(microsecond=ms * 1000)
