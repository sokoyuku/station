import os
import re

from aiohttp import web

from station.api.token import _require_admin
from station.errors import ExternalError

_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
_RANK = {name: index for index, name in enumerate(_LEVELS)}
_LINE = re.compile(
    r"^(?P<time>.*?) - \[(?P<request>.*?)\] - (?P<name>.*?) - "
    r"(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL) - (?P<msg>.*)$"
)
_TAIL_CAP = 5000
_ALL_CAP = 50000


def _log_path(app):
    recorder = app.get("recorder")
    path = getattr(recorder, "log_path", None) if recorder is not None else None
    if path:
        return path
    config = app["config"]
    directory = config.recorder.dir_path
    if not directory:
        return ""
    port = config.port
    name = "station-%s.log" % port if port else "station.log"
    return os.path.join(directory, name)


def _files(path, include_all):
    if not path:
        return []
    names = []
    if include_all:
        directory = os.path.dirname(path) or "."
        base = os.path.basename(path)
        try:
            entries = os.listdir(directory)
        except OSError:
            entries = []
        rotated = []
        for entry in entries:
            if entry == base:
                continue
            prefix = base + "."
            if entry.startswith(prefix) and entry[len(prefix):].isdigit():
                rotated.append((int(entry[len(prefix):]), os.path.join(directory, entry)))
        rotated.sort()
        names.extend(item[1] for item in rotated)
    if os.path.isfile(path):
        names.append(path)
    return names


def _read_records(paths):
    records = []
    for path in paths:
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as handle:
                for raw in handle:
                    text = raw.rstrip("\n")
                    match = _LINE.match(text)
                    if match is None and records:
                        records[-1]["text"] += "\n" + text
                        records[-1]["msg"] += "\n" + text
                        continue
                    if match is None:
                        records.append(
                            {
                                "file": os.path.basename(path),
                                "time": "",
                                "request_id": "",
                                "name": "",
                                "level": "",
                                "msg": text,
                                "text": text,
                            }
                        )
                        continue
                    records.append(
                        {
                            "file": os.path.basename(path),
                            "time": match.group("time"),
                            "request_id": match.group("request"),
                            "name": match.group("name"),
                            "level": match.group("level"),
                            "msg": match.group("msg"),
                            "text": text,
                        }
                    )
        except OSError:
            continue
    return records


def _keep(record, *, minimum, query, name):
    level = record.get("level") or ""
    if minimum and level in _RANK and _RANK[level] < _RANK[minimum]:
        return False
    if minimum and not level:
        return False
    if name and name not in (record.get("name") or "") and name not in record.get("text", ""):
        return False
    if query and query not in record.get("text", "").lower():
        return False
    return True


async def handle_admin_logs(request):
    denied = _require_admin(request)
    if denied is not None:
        return denied
    include_all = (request.query.get("all") or "").strip() in ("1", "true", "yes")
    minimum = (request.query.get("level") or "").strip().upper()
    if minimum and minimum not in _RANK:
        raise ExternalError("Invalid log level")
    query = (request.query.get("q") or "").strip().lower()
    name = (request.query.get("name") or "").strip()
    try:
        tail = int(request.query.get("tail") or 400)
    except ValueError:
        raise ExternalError("Invalid tail")
    if tail < 1:
        tail = 1
    if tail > _TAIL_CAP:
        tail = _TAIL_CAP
    path = _log_path(request.app)
    files = _files(path, include_all)
    matched = [row for row in _read_records(files) if _keep(row, minimum=minimum, query=query, name=name)]
    cap = _ALL_CAP if include_all else tail
    total = len(matched)
    truncated = total > cap
    if truncated:
        matched = matched[-cap:]
    return web.json_response(
        {
            "result": 0,
            "msg": "ok",
            "data": {
                "path": path,
                "files": [os.path.basename(item) for item in files],
                "lines": matched,
                "truncated": truncated,
                "matched": total,
            },
        }
    )
