import asyncio
import json
import os

from station import logger
from station.prototypes.fs_paths import write_atomic

TYPING_INTERVAL_SECONDS = 4
TYPING_FLAG_NAME = "typing_indicator.json"


def typing_flag_path(storage_dir):
    return os.path.join(storage_dir, TYPING_FLAG_NAME)


def load_typing_enabled(storage_dir):
    path = typing_flag_path(storage_dir)
    if not os.path.isfile(path):
        return True
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()
    except OSError as exc:
        logger.info("typing indicator flag unreadable path=%s error=%s", path, exc)
        return False
    if not raw.strip():
        return False
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.info("typing indicator flag unreadable path=%s error=%s", path, exc)
        return False
    if not isinstance(data, dict):
        return False
    return data.get("enabled") is True


def save_typing_enabled(storage_dir, enabled):
    write_atomic(
        typing_flag_path(storage_dir),
        json.dumps({"enabled": bool(enabled)}) + "\n",
    )


def _pulses(proto):
    tasks = getattr(proto, "_typing_pulses", None)
    if tasks is None:
        tasks = {}
        proto._typing_pulses = tasks
    return tasks


async def _pulse(proto, *, chat_id, acct_id):
    try:
        while True:
            ok = await proto.send_proxy(method="send_typing", chat_id=chat_id, acct_id=acct_id)
            if not ok:
                logger.info(
                    "typing indicator stopped model_id=%s acct_id=%s chat_id=%s",
                    getattr(proto, "model_id", ""),
                    acct_id,
                    chat_id,
                )
                return
            await asyncio.sleep(TYPING_INTERVAL_SECONDS)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.error(
            "typing indicator failed model_id=%s acct_id=%s chat_id=%s error=%s",
            getattr(proto, "model_id", ""),
            acct_id,
            chat_id,
            exc,
            exc_info=exc,
        )


class TypingWhile:
    def __init__(self, proto, *, chat_id, acct_id):
        self._proto = proto
        self._chat_id = (chat_id or "").strip()
        self._acct_id = (acct_id or "").strip()
        self._key = None

    async def __aenter__(self):
        if not self._chat_id or not self._acct_id:
            return self
        if not load_typing_enabled(self._proto.storage_dir):
            return self
        key = (self._acct_id, self._chat_id)
        tasks = _pulses(self._proto)
        if key in tasks:
            return self
        task = asyncio.create_task(
            _pulse(self._proto, chat_id=self._chat_id, acct_id=self._acct_id),
            name="typing:%s:%s" % (self._acct_id, self._chat_id),
        )
        tasks[key] = task
        self._key = key
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._key is None:
            return False
        tasks = _pulses(self._proto)
        task = tasks.pop(self._key, None)
        self._key = None
        if task is None:
            return False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.info("typing indicator ended error=%s", exc)
        return False
