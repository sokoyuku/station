import asyncio
import json

from station import logger
from station.prototypes.gateway_process import close_subprocess
from station.timeouts import DRAIN_TIMEOUT_S, TURN_TIMEOUT_S, BoundedLock, read_line, read_some, wait

class JsonLineRpcProcess:
    def __init__(self, *, request_id_prefix, stop_error):
        self.proc = None
        self._next_request_id = 0
        self._pending = {}
        self._stdout_task = None
        self._stderr_task = None
        self._write_lock = BoundedLock(where="%s stdin" % request_id_prefix)
        self._request_id_prefix = request_id_prefix
        self._stop_error = stop_error
        self._line_buf = b""

    async def close(self):
        proc = self.proc
        self.proc = None
        tasks = [self._stdout_task, self._stderr_task]
        self._stdout_task = None
        self._stderr_task = None
        await close_subprocess(proc, tasks, terminate_timeout_s=5.0)
        self._fail_pending(self._stop_error)

    def _new_request_id(self):
        self._next_request_id += 1
        return "%s%d" % (self._request_id_prefix, self._next_request_id)

    async def _write_line(self, message):
        if self.proc is None or self.proc.returncode is not None or self.proc.stdin is None:
            raise RuntimeError(self._stop_error)
        line = json.dumps(message, ensure_ascii=True)
        async with self._write_lock:
            self.proc.stdin.write((line + "\n").encode("utf-8"))
            await wait(self.proc.stdin.drain(), DRAIN_TIMEOUT_S, "%s stdin drain" % self._request_id_prefix)

    async def _request_future(self, message, *, timeout_s=TURN_TIMEOUT_S):
        if self.proc is None or self.proc.returncode is not None or self.proc.stdin is None:
            raise RuntimeError(self._stop_error)
        request_id = self._new_request_id()
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        payload = dict(message)
        payload["id"] = request_id
        await self._write_line(payload)
        try:
            return await wait(future, timeout_s, "%s request" % self._request_id_prefix)
        finally:
            self._pending.pop(request_id, None)

    def _resolve_pending(self, request_id, *, result=None, error=None):
        future = self._pending.get(request_id)
        if future is None or future.done():
            return False
        if error is not None:
            future.set_exception(error)
        else:
            future.set_result(result)
        return True

    def _fail_pending(self, message):
        for future in list(self._pending.values()):
            if not future.done():
                future.set_exception(RuntimeError(message))
        self._pending.clear()

    async def _read_stdout_lines(self, *, on_line, buffered=False):
        proc = self.proc
        if buffered:
            while True:
                chunk = await read_some(proc.stdout, 65536, "%s stdout" % self._request_id_prefix)
                if not chunk:
                    break
                self._line_buf += chunk
                while True:
                    idx = self._line_buf.find(b"\n")
                    if idx < 0:
                        break
                    raw_line = self._line_buf[:idx]
                    self._line_buf = self._line_buf[idx + 1 :]
                    if raw_line.endswith(b"\r"):
                        raw_line = raw_line[:-1]
                    if not raw_line:
                        continue
                    await on_line(raw_line.decode("utf-8", "replace"))
        else:
            while True:
                line = await read_line(proc.stdout, "%s stdout" % self._request_id_prefix)
                if not line:
                    break
                raw = line.decode("utf-8", "replace").strip()
                if raw:
                    await on_line(raw)

    async def _read_stderr_lines(self, *, label):
        proc = self.proc
        while True:
            line = await read_line(proc.stderr, "%s stderr" % self._request_id_prefix)
            if not line:
                break
            text = line.decode("utf-8", "replace").rstrip()
            if text:
                logger.info("%s: %s", label, text)

    def _parse_json_line(self, text, *, label):

        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            logger.error("%s malformed JSON error=%s text=%s", label, exc, text[:500])
            raise RuntimeError("%s malformed JSON" % label) from exc

    async def _pump_stdout(self, *, label, on_frame, buffered=True, on_close=None):
        async def on_line(raw):
            frame = self._parse_json_line(raw, label=label)
            await on_frame(frame)

        try:
            await self._read_stdout_lines(on_line=on_line, buffered=buffered)
        except Exception:
            self._fail_pending("%s stdout failed" % label)
            raise
        if on_close is not None:
            on_close()
            return
        self._fail_pending("%s stdout closed" % label)
