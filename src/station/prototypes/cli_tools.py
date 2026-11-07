import asyncio
import os
import re
import shutil
import signal
import uuid
from dataclasses import dataclass

from station import logger
from station.errors import ExternalError
from station.gateway_processes import install_parent_death_signal
from station.prototypes.boundary import ext_dict, ext_float, ext_list, ext_str
from station.timeouts import PROCESS_STOP_TIMEOUT_S, DeadlineExceeded, wait

MAX_RUNNING = 8
MAX_KEPT = 32
MAX_WAIT_S = 300.0
DEFAULT_WAIT_S = 60.0
TAIL_CHARS = 4000

CLI_TOOL_NAMES = frozenset({"cli_run", "cli_status", "sleep", "wait"})

CLI_TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "cli_run",
            "description": (
                "Start a shell command in the workspace and return immediately. "
                "stdout and stderr are written to output_path. "
                "This call does not wait for the command. "
                "Use cli_status to check the file later, or sleep/wait to pause until it exits."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "command": {"type": "string"},
                    "cwd": {"type": "string", "description": "Directory inside the workspace. Defaults to the workspace root."},
                },
                "required": ["command"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "cli_status",
            "description": (
                "Read one cli run without waiting. "
                "Returns status, exit_code when the process has exited, output_path, size in bytes, and output. "
                "When the file is longer than the preview, output is the start, an omission line, and the end."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "string"},
                },
                "required": ["run_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "sleep",
            "description": (
                "Pause until seconds elapses. "
                "If run_ids is set, return early when every listed cli run has exited. "
                "A round has a finite timeout. If seconds is longer than the time left in this round, this call waits only for the time left and returns pending. "
                "Call sleep again on the next round to wait longer. "
                "Does not start commands."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "seconds": {"type": "number"},
                    "run_ids": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["seconds"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "wait",
            "description": (
                "Wait until every run_id has exited, or until seconds elapses. "
                "Returns each run's status, output_path, and output. "
                "When the file is longer than the preview, output is the start, an omission line, and the end. "
                "seconds defaults to 60. A round has a finite timeout. "
                "If the runs are still going when the round times out, this call returns pending and the runs keep going. "
                "Call wait again on the next round to keep waiting."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "run_ids": {"type": "array", "items": {"type": "string"}},
                    "seconds": {"type": "number"},
                },
                "required": ["run_ids"],
            },
        },
    },
]


@dataclass(slots=True)
class CliRun:
    run_id: str
    command: str
    cwd: str
    output_path: str
    proc: asyncio.subprocess.Process
    done: asyncio.Event
    watcher: asyncio.Task | None = None
    status: str = "running"
    exit_code: int | None = None


_ANSI = re.compile(r"\x1b(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
_SECRET_VALUE = re.compile(
    r"(?i)"
    r"((?:app[\s_-]*password|password(?:\.raw)?|auth\.raw|api[\s_-]*key|access[\s_-]*token|refresh[\s_-]*token|authorization)"
    r"\s*['\"]?\s*[:=]\s*['\"]?)"
    r"([^\s'\",}]+)"
)
_BEARER = re.compile(r"(?i)(\bbearer\s+)([A-Za-z0-9._\-+/=]{8,})")


def redact_tool_text(text):
    if type(text) is not str or not text:
        return "" if text is None else text
    text = _ANSI.sub("", text)
    text = _BEARER.sub(r"\1***", text)
    return _SECRET_VALUE.sub(r"\1***", text)


def _preview(path):
    try:
        size = os.path.getsize(path)
    except OSError:
        return "", 0
    head_bytes = int(TAIL_CHARS * 0.4)
    tail_bytes = TAIL_CHARS - head_bytes
    try:
        with open(path, "rb") as handle:
            if size <= TAIL_CHARS:
                raw = handle.read(TAIL_CHARS)
                return redact_tool_text(raw.decode("utf-8", errors="replace")), size
            head_raw = handle.read(head_bytes)
            handle.seek(size - tail_bytes)
            tail_raw = handle.read(tail_bytes)
    except OSError:
        return "", size
    head = redact_tool_text(head_raw.decode("utf-8", errors="replace"))
    tail = redact_tool_text(tail_raw.decode("utf-8", errors="replace"))
    omitted = size - head_bytes - tail_bytes
    notice = "\n\n... [OUTPUT TRUNCATED - %d chars omitted out of %d total] ...\n\n" % (omitted, size)
    return head + notice + tail, size


def _run_ids(arguments, *, required):
    raw = arguments.get("run_ids")
    if raw is None:
        if required:
            raise ExternalError("run_ids is required")
        return []
    items = ext_list("run_ids", raw)
    ids = []
    for index, item in enumerate(items):
        text = ext_str("run_ids[%d]" % index, item)
        if text:
            ids.append(text)
    if required and not ids:
        raise ExternalError("run_ids is required")
    if len(ids) > MAX_RUNNING:
        raise ExternalError("too many run_ids")
    return ids


def _seconds(value, *, default=None, limit=None):
    if value is None:
        if default is None:
            raise ExternalError("seconds is required")
        value = default
    seconds = float(ext_float("seconds", value))
    if seconds <= 0:
        raise ExternalError("seconds must be greater than 0")
    cap = MAX_WAIT_S
    if limit is not None:
        cap = min(cap, max(0.0, float(limit)))
    if seconds > cap:
        if limit is None:
            raise ExternalError("seconds cannot exceed %s" % int(MAX_WAIT_S))
        return cap, seconds
    return seconds, seconds


class CliRegistry:
    def __init__(self, *, workspace, output_dir):
        self.workspace = os.path.realpath(workspace)
        self.output_dir = output_dir
        self._runs = {}
        self._order = []
        self._lock = asyncio.Lock()
        self._closed = False
        os.makedirs(self.workspace, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

    def _resolve_cwd(self, cwd):
        if cwd is None or not str(cwd).strip():
            path = self.workspace
        else:
            raw = str(cwd).strip()
            path = raw if os.path.isabs(raw) else os.path.join(self.workspace, raw)
        real = os.path.realpath(path)
        if not os.path.isdir(real):
            raise ExternalError("cwd is not a directory")
        if os.path.commonpath([self.workspace, real]) != self.workspace:
            raise ExternalError("cwd must stay inside the workspace")
        return real

    def _snapshot(self, run):
        code = run.proc.returncode
        if code is not None:
            run.exit_code = code
            run.status = "exited"
        preview, size = _preview(run.output_path)
        body = {
            "ok": True,
            "run_id": run.run_id,
            "status": run.status,
            "output_path": run.output_path,
            "bytes": size,
            "output": preview,
        }
        if size > TAIL_CHARS:
            body["truncated"] = True
        if run.exit_code is not None:
            body["exit_code"] = run.exit_code
        if run.proc.pid:
            body["pid"] = run.proc.pid
        return body

    def _running_count(self):
        count = 0
        for run in self._runs.values():
            if run.proc.returncode is None and run.status == "running":
                count += 1
        return count

    def _evict_finished(self):
        finished = [run_id for run_id in self._order if self._runs[run_id].status == "exited"]
        extra = len(finished) - MAX_KEPT
        if extra <= 0:
            return
        for run_id in finished[:extra]:
            self._runs.pop(run_id, None)
            self._order.remove(run_id)

    async def run(self, command, cwd=None):
        command = (command or "").strip()
        if not command:
            return {"ok": False, "error": "command is required"}
        directory = self._resolve_cwd(cwd)
        async with self._lock:
            if self._closed:
                return {"ok": False, "error": "cli tools are closed"}
            if self._running_count() >= MAX_RUNNING:
                return {"ok": False, "error": "too many cli runs"}
            run_id = uuid.uuid4().hex[:12]
            output_path = os.path.join(self.output_dir, "%s.log" % run_id)
            argv = ["/bin/sh", "-c", command]
            if shutil.which("stdbuf"):
                argv = ["stdbuf", "-oL", "-eL"] + argv
            # Python resets stdio buffering when stdout is not a terminal, which
            # overrides stdbuf. PYTHONUNBUFFERED makes those prints reach the log
            # while the process is still running.
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            log_file = open(output_path, "wb")
            try:
                proc = await asyncio.create_subprocess_exec(
                    *argv,
                    cwd=directory,
                    env=env,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=log_file,
                    stderr=asyncio.subprocess.STDOUT,
                    start_new_session=True,
                    preexec_fn=install_parent_death_signal,
                )
            except OSError as exc:
                logger.error("cli spawn failed command=%s error=%s", command, exc)
                try:
                    os.remove(output_path)
                except OSError:
                    pass
                return {"ok": False, "error": "cli spawn failed: %s" % exc}
            finally:
                log_file.close()
            run = CliRun(
                run_id=run_id,
                command=command,
                cwd=directory,
                output_path=output_path,
                proc=proc,
                done=asyncio.Event(),
            )
            run.watcher = asyncio.create_task(self._watch(run), name="cli-%s" % run_id)
            self._runs[run_id] = run
            self._order.append(run_id)
            self._evict_finished()
            return {
                "ok": True,
                "run_id": run_id,
                "pid": proc.pid,
                "status": "running",
                "output_path": output_path,
            }

    async def _watch(self, run):
        try:
            await run.proc.wait()
        except asyncio.CancelledError:
            run.done.set()
            raise
        except Exception as exc:
            logger.error("cli watch failed run_id=%s error=%s", run.run_id, exc)
        run.exit_code = run.proc.returncode
        run.status = "exited"
        run.done.set()

    async def status(self, run_id):
        run_id = (run_id or "").strip()
        if not run_id:
            return {"ok": False, "error": "run_id is required"}
        async with self._lock:
            run = self._runs.get(run_id)
        if run is None:
            return {"ok": False, "error": "unknown run_id"}
        return self._snapshot(run)

    def _known(self, run_ids):
        missing = [run_id for run_id in run_ids if run_id not in self._runs]
        return missing

    async def _wait_until(self, run_ids, seconds):
        events = []
        async with self._lock:
            for run_id in run_ids:
                run = self._runs.get(run_id)
                if run is None:
                    continue
                if run.proc.returncode is None and not run.done.is_set():
                    events.append(run.done)
        if not events:
            return
        waiters = [asyncio.create_task(event.wait()) for event in events]
        try:
            await asyncio.wait(waiters, timeout=seconds)
        finally:
            for waiter in waiters:
                if not waiter.done():
                    waiter.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)

    def _result_runs(self, run_ids):
        rows = []
        pending = False
        for run_id in run_ids:
            run = self._runs.get(run_id)
            if run is None:
                rows.append({"ok": False, "run_id": run_id, "error": "unknown run_id"})
                continue
            snap = self._snapshot(run)
            if snap["status"] != "exited":
                pending = True
            rows.append(snap)
        return rows, pending

    async def sleep(self, seconds, run_ids=None, *, limit=None):
        waited, requested = _seconds(seconds, limit=limit)
        ids = _run_ids({"run_ids": run_ids}, required=False)
        async with self._lock:
            missing = self._known(ids)
        if missing:
            return {"ok": False, "error": "unknown run_id", "run_ids": missing}
        if waited > 0:
            if ids:
                await self._wait_until(ids, waited)
            else:
                await asyncio.sleep(waited)
        rows, pending = self._result_runs(ids)
        if not ids and waited < requested:
            pending = True
        result = {"ok": True, "seconds": waited, "pending": pending, "runs": rows}
        if waited != requested:
            result["requested"] = requested
        return result

    async def wait(self, run_ids, seconds=None, *, limit=None):
        ids = _run_ids({"run_ids": run_ids}, required=True)
        waited, requested = _seconds(seconds, default=DEFAULT_WAIT_S, limit=limit)
        async with self._lock:
            missing = self._known(ids)
        if missing:
            return {"ok": False, "error": "unknown run_id", "run_ids": missing}
        if waited > 0:
            await self._wait_until(ids, waited)
        rows, pending = self._result_runs(ids)
        result = {"ok": True, "seconds": waited, "pending": pending, "runs": rows}
        if waited != requested:
            result["requested"] = requested
        return result

    async def dispatch(self, name, arguments):
        try:
            arguments = ext_dict("arguments", arguments if arguments is not None else {})
            if name == "cli_run":
                cwd = arguments.get("cwd")
                if cwd is not None:
                    cwd = ext_str("cwd", cwd)
                return await self.run(ext_str("command", arguments.get("command")), cwd=cwd or None)
            if name == "cli_status":
                return await self.status(ext_str("run_id", arguments.get("run_id")))
            if name == "sleep":
                return await self.sleep(arguments.get("seconds"), run_ids=_run_ids(arguments, required=False))
            if name == "wait":
                seconds = arguments.get("seconds")
                return await self.wait(_run_ids(arguments, required=True), seconds=seconds)
        except ExternalError as exc:
            return {"ok": False, "error": str(exc)}
        return {"ok": False, "error": "unknown tool"}

    async def _stop_run(self, run):
        proc = run.proc
        if proc.returncode is None:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError as exc:
                logger.info("cli process already exited run_id=%s error=%s", run.run_id, exc)
            except OSError as exc:
                logger.error("cli terminate failed run_id=%s error=%s", run.run_id, exc)
            try:
                await wait(proc.wait(), PROCESS_STOP_TIMEOUT_S, "cli terminate")
            except DeadlineExceeded:
                logger.error("cli process did not exit after terminate run_id=%s pid=%s", run.run_id, proc.pid)
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError as exc:
                    logger.info("cli process already exited during kill run_id=%s error=%s", run.run_id, exc)
                await wait(proc.wait(), PROCESS_STOP_TIMEOUT_S, "cli kill")
        run.exit_code = proc.returncode
        run.status = "exited"
        run.done.set()
        watcher = run.watcher
        if watcher is not None and not watcher.done():
            try:
                await wait(watcher, PROCESS_STOP_TIMEOUT_S, "cli watcher")
            except DeadlineExceeded:
                watcher.cancel()
                try:
                    await wait(watcher, PROCESS_STOP_TIMEOUT_S, "cli watcher cancel")
                except (asyncio.CancelledError, DeadlineExceeded) as exc:
                    logger.info("cli watcher cancel run_id=%s error=%s", run.run_id, exc)

    async def close(self):
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            runs = [self._runs[run_id] for run_id in self._order if run_id in self._runs]
        for run in runs:
            await self._stop_run(run)


class PrototypeCli:
    def cli_tools(self):
        registry = getattr(self, "_cli_tools", None)
        if registry is None:
            root = self._workspace_root()
            registry = CliRegistry(
                workspace=root,
                output_dir=os.path.join(root, "cli_runs"),
            )
            self._cli_tools = registry
        return registry

    async def _stop_cli_tools(self):
        registry = getattr(self, "_cli_tools", None)
        if registry is None:
            return
        await registry.close()
        self._cli_tools = None

    async def _stop_chat(self, *, chat_id, acct_id):
        await self._stop_cli_tools()
        await super()._stop_chat(chat_id=chat_id, acct_id=acct_id)

    async def stop(self):
        await self._stop_cli_tools()
        return await super().stop()
