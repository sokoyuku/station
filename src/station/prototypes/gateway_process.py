import asyncio
from dataclasses import dataclass

from station import logger
from station.gateway_processes import install_parent_death_signal
from station.timeouts import PROCESS_STOP_TIMEOUT_S, DeadlineExceeded, wait

@dataclass(slots=True)
class SessionEvent:
    type: str
    session_id: str
    payload: dict

class SessionEventBus:
    def __init__(self):
        self._queues = {}

    def queue(self, session_id):
        queue = self._queues.get(session_id)
        if queue is None:
            queue = asyncio.Queue()
            self._queues[session_id] = queue
        return queue

    async def put(self, event):
        if event.session_id:
            await self.queue(event.session_id).put(event)

    def drop_queued(self, session_id):
        queue = self._queues.get(session_id)
        if queue is None:
            return
        while True:
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                return

    def clear(self):
        self._queues.clear()

async def close_subprocess(proc, tasks, *, terminate_timeout_s=5.0):
    if proc is not None and proc.returncode is None:
        proc.terminate()
        try:
            await wait(proc.wait(), terminate_timeout_s, "process terminate")
        except DeadlineExceeded:
            logger.error("process did not exit after terminate pid=%s; killing", proc.pid)
            proc.kill()
            await wait(proc.wait(), terminate_timeout_s, "process kill")
        except ProcessLookupError as exc:
            logger.info("process already exited during terminate pid=%s error=%s", proc.pid, exc)
    for task in tasks:
        if task is None:
            continue
        task.cancel()
        try:
            await wait(task, PROCESS_STOP_TIMEOUT_S, "cancelled task")
        except asyncio.CancelledError:
            logger.info("cancelled task finished name=%s", task.get_name())
        except DeadlineExceeded:
            logger.error("cancelled task did not finish name=%s", task.get_name())
            raise

def gateway_subprocess_kwargs():
    return {"preexec_fn": install_parent_death_signal}
