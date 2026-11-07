import asyncio

import aiohttp

CONNECT_TIMEOUT_S = 10.0
IO_TIMEOUT_S = 60.0
LONG_IO_TIMEOUT_S = 300.0
TURN_TIMEOUT_S = 1800.0
STREAM_IDLE_TIMEOUT_S = 120.0
WS_READ_TIMEOUT_S = 90.0
PROCESS_STOP_TIMEOUT_S = 5.0
LOCK_TIMEOUT_S = 1800.0
DB_TIMEOUT_S = 60.0
IDLE_WAIT_TIMEOUT_S = 30.0
DRAIN_TIMEOUT_S = 30.0


def http_timeout(total=LONG_IO_TIMEOUT_S, sock_read=None):
    read = total if sock_read is None else sock_read
    connect = CONNECT_TIMEOUT_S if total is None else min(CONNECT_TIMEOUT_S, total)
    return aiohttp.ClientTimeout(
        total=total,
        connect=connect,
        sock_connect=connect,
        sock_read=read,
    )


def ws_timeout(sock_read=WS_READ_TIMEOUT_S):
    return aiohttp.ClientTimeout(
        total=None,
        connect=CONNECT_TIMEOUT_S,
        sock_connect=CONNECT_TIMEOUT_S,
        sock_read=sock_read,
    )


class DeadlineExceeded(TimeoutError):
    pass


async def wait(awaitable, timeout, where):
    try:
        return await asyncio.wait_for(awaitable, timeout=timeout)
    except asyncio.TimeoutError as exc:
        raise DeadlineExceeded("%s timed out after %ss" % (where, timeout)) from exc


async def read_line(stream, where, timeout=TURN_TIMEOUT_S):
    return await wait(stream.readline(), timeout, where)


async def read_some(stream, n, where, timeout=TURN_TIMEOUT_S):
    return await wait(stream.read(n), timeout, where)


class BoundedLock:
    def __init__(self, *, where, timeout=LOCK_TIMEOUT_S):
        self._lock = asyncio.Lock()
        self._where = where
        self._timeout = timeout

    async def __aenter__(self):
        await wait(self._lock.acquire(), self._timeout, "%s lock" % self._where)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._lock.release()
        return False


class BoundedSemaphore:
    def __init__(self, value, *, where, timeout=LOCK_TIMEOUT_S):
        self._sem = asyncio.Semaphore(value)
        self._where = where
        self._timeout = timeout

    async def __aenter__(self):
        await wait(self._sem.acquire(), self._timeout, "%s semaphore" % self._where)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._sem.release()
        return False
