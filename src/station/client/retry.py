import asyncio
import contextvars
import errno
import random
import time
from urllib.parse import urlparse

import aiohttp

from station import logger

_circuits = {}
_retry_attempts = 3
_retry_base_seconds = 0.5
_circuit_failures = 5
_circuit_cooldown_seconds = 1.0

class CircuitOpen(Exception):
    pass

class Circuit:
    def __init__(self):
        self.failures = 0
        self.open_until = 0.0

    def allow(self):
        if time.monotonic() < self.open_until:
            raise CircuitOpen()

    def success(self):
        self.failures = 0

    def failure(self):
        self.failures += 1
        if self.failures >= _circuit_failures:
            self.open_until = time.monotonic() + _circuit_cooldown_seconds
            self.failures = 0
            return True
        return False

def apply_retry_config(server):
    global _retry_attempts, _retry_base_seconds, _circuit_failures, _circuit_cooldown_seconds
    _retry_attempts = server.outbound_retry_attempts
    _retry_base_seconds = server.outbound_retry_base_seconds
    _circuit_failures = server.outbound_circuit_failures
    _circuit_cooldown_seconds = server.outbound_circuit_cooldown_seconds

def circuit_for(name):
    circuit = _circuits.get(name)
    if circuit is None:
        circuit = Circuit()
        _circuits[name] = circuit
    return circuit

class RetryableError(Exception):
    def __init__(self, message="", *, delay=None):
        super().__init__(message)
        self.delay = delay


_request_level_owner = contextvars.ContextVar("station_request_retry", default=False)

_RETRYABLE_ERRNO = frozenset({
    errno.EAGAIN,
    errno.ECONNABORTED,
    errno.ECONNREFUSED,
    errno.ECONNRESET,
    errno.EHOSTUNREACH,
    errno.ENETUNREACH,
    errno.EPIPE,
    errno.ETIMEDOUT,
    errno.EWOULDBLOCK,
})


def is_retryable_error(exc):
    if isinstance(exc, RetryableError):
        return True
    if isinstance(exc, (aiohttp.ClientConnectorCertificateError, aiohttp.InvalidURL, aiohttp.ServerFingerprintMismatch)):
        return False
    if isinstance(exc, aiohttp.ClientHttpProxyError):
        status = exc.status
        return status == 408 or status == 429 or status >= 500
    if isinstance(exc, (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError, asyncio.TimeoutError, TimeoutError, ConnectionError)):
        return True
    if isinstance(exc, OSError) and exc.errno in _RETRYABLE_ERRNO:
        return True
    return False


def retry_sleep_seconds(attempt, *, delay=None, limit=None):
    if delay is not None and delay > 0:
        sleep_time = min(float(delay), 120.0)
    else:
        sleep_time = (_retry_base_seconds * (2 ** attempt)) + (random.random() * 0.5)
    if limit is not None:
        sleep_time = min(sleep_time, limit)
    return sleep_time


def _retry_delay_of(exc):
    if isinstance(exc, RetryableError) and exc.delay is not None and exc.delay > 0:
        return exc.delay
    return None


def _request_host(url):
    return urlparse(str(url)).hostname or ""


def _retry_after_header(resp):
    raw = resp.headers.get("Retry-After")
    if raw is None:
        return None
    text = str(raw).strip()
    if not text.isdigit():
        return None
    return min(int(text), 120)


def _replayable_body(data):
    return data is None or isinstance(data, (bytes, bytearray, memoryview, str, dict))

def is_transient_status(status):
    return status == 429 or status >= 500

async def retry_transient(func, *args, circuit_name="outbound", **kwargs):
    circuit = circuit_for(circuit_name)
    try:
        circuit.allow()
    except CircuitOpen:
        logger.warning("circuit open name=%s", circuit_name)
        raise
    token = _request_level_owner.set(True)
    try:
        for attempt in range(_retry_attempts + 1):
            try:
                result = await func(*args, **kwargs)
                circuit.success()
                if attempt:
                    logger.info("retry recovered name=%s attempt=%s", circuit_name, attempt + 1)
                return result
            except CircuitOpen:
                raise
            except Exception as e:
                if not is_retryable_error(e):
                    raise
                opened = circuit.failure()
                if opened:
                    logger.warning("circuit open name=%s error=%s", circuit_name, e)
                if attempt == _retry_attempts:
                    raise
                sleep_time = retry_sleep_seconds(attempt, delay=_retry_delay_of(e))
                logger.warning(
                    "retry attempt=%s/%s name=%s sleep_s=%.2f error=%s",
                    attempt + 1,
                    _retry_attempts,
                    circuit_name,
                    sleep_time,
                    e,
                )
                await asyncio.sleep(sleep_time)
    finally:
        _request_level_owner.reset(token)


class RetryConnector(aiohttp.TCPConnector):
    async def _create_connection(self, req, traces, timeout):
        where = "proxy" if req.proxy else "connect"
        for attempt in range(_retry_attempts + 1):
            try:
                proto = await super()._create_connection(req, traces, timeout)
                if attempt:
                    logger.info("%s retry recovered host=%s attempt=%s", where, req.host, attempt + 1)
                return proto
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not is_retryable_error(e) or attempt == _retry_attempts:
                    raise
                sleep_time = retry_sleep_seconds(attempt, limit=0.3)
                logger.warning(
                    "%s retry host=%s attempt=%s/%s sleep_s=%.2f error=%s",
                    where,
                    req.host,
                    attempt + 1,
                    _retry_attempts,
                    sleep_time,
                    e,
                )
                await asyncio.sleep(sleep_time)


class RetryClientSession(aiohttp.ClientSession):
    async def _request(self, method, str_or_url, **kwargs):
        if _request_level_owner.get() or not _replayable_body(kwargs.get("data")):
            return await super()._request(method, str_or_url, **kwargs)
        host = _request_host(str_or_url)
        for attempt in range(_retry_attempts + 1):
            try:
                resp = await super()._request(method, str_or_url, **kwargs)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not is_retryable_error(e) or attempt == _retry_attempts:
                    raise
                sleep_time = retry_sleep_seconds(attempt, delay=_retry_delay_of(e))
                logger.warning(
                    "request retry method=%s host=%s attempt=%s/%s sleep_s=%.2f error=%s",
                    method,
                    host,
                    attempt + 1,
                    _retry_attempts,
                    sleep_time,
                    e,
                )
                await asyncio.sleep(sleep_time)
                continue
            if not is_transient_status(resp.status) or attempt == _retry_attempts:
                if attempt and not is_transient_status(resp.status):
                    logger.info("request retry recovered method=%s host=%s attempt=%s", method, host, attempt + 1)
                return resp
            delay = _retry_after_header(resp)
            read_error = None
            try:
                await resp.read()
            except Exception as e:
                read_error = e
            try:
                await resp.release()
            except Exception as e:
                if read_error is None:
                    read_error = e
            if read_error is not None and not is_retryable_error(read_error):
                raise read_error
            sleep_time = retry_sleep_seconds(attempt, delay=delay)
            logger.warning(
                "request retry method=%s host=%s attempt=%s/%s status=%s sleep_s=%.2f",
                method,
                host,
                attempt + 1,
                _retry_attempts,
                resp.status,
                sleep_time,
            )
            await asyncio.sleep(sleep_time)


def client_connector(**kwargs):
    return RetryConnector(**kwargs)


def client_session(*, connector=None, request_retry=True, **kwargs):
    if connector is None:
        connector = RetryConnector()
    if request_retry:
        return RetryClientSession(connector=connector, **kwargs)
    return aiohttp.ClientSession(connector=connector, **kwargs)
