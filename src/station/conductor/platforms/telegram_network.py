import asyncio
import ipaddress
import socket
from typing import Iterable

import aiohttp
from station import logger
from station.client.retry import client_session
from station.timeouts import wait
from station.conductor.util import ext_str
from station.prototypes.boundary import ext_dict, ext_list

_TELEGRAM_API_HOST = "api.telegram.org"
_DOH_TIMEOUT = 4.0
_DOH_PROVIDERS: list[dict] = [
    {"url": "https://dns.google/resolve", "params": {"name": _TELEGRAM_API_HOST, "type": "A"}, "headers": {}},
    {"url": "https://cloudflare-dns.com/dns-query", "params": {"name": _TELEGRAM_API_HOST, "type": "A"}, "headers": {"Accept": "application/dns-json"}},
]
_SEED_FALLBACK_IPS: list[str] = ["149.154.167.220"]

def normalize_fallback_ips(values: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    for value in values:
        raw = value.strip()
        if not raw:
            continue
        try:
            addr = ipaddress.ip_address(raw)
        except ValueError:
            continue
        if addr.version != 4:
            continue
        if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_unspecified:
            continue
        normalized.append(str(addr))
    return normalized

def parse_fallback_ip_env(value: str | None) -> list[str]:
    if not value:
        return []
    parts = [part.strip() for part in value.split(",")]
    return normalize_fallback_ips(parts)

def _resolve_system_dns() -> set[str]:
    try:
        results = socket.getaddrinfo(_TELEGRAM_API_HOST, 443, socket.AF_INET)
        return {addr[4][0] for addr in results}
    except OSError as exc:
        logger.error("telegram system dns failed error=%s", exc)
        raise

async def _query_doh_provider(session: aiohttp.ClientSession, provider: dict) -> list[str]:
    try:
        async with session.get(provider["url"], params=provider["params"], headers=provider["headers"]) as resp:
            if resp.status != 200:
                raw = await resp.text()
                raise RuntimeError("doh status=%s body=%s" % (resp.status, raw[:300]))
            data = await resp.json(content_type=None)
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
        logger.error("telegram doh query failed url=%s error=%s", provider["url"], exc)
        raise
    data = ext_dict('doh response', data)
    answers = data.get("Answer")
    if answers is None:
        answers = []
    else:
        answers = ext_list('doh Answer', answers)
    ips: list[str] = []
    for answer in answers:
        answer = ext_dict('doh Answer item', answer)
        if answer.get("type") != 1:
            continue
        raw = ext_str(answer.get("data"), "data").strip()
        try:
            ipaddress.ip_address(raw)
            ips.append(raw)
        except ValueError:
            continue
    return ips

async def discover_fallback_ips() -> list[str]:
    timeout = aiohttp.ClientTimeout(total=_DOH_TIMEOUT)
    async with client_session(timeout=timeout) as session:
        doh_tasks = [_query_doh_provider(session, p) for p in _DOH_PROVIDERS]
        async def _system_dns():
            return await wait(asyncio.to_thread(_resolve_system_dns), _DOH_TIMEOUT, "telegram system dns")

        results = await asyncio.gather(_system_dns(), *doh_tasks, return_exceptions=True)

    doh_ips: list[str] = []
    for r in results:
        if isinstance(r, BaseException):
            logger.error("telegram dns discovery failed error=%s", r, exc_info=r)
            continue
        if isinstance(r, list):
            doh_ips.extend(r)
        elif isinstance(r, set):
            doh_ips.extend(r)

    seen: set[str] = set()
    candidates: list[str] = []
    for ip in doh_ips:
        if ip not in seen:
            seen.add(ip)
            candidates.append(ip)

    validated = normalize_fallback_ips(candidates)
    if validated:
        return validated
    return list(_SEED_FALLBACK_IPS)

class TelegramFallbackResolver(aiohttp.abc.AbstractResolver):
    def __init__(self, primary: aiohttp.abc.AbstractResolver | None, fallback_ips: list[str]):
        self._primary = primary or aiohttp.resolver.DefaultResolver()
        self._fallback_ips = list(dict.fromkeys(normalize_fallback_ips(fallback_ips)))
        self._sticky_ip: str | None = None

    async def resolve(self, host: str, port: int = 0, family: int = socket.AF_INET) -> list[dict]:
        if host != _TELEGRAM_API_HOST or not self._fallback_ips:
            return await self._primary.resolve(host, port, family)

        try:
            results = await self._primary.resolve(host, port, family)
        except OSError as exc:
            logger.error("telegram resolver failed host=%s error=%s", host, exc)
            results = []

        primary_hosts = []
        for r in results or []:
            r = ext_dict('resolver result', r)
            host = r.get("host")
            if host is None or host == "":
                continue
            host = ext_str(host, 'host')
            primary_hosts.append(host)

        attempt_ips = []
        if self._sticky_ip:
            attempt_ips.append(self._sticky_ip)
        for ip in primary_hosts + self._fallback_ips:
            if ip not in attempt_ips:
                attempt_ips.append(ip)

        out: list[dict] = []
        for ip in attempt_ips:
            out.append(
                {
                    "hostname": host,
                    "host": ip,
                    "port": port,
                    "family": socket.AF_INET,
                    "proto": 0,
                    "flags": socket.AI_NUMERICHOST,
                }
            )
        return out

    async def close(self) -> None:
        try:
            await self._primary.close()
        except Exception as e:
            logger.error("unexpected where=telegram_resolver_close error=%s", e, exc_info=e)

