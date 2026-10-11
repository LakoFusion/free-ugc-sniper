"""
src/xcsrf.py
============
Lay va tu dong refresh xCSRF token cho Roblox.
"""

import asyncio
import logging
import random
import time
from collections import Counter
from datetime import datetime, timedelta
from typing import Optional

import aiohttp

from src.roblox_common import (
    ProxyRequiredError,
    TCP_CONNECTOR,
    is_proxy_auth_error,
    proxy_cache_key,
    proxy_candidates,
)

logger = logging.getLogger(__name__)

_LOGOUT_URL = "https://auth.roblox.com/v2/logout"
_TOKEN_TTL_MINUTES = 10
_TOKEN_REFRESH_AFTER_MINUTES = 8.5
_XCSRF_SUMMARY_LOG_INTERVAL_SECONDS = 60
_XCSRF_AUTH_COOLDOWN_SECONDS = 20 * 60
_XCSRF_BASE_TRANSIENT_COOLDOWN_SECONDS = 30
_XCSRF_MAX_TRANSIENT_COOLDOWN_SECONDS = 5 * 60

_TRANSIENT_ERRORS = (
    aiohttp.ClientConnectorError,
    aiohttp.ClientProxyConnectionError,
    aiohttp.ServerDisconnectedError,
    aiohttp.ClientOSError,
    asyncio.TimeoutError,
    ConnectionError,
    OSError,
)

_MAX_XCSRF_RETRIES = 3

_xcsrf_proxy_cooldowns: dict[str, float] = {}
_xcsrf_proxy_failures: dict[str, int] = {}
_last_xcsrf_summary_log = 0.0


def _log_xcsrf_summary(message: str, level: int = logging.WARNING):
    global _last_xcsrf_summary_log
    now = time.monotonic()
    if now - _last_xcsrf_summary_log < _XCSRF_SUMMARY_LOG_INTERVAL_SECONDS:
        return
    _last_xcsrf_summary_log = now
    logger.log(level, f"[xCSRF] {message}")


def _failure_kind(exc: Exception) -> str:
    if is_proxy_auth_error(exc):
        return "auth"
    if isinstance(exc, asyncio.TimeoutError):
        return "timeout"
    if isinstance(
        exc,
        (
            aiohttp.ClientConnectorError,
            aiohttp.ClientProxyConnectionError,
            aiohttp.ServerDisconnectedError,
            aiohttp.ClientOSError,
            ConnectionError,
            OSError,
        ),
    ):
        return "connect"
    return type(exc).__name__


def _cooldown_for_failure(kind: str, count: int) -> float:
    if kind == "auth":
        return float(_XCSRF_AUTH_COOLDOWN_SECONDS)
    level = min(max(count, 1), 4)
    base = _XCSRF_BASE_TRANSIENT_COOLDOWN_SECONDS * (2 ** (level - 1))
    return min(base, _XCSRF_MAX_TRANSIENT_COOLDOWN_SECONDS) + random.uniform(0, 2)


def _is_xcsrf_proxy_cooled(candidate) -> bool:
    key = proxy_cache_key(candidate)
    if not key:
        return False
    until = _xcsrf_proxy_cooldowns.get(key, 0)
    if until <= 0:
        return False
    if time.monotonic() >= until:
        _xcsrf_proxy_cooldowns.pop(key, None)
        return False
    return True


def _record_xcsrf_proxy_success(candidate):
    key = proxy_cache_key(candidate)
    if not key:
        return
    _xcsrf_proxy_cooldowns.pop(key, None)
    _xcsrf_proxy_failures.pop(key, None)


def _record_xcsrf_proxy_failure(candidate, exc: Exception, counts: Counter):
    kind = _failure_kind(exc)
    counts[kind] += 1
    key = proxy_cache_key(candidate)
    if not key:
        return
    failures = _xcsrf_proxy_failures.get(key, 0) + 1
    _xcsrf_proxy_failures[key] = failures
    _xcsrf_proxy_cooldowns[key] = time.monotonic() + _cooldown_for_failure(kind, failures)


def _xcsrf_proxy_candidates(proxies: list) -> tuple[list, Counter]:
    counts = Counter()
    candidates = []
    seen = set()
    for proxy in proxies:
        try:
            proxy_attempts = proxy_candidates(proxy)
        except ValueError:
            counts["invalid"] += 1
            continue
        for candidate in proxy_attempts:
            key = proxy_cache_key(candidate)
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            if _is_xcsrf_proxy_cooled(candidate):
                counts["cooled"] += 1
                continue
            candidates.append(candidate)
    return candidates, counts


def _format_failure_counts(counts: Counter) -> str:
    if not counts:
        return "no attempts"
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))


async def _request_xcsrf_token(cookie: str, proxy=None, session=None) -> dict:
    proxy_url = proxy.url if proxy else None
    proxy_auth = proxy.auth if proxy else None
    owns_session = session is None
    if session is None:
        session = aiohttp.ClientSession(connector=TCP_CONNECTOR())
    try:
        async with session.post(
            _LOGOUT_URL,
            cookies={".ROBLOSECURITY": cookie},
            proxy=proxy_url,
            proxy_auth=proxy_auth,
            ssl=False,
            timeout=aiohttp.ClientTimeout(total=5, connect=3),
        ) as response:
            xcsrf = response.headers.get("x-csrf-token")
            if xcsrf is None:
                raise Exception(
                    "Khong lay duoc x-csrf-token. Cookie co the da het han hoac sai."
                )
            return {
                "xcsrf_token": xcsrf,
                "created": datetime.now(),
                "proxy_used": proxy.masked if proxy else None,
                "proxy_key": proxy_cache_key(proxy) if proxy else None,
            }
    finally:
        if owns_session:
            await session.close()


async def get_xcsrf_token(
    cookie: str,
    proxies: Optional[list] = None,
    fallback_direct: bool = True,
    session: Optional[aiohttp.ClientSession] = None,
) -> dict:
    """
    Lay xCSRF token moi tu Roblox bang cach POST len /v2/logout.
    Neu co proxies, thu tung proxy truoc; neu that bai va fallback_direct=True
    thi lay token truc tiep de app van khoi dong duoc.
    """
    proxy_list = [p for p in (proxies or []) if p]
    last_exc = None
    failure_counts = Counter()

    if not proxy_list and not fallback_direct:
        _log_xcsrf_summary(
            "proxy required but no proxy is available",
            logging.ERROR,
        )
        raise ProxyRequiredError("xCSRF proxy required but no proxy is available")

    candidates, skipped_counts = _xcsrf_proxy_candidates(proxy_list)
    failure_counts.update(skipped_counts)

    for candidate in candidates:
        try:
            token_data = await _request_xcsrf_token(cookie, candidate, session=session)
            _record_xcsrf_proxy_success(candidate)
            return token_data
        except Exception as e:
            last_exc = e
            _record_xcsrf_proxy_failure(candidate, e, failure_counts)
            continue

    if proxy_list and not fallback_direct:
        _log_xcsrf_summary(
            "proxy required but all attempts failed "
            f"({_format_failure_counts(failure_counts)})",
            logging.ERROR,
        )
        raise ProxyRequiredError(
            "xCSRF proxy required but all proxies failed",
            last_exc,
        )

    if proxy_list:
        _log_xcsrf_summary(
            "proxy attempts failed, fallback direct "
            f"({_format_failure_counts(failure_counts)})"
        )

    for attempt in range(1, _MAX_XCSRF_RETRIES + 1):
        try:
            return await _request_xcsrf_token(cookie, session=session)
        except _TRANSIENT_ERRORS as e:
            last_exc = e
            if attempt < _MAX_XCSRF_RETRIES:
                logger.warning(
                    f"[xCSRF] {type(e).__name__} "
                    f"(retry {attempt}/{_MAX_XCSRF_RETRIES})"
                )
                continue
            logger.error(
                f"[xCSRF] Loi get_xcsrf_token sau {_MAX_XCSRF_RETRIES} lan: "
                f"{type(e).__name__}: {e}"
            )
            raise
        except Exception as e:
            logger.error(f"[xCSRF] Loi get_xcsrf_token: {type(e).__name__}: {e}")
            raise
    raise last_exc


def is_token_expired(created: datetime) -> bool:
    """Kiem tra token da het TTL chua."""
    return datetime.now() > created + timedelta(minutes=_TOKEN_TTL_MINUTES)


def is_token_refresh_due(created: datetime) -> bool:
    """Refresh som hon TTL de co thoi gian retry neu proxy chap chon."""
    return datetime.now() > created + timedelta(minutes=_TOKEN_REFRESH_AFTER_MINUTES)


async def refresh_token_if_needed(
    cookie_info: dict,
    proxies: Optional[list] = None,
    fallback_direct: bool = True,
    session: Optional[aiohttp.ClientSession] = None,
) -> Optional[dict]:
    """Refresh xCSRF token neu gan het han. Giu token cu neu van con han."""
    old_token = cookie_info.get("xcsrf_token")
    old_created = cookie_info.get("created")
    needs_refresh = (
        old_token is None
        or old_created is None
        or is_token_refresh_due(old_created)
    )
    if not needs_refresh:
        return None

    try:
        token_data = await get_xcsrf_token(
            cookie_info["cookie"],
            proxies=proxies,
            fallback_direct=fallback_direct,
            session=session,
        )
    except ProxyRequiredError:
        if old_token is not None and old_created is not None and not is_token_expired(old_created):
            _log_xcsrf_summary(
                "proxy refresh failed; using cached token until it expires"
            )
            return None
        raise

    cookie_info["xcsrf_token"] = token_data["xcsrf_token"]
    cookie_info["created"] = token_data["created"]
    cookie_info["xcsrf_proxy_used"] = token_data.get("proxy_used")
    cookie_info["xcsrf_proxy_key"] = token_data.get("proxy_key")
    return token_data


async def auto_refresh_loop(
    account: dict,
    log_fn=None,
    proxies: Optional[list] = None,
    fallback_direct: bool = True,
    error_fn=None,
    session: Optional[aiohttp.ClientSession] = None,
) -> None:
    """Vong lap refresh token dinh ky."""
    while True:
        await asyncio.sleep(10)
        try:
            search_info = {
                "cookie": account["search_cookie"],
                "xcsrf_token": account.get("search_xcsrf_token"),
                "created": account.get("search_created"),
                "xcsrf_proxy_used": account.get("search_xcsrf_proxy_used"),
                "xcsrf_proxy_key": account.get("search_xcsrf_proxy_key"),
                "user_name": "search_account",
            }
            token_data = await refresh_token_if_needed(
                search_info,
                proxies=proxies,
                fallback_direct=fallback_direct,
                session=session,
            )
            if token_data is not None:
                account["search_xcsrf_token"] = search_info["xcsrf_token"]
                account["search_created"] = search_info["created"]
                account["search_xcsrf_proxy_used"] = search_info.get("xcsrf_proxy_used")
                account["search_xcsrf_proxy_key"] = search_info.get("xcsrf_proxy_key")

            for cookie_info in account.get("buy_cookies", []):
                await refresh_token_if_needed(
                    cookie_info,
                    proxies=proxies,
                    fallback_direct=fallback_direct,
                    session=session,
                )

        except ProxyRequiredError as e:
            if error_fn:
                await error_fn("auto xCSRF refresh", str(e))
            else:
                logger.error(f"[xCSRF] auto_refresh_loop: {e}")
        except _TRANSIENT_ERRORS as e:
            logger.warning(
                f"[xCSRF] auto_refresh_loop: {type(e).__name__} - se thu lai lan sau"
            )
        except Exception as e:
            logger.error(f"[xCSRF] Loi auto_refresh_loop: {type(e).__name__}: {e}")
