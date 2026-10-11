"""
src/roblox_buy.py
=================
Roblox purchase helper with proxy retry support.
"""

import asyncio
import logging
import time
import uuid
from typing import Optional

import aiohttp

from src.roblox_common import BUY_URL, is_proxy_auth_error, mask_proxy, proxy_cache_key, proxy_candidates

logger = logging.getLogger(__name__)


def _result(
    *,
    success: bool = False,
    purchased: bool = False,
    error: Optional[str] = None,
    status: int = 0,
    elapsed: float = 0,
    proxy_used: Optional[str] = None,
    proxy_key: Optional[str] = None,
    proxy_line_key: Optional[str] = None,
    proxy_candidate_keys: Optional[list] = None,
    proxy_failed: bool = False,
    proxy_error_type: Optional[str] = None,
) -> dict:
    return {
        "success": success,
        "purchased": purchased,
        "error": error,
        "status": status,
        "response_time_ms": elapsed,
        "proxy_used": proxy_used,
        "proxy_key": proxy_key,
        "proxy_line_key": proxy_line_key,
        "proxy_candidate_keys": proxy_candidate_keys or [],
        "proxy_failed": proxy_failed,
        "proxy_error_type": proxy_error_type,
    }


def _error_from_data(data, status: int) -> str:
    if isinstance(data, dict):
        for key in ("errorMessage", "error", "message"):
            if data.get(key):
                return str(data[key])
        errors = data.get("errors")
        if isinstance(errors, list) and errors:
            first = errors[0]
            if isinstance(first, dict):
                return str(first.get("message") or first.get("code") or f"HTTP {status}")
    return f"HTTP {status}"


def _is_xcsrf_error(data) -> bool:
    text = str(data).lower()
    return (
        "token validation failed" in text
        or "xsrf" in text
        or "csrf" in text
    )


async def _read_error(resp: aiohttp.ClientResponse, status: int) -> str:
    try:
        data = await resp.json()
    except Exception:
        data = None
    return _error_from_data(data, status)


async def buy_item(
    session: aiohttp.ClientSession,
    info: dict,
    cookie_info: dict,
    proxies: Optional[list] = None,
    quiet: bool = False,
) -> dict:
    """
    Buy one Roblox item.

    If proxies are supplied, each proxy is tried in order. For ambiguous
    host:port:user:pass proxies, a swapped user/pass candidate is tried only
    when the proxy itself returns 407 Proxy Authentication Required.
    """
    if not isinstance(cookie_info.get("xcsrf_token"), str):
        return _result(error="Thieu xcsrf_token")

    collectible_id = info["collectibleItemId"]
    url = BUY_URL.format(collectible_id=collectible_id)
    item_id = info.get("item_id", "?")

    payload = {
        "collectibleItemId": collectible_id,
        "expectedCurrency": 1,
        "expectedPrice": info.get("price", 0),
        "expectedPurchaserId": cookie_info["user_id"],
        "expectedPurchaserType": "User",
        "expectedSellerId": info["creator"],
        "expectedSellerType": "User",
        "idempotencyKey": str(uuid.uuid4()),
        "collectibleProductId": info["productid_data"],
    }

    use_proxies = bool(proxies and any(p is not None for p in proxies))
    proxy_list = proxies if use_proxies else [None]
    last_result = None

    for idx, proxy in enumerate(proxy_list):
        proxy_line_key = None
        proxy_candidate_keys = []
        if proxy is None:
            candidates = [None]
        else:
            try:
                candidates = proxy_candidates(proxy)
                proxy_candidate_keys = [
                    key for candidate in candidates
                    if (key := proxy_cache_key(candidate))
                ]
                proxy_line_key = "||".join(proxy_candidate_keys) if proxy_candidate_keys else None
            except ValueError as e:
                if not quiet:
                    logger.warning(
                        f"[BuyItem] Proxy [{idx+1}/{len(proxy_list)}] {mask_proxy(proxy)} "
                        f"format sai: {e}, thu proxy tiep..."
                    )
                last_result = _result(
                    error=f"Invalid proxy: {e}",
                    proxy_used=mask_proxy(proxy),
                    proxy_line_key=proxy_line_key,
                    proxy_candidate_keys=proxy_candidate_keys,
                    proxy_failed=True,
                    proxy_error_type="invalid",
                )
                continue

        for cidx, candidate in enumerate(candidates):
            start = time.perf_counter()
            proxy_url = candidate.url if candidate else None
            proxy_auth = candidate.auth if candidate else None
            proxy_display = candidate.masked if candidate else None
            proxy_key = proxy_cache_key(candidate) if candidate else None

            try:
                async with session.post(
                    url,
                    json=payload,
                    headers={"x-csrf-token": cookie_info["xcsrf_token"]},
                    cookies={".ROBLOSECURITY": cookie_info["cookie"]},
                    proxy=proxy_url,
                    proxy_auth=proxy_auth,
                    ssl=False,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    elapsed = (time.perf_counter() - start) * 1000
                    status = resp.status

                    if status == 200:
                        try:
                            data = await resp.json()
                        except Exception as e:
                            return _result(
                                error=f"InvalidResponse: {type(e).__name__}",
                                status=status,
                                elapsed=elapsed,
                                proxy_used=proxy_display,
                                proxy_key=proxy_key,
                                proxy_line_key=proxy_line_key,
                                proxy_candidate_keys=proxy_candidate_keys,
                            )
                        purchased = data.get("purchased", False)
                        error = data.get("errorMessage") if not purchased else None
                        if not quiet:
                            if use_proxies:
                                logger.info(
                                    f"[BuyItem] {'Purchased' if purchased else 'Not purchased'} "
                                    f"item={item_id} proxy={proxy_display} time={elapsed:.0f}ms"
                                )
                            else:
                                logger.info(
                                    f"[BuyItem] {'Purchased' if purchased else 'Not purchased'} "
                                    f"item={item_id} time={elapsed:.0f}ms"
                                )
                        return _result(
                            success=True,
                            purchased=purchased,
                            error=error,
                            status=status,
                            elapsed=elapsed,
                            proxy_used=proxy_display,
                            proxy_key=proxy_key,
                            proxy_line_key=proxy_line_key,
                            proxy_candidate_keys=proxy_candidate_keys,
                        )

                    if status == 403:
                        try:
                            data = await resp.json()
                        except Exception:
                            data = None
                        if _is_xcsrf_error(data) or "x-csrf-token" in resp.headers:
                            logger.warning("[BuyItem] xCSRF token het han, can refresh")
                            return _result(
                                error="xcsrf_expired",
                                status=403,
                                elapsed=elapsed,
                                proxy_used=proxy_display,
                                proxy_key=proxy_key,
                                proxy_line_key=proxy_line_key,
                                proxy_candidate_keys=proxy_candidate_keys,
                            )
                        error = _error_from_data(data, status)
                    else:
                        error = await _read_error(resp, status)
                    if not quiet:
                        if use_proxies:
                            logger.warning(
                                f"[BuyItem] Proxy [{idx+1}/{len(proxy_list)}] {proxy_display} "
                                f"-> HTTP {status} ({elapsed:.0f}ms), dung proxy rotation"
                            )
                        else:
                            logger.warning(
                                f"[BuyItem] HTTP {status} ({elapsed:.0f}ms) item={item_id}"
                            )
                    return _result(
                        error=error,
                        status=status,
                        elapsed=elapsed,
                        proxy_used=proxy_display,
                        proxy_key=proxy_key,
                        proxy_line_key=proxy_line_key,
                        proxy_candidate_keys=proxy_candidate_keys,
                    )

            except aiohttp.ClientHttpProxyError as e:
                elapsed = (time.perf_counter() - start) * 1000
                last_result = _result(
                    error="ProxyAuthRejected" if is_proxy_auth_error(e) else str(e),
                    status=getattr(e, "status", 0) or 0,
                    elapsed=elapsed,
                    proxy_used=proxy_display,
                    proxy_key=proxy_key,
                    proxy_line_key=proxy_line_key,
                    proxy_candidate_keys=proxy_candidate_keys,
                    proxy_failed=True,
                    proxy_error_type="auth" if is_proxy_auth_error(e) else "connect",
                )
                if is_proxy_auth_error(e) and cidx + 1 < len(candidates):
                    if not quiet:
                        logger.warning(
                            f"[BuyItem] Proxy auth rejected {proxy_display}; "
                            "thu auth candidate tiep theo..."
                        )
                    continue
                if not quiet:
                    logger.warning(
                        f"[BuyItem] Proxy [{idx+1}/{len(proxy_list)}] {proxy_display} "
                        f"proxy HTTP error: {e}, thu proxy tiep..."
                    )
                break

            except asyncio.TimeoutError:
                elapsed = (time.perf_counter() - start) * 1000
                if not quiet:
                    if use_proxies:
                        logger.warning(
                            f"[BuyItem] Proxy [{idx+1}/{len(proxy_list)}] {proxy_display} "
                            f"timeout ({elapsed:.0f}ms), thu proxy tiep..."
                        )
                    else:
                        logger.warning(f"[BuyItem] Timeout ({elapsed:.0f}ms) item={item_id}")
                last_result = _result(
                    error="Timeout",
                    elapsed=elapsed,
                    proxy_used=proxy_display,
                    proxy_key=proxy_key,
                    proxy_line_key=proxy_line_key,
                    proxy_candidate_keys=proxy_candidate_keys,
                    proxy_failed=True,
                    proxy_error_type="timeout",
                )
                break

            except (
                aiohttp.ClientProxyConnectionError,
                aiohttp.ClientConnectorError,
                aiohttp.ServerDisconnectedError,
                aiohttp.ClientOSError,
            ) as e:
                elapsed = (time.perf_counter() - start) * 1000
                if not quiet:
                    logger.warning(
                        f"[BuyItem] Proxy [{idx+1}/{len(proxy_list)}] {proxy_display} "
                        f"khong ket noi duoc: {e}, thu proxy tiep..."
                    )
                last_result = _result(
                    error=f"ProxyConnectionError: {e}",
                    elapsed=elapsed,
                    proxy_used=proxy_display,
                    proxy_key=proxy_key,
                    proxy_line_key=proxy_line_key,
                    proxy_candidate_keys=proxy_candidate_keys,
                    proxy_failed=True,
                    proxy_error_type="connect",
                )
                break

            except Exception as e:
                elapsed = (time.perf_counter() - start) * 1000
                if not quiet:
                    if use_proxies:
                        logger.warning(
                            f"[BuyItem] Proxy [{idx+1}/{len(proxy_list)}] {proxy_display} "
                            f"loi: {e}, thu proxy tiep..."
                        )
                    else:
                        logger.warning(f"[BuyItem] Loi: {e} ({elapsed:.0f}ms) item={item_id}")
                last_result = _result(
                    error=str(e),
                    elapsed=elapsed,
                    proxy_used=proxy_display,
                    proxy_key=proxy_key,
                    proxy_line_key=proxy_line_key,
                    proxy_candidate_keys=proxy_candidate_keys,
                    proxy_failed=use_proxies,
                    proxy_error_type="other" if use_proxies else None,
                )
                break

    if not quiet:
        if use_proxies:
            logger.error(f"[BuyItem] Da thu het {len(proxy_list)} proxy, item={item_id} that bai.")
        else:
            logger.error(f"[BuyItem] Buy that bai item={item_id}")

    return last_result or _result(
        error="Het proxy" if use_proxies else "Buy failed",
    )
