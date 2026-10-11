"""
src/roblox_fetch.py
===================
Tất cả hàm fetch dữ liệu từ Roblox API:
  - get_user_info           → lấy thông tin user
  - fetch_asset_details     → chi tiết item (Economy API)
  - get_product_id          → lấy collectibleProductId
  - fetch_product_ids_batch → lấy batch collectibleProductId
  - fetch_details_and_product_id → fetch V2 Express (details + productId)
  - fetch_catalog_batch     → batch fetch qua Catalog API
  - get_serial              → lấy serial number sau mua
  - get_image_url           → lấy thumbnail URL
"""

import asyncio
import logging
import time
from typing import Any, Optional

import aiohttp

from src.roblox_common import (
    ProxyRequiredError,
    USER_INFO_URL,
    ASSET_DETAILS_URL,
    CATALOG_DETAILS,
    MARKETPLACE_ITEMS,
    INVENTORY_URL,
    THUMBNAIL_URL,
    TCP_CONNECTOR,
    is_proxy_auth_error,
    mask_proxy,
    proxy_cache_key,
    proxy_candidates,
)

logger = logging.getLogger(__name__)

_FETCH_LOG_INTERVAL_SECONDS = 60
_last_fetch_failure_log: dict[tuple[int, int], float] = {}


def _log_fetch_failure_once(item_id: int, status: int):
    key = (int(item_id), int(status))
    now = time.monotonic()
    last = _last_fetch_failure_log.get(key, 0)
    if now - last < _FETCH_LOG_INTERVAL_SECONDS:
        return
    _last_fetch_failure_log[key] = now
    logger.warning(f"[FetchDetails] HTTP {status} item={item_id}")

_PROXY_TRANSPORT_ERRORS = (
    aiohttp.ClientProxyConnectionError,
    aiohttp.ClientConnectorError,
    aiohttp.ServerDisconnectedError,
    aiohttp.ClientOSError,
    asyncio.TimeoutError,
    ConnectionError,
    OSError,
)


def _fetch_meta(source: str, **values) -> dict:
    return {"source": source, **values}


def _describe_fetch_step(meta: Optional[dict]) -> str:
    if not meta:
        return "unknown"
    source = meta.get("source") or "fetch"
    status = meta.get("status")
    error = meta.get("error")
    if status is not None:
        if error and error != "http":
            return f"{source} HTTP {status} {error}"
        return f"{source} HTTP {status}"
    if error:
        return f"{source} {error}"
    return source


def format_fetch_failure(meta: Optional[dict]) -> str:
    if not meta:
        return "unknown"
    if "primary" not in meta and "fallback" not in meta:
        return _describe_fetch_step(meta)

    parts = []
    for key in ("primary", "fallback", "marketplace"):
        step = meta.get(key)
        if step:
            parts.append(_describe_fetch_step(step))
    return "; ".join(parts) if parts else "unknown"


def _catalog_is_for_sale(item: dict) -> bool:
    price_status = item.get("priceStatus")
    if price_status == "Off Sale":
        return False
    if item.get("collectibleItemId") or item.get("price") is not None:
        return True
    return False


def _normalize_catalog_item(item: dict) -> dict:
    creator_id = item.get("creatorTargetId")
    return {
        "Id": item.get("id"),
        "Name": item.get("name", "Unknown"),
        "IsForSale": _catalog_is_for_sale(item),
        "Remaining": item.get("unitsAvailableForConsumption"),
        "PriceInRobux": item.get("price", 0) or 0,
        "SaleLocation": item.get("saleLocationType") or "",
        "CollectibleItemId": item.get("collectibleItemId"),
        "CollectibleProductId": item.get("collectibleProductId"),
        "Creator": {"CreatorTargetId": creator_id} if creator_id is not None else {},
        "AssetTypeId": item.get("assetType"),
    }


async def _post_xcsrf_json(
    session: aiohttp.ClientSession,
    url: str,
    *,
    json_payload: dict,
    headers: dict,
    cookie: str,
    proxies: Optional[list] = None,
    proxy_required: bool = False,
    api_name: str = "Roblox API",
    timeout: Optional[aiohttp.ClientTimeout] = None,
    on_proxy_error=None,
) -> Optional[dict]:
    """
    on_proxy_error(proxy_key, error_type) được gọi khi proxy lỗi transport
    (timeout/connect/auth) để caller ghi cooldown ngay tại fetch-layer.
    """
    proxy_list = [p for p in (proxies or []) if p]
    if proxy_required and not proxy_list:
        raise ProxyRequiredError(f"{api_name}: proxy required but no proxy is available")

    request_proxies = proxy_list if proxy_list else [None]
    last_exc = None

    def _notify_proxy_error(candidate, error_type: str):
        if on_proxy_error is None or candidate is None:
            return
        try:
            on_proxy_error(proxy_cache_key(candidate), error_type)
        except Exception:
            pass

    for idx, proxy in enumerate(request_proxies):
        if proxy is None:
            candidates = [None]
        else:
            try:
                candidates = proxy_candidates(proxy)
            except ValueError as e:
                last_exc = e
                logger.warning(f"[{api_name}] Proxy {mask_proxy(proxy)} format sai: {e}")
                continue

        for cidx, candidate in enumerate(candidates):
            proxy_url = candidate.url if candidate else None
            proxy_auth = candidate.auth if candidate else None
            proxy_display = candidate.masked if candidate else "direct"
            try:
                async with session.post(
                    url,
                    json=json_payload,
                    headers=headers,
                    cookies={".ROBLOSECURITY": cookie},
                    proxy=proxy_url,
                    proxy_auth=proxy_auth,
                    ssl=False,
                    timeout=timeout or aiohttp.ClientTimeout(total=5, connect=3),
                ) as resp:
                    data = None
                    try:
                        data = await resp.json()
                    except Exception:
                        pass
                    return {
                        "status": resp.status,
                        "data": data,
                        "headers": dict(resp.headers),
                        "proxy_used": proxy_display if candidate else None,
                    }
            except aiohttp.ClientHttpProxyError as e:
                last_exc = e
                if is_proxy_auth_error(e):
                    _notify_proxy_error(candidate, "auth")
                    if cidx + 1 < len(candidates):
                        logger.warning(
                            f"[{api_name}] Proxy auth rejected {proxy_display}; "
                            "thu auth candidate tiep theo..."
                        )
                        continue
                logger.warning(
                    f"[{api_name}] Proxy {proxy_display} HTTP error: {e}"
                )
                break
            except _PROXY_TRANSPORT_ERRORS as e:
                last_exc = e
                _notify_proxy_error(
                    candidate,
                    "timeout" if isinstance(e, asyncio.TimeoutError) else "connect",
                )
                logger.warning(
                    f"[{api_name}] Proxy {proxy_display} loi ket noi: "
                    f"{type(e).__name__}: {e}"
                )
                break
            except Exception as e:
                last_exc = e
                if proxy_list:
                    _notify_proxy_error(candidate, "other")
                    logger.warning(
                        f"[{api_name}] Proxy {proxy_display} loi: "
                        f"{type(e).__name__}: {e}"
                    )
                    break
                logger.error(f"[{api_name}] Loi: {type(e).__name__}: {e}")
                return None

    if proxy_required:
        raise ProxyRequiredError(f"{api_name}: all proxy attempts failed", last_exc)
    return None


# ─── User Info ────────────────────────────────────────────────────────────────

async def get_user_info(
    cookie: str,
    session: Optional[aiohttp.ClientSession] = None,
) -> dict:
    owns_session = session is None
    if session is None:
        session = aiohttp.ClientSession(connector=TCP_CONNECTOR())
    try:
        async with session.get(
            USER_INFO_URL,
            cookies={".ROBLOSECURITY": cookie},
            ssl=False,
        ) as response:
            data = await response.json()
            if data.get("id") is None:
                raise Exception(f"Không lấy được user info: {data}")
            return {"id": data["id"], "name": data["name"]}
    finally:
        if owns_session:
            await session.close()


# ─── Asset Details ────────────────────────────────────────────────────────────

async def fetch_asset_details(
    session: aiohttp.ClientSession,
    item_id: int,
    search_cookie: str,
    max_retries: int = 3,
    allow_429_retry: bool = False,
) -> Optional[dict]:
    item, _ = await fetch_asset_details_with_meta(
        session,
        item_id,
        search_cookie,
        max_retries=max_retries,
        allow_429_retry=allow_429_retry,
    )
    return item


# ─── Asset Details Meta ─────────────────────────────────────────────────────

async def fetch_asset_details_with_meta(
    session: aiohttp.ClientSession,
    item_id: int,
    search_cookie: str,
    max_retries: int = 3,
    allow_429_retry: bool = False,
) -> tuple[Optional[dict], dict]:
    url = ASSET_DETAILS_URL.format(item_id=item_id)
    last_meta = _fetch_meta("economy", error="not_attempted")
    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(
                url,
                headers={"Accept-Encoding": "gzip, deflate", "Connection": "keep-alive"},
                cookies={".ROBLOSECURITY": search_cookie},
                ssl=False,
            ) as resp:
                if resp.status == 200:
                    try:
                        return await resp.json(), _fetch_meta(
                            "economy", status=resp.status, attempt=attempt
                        )
                    except Exception as e:
                        last_meta = _fetch_meta(
                            "economy",
                            status=resp.status,
                            error=f"json_error:{type(e).__name__}",
                            attempt=attempt,
                        )
                        return None, last_meta

                last_meta = _fetch_meta(
                    "economy", status=resp.status, error="http", attempt=attempt
                )
                if resp.status == 429:
                    if allow_429_retry and attempt < max_retries:
                        logger.warning(
                            f"[Express Fetch] 429 item={item_id}, retry {attempt}/{max_retries}..."
                        )
                        await asyncio.sleep(0.5)
                        continue
                    _log_fetch_failure_once(item_id, resp.status)
                    return None, last_meta

                if attempt < max_retries:
                    logger.warning(
                        f"[FetchDetails] HTTP {resp.status} item={item_id}, retry {attempt}/{max_retries}..."
                    )
                    await asyncio.sleep(0.3)
                    continue
                _log_fetch_failure_once(item_id, resp.status)
                return None, last_meta
        except Exception as e:
            error_type = "timeout" if isinstance(e, asyncio.TimeoutError) else type(e).__name__
            last_meta = _fetch_meta("economy", error=error_type, attempt=attempt)
            if attempt < max_retries:
                logger.warning(
                    f"[FetchDetails] Error item={item_id}: {type(e).__name__}: {e}, "
                    f"retry {attempt}/{max_retries}..."
                )
                await asyncio.sleep(0.3)
                continue
            logger.error(
                f"[FetchDetails] Error item={item_id} after {max_retries} attempts: "
                f"{type(e).__name__}: {e}"
            )
            return None, last_meta
    return None, last_meta


# ─── Product ID ──────────────────────────────────────────────────────────────

async def get_product_id(
    session: aiohttp.ClientSession,
    collectible_item_id: str,
    search_xcsrf: str,
    search_cookie: str,
    proxies: Optional[list] = None,
    proxy_required: bool = False,
) -> Optional[str]:
    try:
        result = await _post_xcsrf_json(
            session,
            MARKETPLACE_ITEMS,
            json_payload={"itemIds": [collectible_item_id]},
            headers={"x-csrf-token": search_xcsrf},
            cookie=search_cookie,
            proxies=proxies,
            proxy_required=proxy_required,
            api_name="MarketplaceItems",
        )
        if result and result["status"] == 200 and result["data"]:
            return result["data"][0].get("collectibleProductId")
    except ProxyRequiredError:
        raise
    except Exception as e:
        logger.error(f"Lỗi get_product_id: {type(e).__name__}: {e}")
    return None


async def fetch_product_ids_batch(
    session: aiohttp.ClientSession,
    collectible_item_ids: list,
    search_xcsrf: str,
    search_cookie: str,
    proxies: Optional[list] = None,
    proxy_required: bool = False,
) -> dict:
    """
    Fetch collectibleProductId cho nhiều items cùng lúc qua marketplace-items API.
    Returns: {collectibleItemId: collectibleProductId}
    """
    if not collectible_item_ids:
        return {}
    try:
        result = await _post_xcsrf_json(
            session,
            MARKETPLACE_ITEMS,
            json_payload={"itemIds": collectible_item_ids},
            headers={"x-csrf-token": search_xcsrf},
            cookie=search_cookie,
            proxies=proxies,
            proxy_required=proxy_required,
            api_name="MarketplaceItemsBatch",
        )
        if result and result["status"] == 200 and isinstance(result["data"], list):
            return {
                entry["collectibleItemId"]: entry.get("collectibleProductId")
                for entry in result["data"]
                if entry.get("collectibleItemId")
            }
    except ProxyRequiredError:
        raise
    except Exception as e:
        logger.error(f"Lỗi fetch_product_ids_batch: {type(e).__name__}: {e}")
    return {}


# ─── Details + Product ID (V2 Express) ───────────────────────────────────────

async def _fetch_catalog_item_once(
    session: aiohttp.ClientSession,
    item_id: int,
    search_xcsrf: Optional[str],
    search_cookie: str,
    proxies: Optional[list] = None,
    proxy_required: bool = False,
) -> tuple[Optional[dict], dict]:
    if not search_xcsrf:
        return None, _fetch_meta("catalog", error="skipped_no_xcsrf")
    try:
        result = await _post_xcsrf_json(
            session,
            CATALOG_DETAILS,
            json_payload={"items": [{"itemType": "Asset", "id": item_id}]},
            headers={"x-csrf-token": search_xcsrf, "Accept-Encoding": "gzip, deflate"},
            cookie=search_cookie,
            proxies=proxies,
            proxy_required=proxy_required,
            api_name="CatalogExpress",
            timeout=aiohttp.ClientTimeout(total=3, connect=2),
        )
    except ProxyRequiredError as e:
        return None, _fetch_meta("catalog", error="proxy_required", detail=str(e))
    except Exception as e:
        error_type = "timeout" if isinstance(e, asyncio.TimeoutError) else type(e).__name__
        return None, _fetch_meta("catalog", error=error_type)

    if result is None:
        return None, _fetch_meta("catalog", error="transport")

    status = result["status"]
    data = result["data"]
    if status != 200:
        if status == 403 and (
            "Token Validation Failed" in str(data)
            or "x-csrf-token" in result.get("headers", {})
        ):
            return None, _fetch_meta("catalog", status=status, error="token_expired")
        return None, _fetch_meta("catalog", status=status, error="http")

    items: list[Any] = []
    if isinstance(data, dict):
        raw_items = data.get("data")
        if isinstance(raw_items, list):
            items = raw_items
    elif isinstance(data, list):
        items = data

    if not items:
        return None, _fetch_meta("catalog", status=status, error="empty")

    return _normalize_catalog_item(items[0]), _fetch_meta("catalog", status=status)


async def _fetch_product_id_once(
    session: aiohttp.ClientSession,
    collectible_item_id: str,
    search_xcsrf: Optional[str],
    search_cookie: str,
    proxies: Optional[list] = None,
    proxy_required: bool = False,
) -> tuple[Optional[str], dict]:
    if not search_xcsrf:
        return None, _fetch_meta("marketplace", error="skipped_no_xcsrf")
    try:
        result = await _post_xcsrf_json(
            session,
            MARKETPLACE_ITEMS,
            json_payload={"itemIds": [collectible_item_id]},
            headers={"x-csrf-token": search_xcsrf},
            cookie=search_cookie,
            proxies=proxies,
            proxy_required=proxy_required,
            api_name="MarketplaceItemsExpress",
            timeout=aiohttp.ClientTimeout(total=3, connect=2),
        )
    except ProxyRequiredError as e:
        return None, _fetch_meta("marketplace", error="proxy_required", detail=str(e))
    except Exception as e:
        error_type = "timeout" if isinstance(e, asyncio.TimeoutError) else type(e).__name__
        return None, _fetch_meta("marketplace", error=error_type)

    if result is None:
        return None, _fetch_meta("marketplace", error="transport")

    status = result["status"]
    data = result["data"]
    if status != 200:
        return None, _fetch_meta("marketplace", status=status, error="http")
    if not isinstance(data, list) or not data:
        return None, _fetch_meta("marketplace", status=status, error="empty")
    return data[0].get("collectibleProductId"), _fetch_meta("marketplace", status=status)


async def fetch_details_and_product_id_with_meta(
    session: aiohttp.ClientSession,
    item_id: int,
    search_cookie: str,
    search_xcsrf: Optional[str] = None,
    fast: bool = False,
    proxies: Optional[list] = None,
    proxy_required: bool = False,
) -> tuple[Optional[dict], Optional[str], dict]:
    item, economy_meta = await fetch_asset_details_with_meta(
        session,
        item_id,
        search_cookie,
        max_retries=1 if fast else 2,
        allow_429_retry=False if fast else True,
    )
    meta = {"primary": economy_meta}
    if item is not None:
        return item, item.get("CollectibleProductId"), meta

    if economy_meta.get("status") == 429:
        return None, None, meta

    catalog_item, catalog_meta = await _fetch_catalog_item_once(
        session,
        item_id,
        search_xcsrf,
        search_cookie,
        proxies=proxies,
        proxy_required=proxy_required,
    )
    meta["fallback"] = catalog_meta
    if catalog_item is None:
        return None, None, meta

    product_id = catalog_item.get("CollectibleProductId")
    collectible_item_id = catalog_item.get("CollectibleItemId")
    if not product_id and collectible_item_id:
        product_id, marketplace_meta = await _fetch_product_id_once(
            session,
            collectible_item_id,
            search_xcsrf,
            search_cookie,
            proxies=proxies,
            proxy_required=proxy_required,
        )
        meta["marketplace"] = marketplace_meta
        if product_id:
            catalog_item["CollectibleProductId"] = product_id

    return catalog_item, product_id, meta


async def fetch_details_and_product_id(
    session: aiohttp.ClientSession,
    item_id: int,
    search_cookie: str,
    fast: bool = False,
) -> tuple[Optional[dict], Optional[str]]:
    item, product_id, _ = await fetch_details_and_product_id_with_meta(
        session,
        item_id,
        search_cookie,
        fast=fast,
    )
    return item, product_id


# ─── Catalog Batch ────────────────────────────────────────────────────────────

async def fetch_catalog_batch_with_meta(
    session: aiohttp.ClientSession,
    item_ids: list,
    search_xcsrf: str,
    search_cookie: str,
    max_retries: int = 2,
    proxies: Optional[list] = None,
    proxy_required: bool = False,
    on_proxy_error=None,
) -> tuple[Optional[dict], dict]:
    last_meta = _fetch_meta("catalog_batch", error="not_attempted")
    for attempt in range(1, max_retries + 1):
        try:
            result = await _post_xcsrf_json(
                session,
                CATALOG_DETAILS,
                json_payload={"items": [{"itemType": "Asset", "id": i} for i in item_ids]},
                headers={"x-csrf-token": search_xcsrf, "Accept-Encoding": "gzip, deflate"},
                cookie=search_cookie,
                proxies=proxies,
                proxy_required=proxy_required,
                api_name="CatalogBatch",
                timeout=aiohttp.ClientTimeout(total=3, connect=2),
                on_proxy_error=on_proxy_error,
            )
            if result is None:
                last_meta = _fetch_meta("catalog_batch", error="transport", attempt=attempt)
                if attempt < max_retries:
                    await asyncio.sleep(1)
                    continue
                return None, last_meta
            status = result["status"]
            data = result["data"]
            headers = result["headers"]
            if status == 200:
                return data, _fetch_meta("catalog_batch", status=status, attempt=attempt)
            if status == 403:
                if "Token Validation Failed" in str(data):
                    return (
                        {"token_expired": True},
                        _fetch_meta("catalog_batch", status=status, error="token_expired", attempt=attempt),
                    )
                if "x-csrf-token" in headers:
                    return (
                        {"token_expired": True},
                        _fetch_meta("catalog_batch", status=status, error="token_expired", attempt=attempt),
                    )
            last_meta = _fetch_meta("catalog_batch", status=status, error="http", attempt=attempt)
            if status == 429:
                if attempt < max_retries:
                    logger.warning(f"[CatalogBatch] 429, retry {attempt}/{max_retries}...")
                    await asyncio.sleep(3)
                    continue
                return None, last_meta
            if attempt < max_retries:
                logger.warning(f"[CatalogBatch] HTTP {status}, retry {attempt}/{max_retries}...")
                await asyncio.sleep(1)
                continue
            return None, last_meta
        except ProxyRequiredError:
            raise
        except Exception as e:
            error_type = "timeout" if isinstance(e, asyncio.TimeoutError) else type(e).__name__
            last_meta = _fetch_meta("catalog_batch", error=error_type, attempt=attempt)
            if attempt < max_retries:
                logger.warning(f"[CatalogBatch] Lỗi: {type(e).__name__}: {e}, retry {attempt}/{max_retries}...")
                await asyncio.sleep(1)
                continue
            logger.error(f"[CatalogBatch] Lỗi sau {max_retries} lần: {type(e).__name__}: {e}")
            return None, last_meta
    return None, last_meta


# Backwards-compatible Catalog Batch wrapper.

async def fetch_catalog_batch(
    session: aiohttp.ClientSession,
    item_ids: list,
    search_xcsrf: str,
    search_cookie: str,
    max_retries: int = 2,
    proxies: Optional[list] = None,
    proxy_required: bool = False,
) -> Optional[dict]:
    data, _ = await fetch_catalog_batch_with_meta(
        session,
        item_ids,
        search_xcsrf,
        search_cookie,
        max_retries=max_retries,
        proxies=proxies,
        proxy_required=proxy_required,
    )
    return data


# ─── Serial ───────────────────────────────────────────────────────────────────

async def get_serial(
    cookie_info: dict,
    asset_type: int,
    item_id: int,
    session: Optional[aiohttp.ClientSession] = None,
) -> str:
    base_url = INVENTORY_URL.format(user_id=cookie_info["user_id"], asset_type=asset_type)
    owns_session = session is None
    if session is None:
        session = aiohttp.ClientSession(connector=TCP_CONNECTOR())
    try:
        cursor = None
        for _page in range(3):  # tối đa 3 trang (30 item mới nhất)
            url = base_url if cursor is None else f"{base_url}&cursor={cursor}"
            async with session.get(
                url,
                cookies={".ROBLOSECURITY": cookie_info["cookie"]},
                ssl=False,
            ) as resp:
                data = await resp.json()
            for entry in data.get("data", []):
                if int(entry["assetId"]) == int(item_id):
                    return str(entry.get("serialNumber", "?"))
            cursor = data.get("nextPageCursor")
            if not cursor:
                break
    except Exception as e:
        logger.error(f"Lỗi get_serial item {item_id}: {type(e).__name__}: {e}")
    finally:
        if owns_session:
            await session.close()
    return "not found"


# ─── Thumbnail ────────────────────────────────────────────────────────────────

async def get_image_url(
    item_id: int,
    session: Optional[aiohttp.ClientSession] = None,
) -> Optional[str]:
    url = THUMBNAIL_URL.format(item_id=item_id)
    owns_session = session is None
    if session is None:
        session = aiohttp.ClientSession(connector=TCP_CONNECTOR())
    try:
        async with session.get(url, ssl=False) as resp:
            data = await resp.json()
            return data["data"][0]["imageUrl"]
    except Exception as e:
        logger.error(f"Lỗi get_image_url item {item_id}: {type(e).__name__}: {e}")
    finally:
        if owns_session:
            await session.close()
    return None
