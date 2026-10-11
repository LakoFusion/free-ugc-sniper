"""
src/item_checker.py
===================
Kiểm tra trạng thái item (onsale / offsale / điều kiện xoá):
  - is_item_on_sale          → verify item đang onsale không
  - should_remove_offsale    → item offsale có nên xoá khỏi scan list
"""

import logging
import time

import aiohttp

from src.roblox_common import bad_sale_location
from src.roblox_fetch import fetch_asset_details, fetch_asset_details_with_meta

logger = logging.getLogger(__name__)

_VERIFY_LOG_INTERVAL_SECONDS = 60
_last_verify_log: dict[tuple[str, int], float] = {}


def _warning_once_per_interval(key: tuple[str, int], message: str):
    now = time.monotonic()
    last = _last_verify_log.get(key, 0)
    if now - last < _VERIFY_LOG_INTERVAL_SECONDS:
        return
    _last_verify_log[key] = now
    logger.warning(message)


def _meta_is_rate_limited(meta: dict) -> bool:
    return isinstance(meta, dict) and meta.get("status") == 429


async def get_item_sale_state(
    session: aiohttp.ClientSession,
    item_id: int,
    search_cookie: str,
    global_max_price: int,
) -> dict:
    """Fetch item once and classify sale state for buy/scanner decisions."""
    try:
        item, meta = await fetch_asset_details_with_meta(session, item_id, search_cookie)
    except Exception as e:
        logger.error(f"[SaleState] [{item_id}] Error: {type(e).__name__}: {e}")
        return {
            "state": "fetch_failed",
            "reason": f"{type(e).__name__}: {e}",
            "item": None,
            "meta": {"source": "economy", "error": type(e).__name__},
            "remove": False,
        }

    if item is None:
        state = "rate_limited" if _meta_is_rate_limited(meta) else "fetch_failed"
        reason = "HTTP 429" if state == "rate_limited" else "fetch failed"
        return {
            "state": state,
            "reason": reason,
            "item": None,
            "meta": meta,
            "remove": False,
        }

    is_for_sale = item.get("IsForSale", False)
    remaining = item.get("Remaining")
    price = item.get("PriceInRobux", 0) or 0
    sale_location = item.get("SaleLocation")
    remaining_ok = (remaining is None) or (remaining > 0)
    location_ok = not bad_sale_location(sale_location)
    price_ok = price <= global_max_price or price == 0

    if not remaining_ok:
        return {
            "state": "sold_out",
            "reason": f"Sold out (Remaining={remaining})",
            "item": item,
            "meta": meta,
            "remove": True,
        }

    if not location_ok:
        return {
            "state": "bad_location",
            "reason": "DevAPI/bad location",
            "item": item,
            "meta": meta,
            "remove": True,
        }

    if not is_for_sale:
        return {
            "state": "offsale",
            "reason": "NotForSale",
            "item": item,
            "meta": meta,
            "remove": not (remaining_ok and location_ok and price_ok),
        }

    if not price_ok:
        return {
            "state": "offsale",
            "reason": f"Price {price} > {global_max_price}",
            "item": item,
            "meta": meta,
            "remove": True,
        }

    return {
        "state": "onsale",
        "reason": "onsale",
        "item": item,
        "meta": meta,
        "remove": False,
    }


async def is_item_on_sale(
    session: aiohttp.ClientSession,
    item_id: int,
    search_cookie: str,
) -> bool:
    """
    Verify xem item hiện tại có đang onsale hay không.
    Dùng để tránh spam buy khi item đã offsale → gây 403/429.
    """
    try:
        item = await fetch_asset_details(session, item_id, search_cookie)
        if item is None:
            _warning_once_per_interval(
                ("sale", int(item_id)),
                f"[VerifySale] ⚠️ [{item_id}] Không fetch được details",
            )
            return False

        is_for_sale   = item.get("IsForSale", False)
        remaining     = item.get("Remaining")
        remaining_ok  = (remaining is None) or (remaining > 0)
        sale_location = item.get("SaleLocation")

        if not is_for_sale or not remaining_ok or bad_sale_location(sale_location):
            logger.debug(
                f"[VerifySale] 🚫 [{item_id}] KHÔNG onsale "
                f"(IsForSale={is_for_sale}, Remaining={remaining}) → skip buy"
            )
            return False

        logger.info(f"[VerifySale] ✅ [{item_id}] Đang onsale (Remaining={remaining})")
        return True

    except Exception as e:
        logger.error(f"[VerifySale] ❌ [{item_id}] Error: {type(e).__name__}: {e}")
        return False


async def should_remove_offsale(
    session: aiohttp.ClientSession,
    item_id: int,
    search_cookie: str,
    global_max_price: int,
) -> bool:
    """
    Kiểm tra item OFFSALE có nên xoá khỏi scan list không.
    Chỉ xoá nếu: remaining <= 0 HOẶC bad location HOẶC price > globalPrice.
    Nếu vẫn đáp ứng điều kiện → giữ lại trong list để scan tiếp.
    """
    try:
        item = await fetch_asset_details(session, item_id, search_cookie)
        if item is None:
            _warning_once_per_interval(
                ("remove", int(item_id)),
                f"[VerifyRemove] ⚠️ [{item_id}] Không fetch được details → giữ lại",
            )
            return False

        remaining     = item.get("Remaining")
        remaining_ok  = (remaining is None) or (remaining > 0)
        sale_location = item.get("SaleLocation")
        price         = item.get("PriceInRobux", 0) or 0
        location_ok   = not bad_sale_location(sale_location)
        price_ok      = price <= global_max_price or price == 0

        if remaining_ok and location_ok and price_ok:
            logger.info(
                f"[VerifyRemove] ⏳ [{item_id}] OFFSALE nhưng đáp ứng điều kiện "
                f"(Remaining={remaining}, price={price}) → giữ lại scan list"
            )
            return False

        reason = []
        if not remaining_ok:
            reason.append(f"Remaining={remaining}")
        if not location_ok:
            reason.append("bad location")
        if not price_ok:
            reason.append(f"price={price} > {global_max_price}")
        logger.info(
            f"[VerifyRemove] 🗑️ [{item_id}] OFFSALE + điều kiện xấu "
            f"({', '.join(reason)}) → xoá khỏi list"
        )
        return True

    except Exception as e:
        logger.error(f"[VerifyRemove] ❌ [{item_id}] Error: {type(e).__name__}: {e} → giữ lại")
        return False
