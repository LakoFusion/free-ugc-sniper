"""
src/webhook.py
==============
Gửi các loại Discord webhook:
  - send_buy_success    → mua thành công
  - send_buy_failed     → mua thất bại
  - send_not_available  → item không đủ điều kiện
  - send_detect_error   → lỗi khi detect/fetch
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)


async def send_buy_success(
    session: aiohttp.ClientSession,
    webhook_url: str,
    item_id: int,
    serial: str,
    cookie_info: dict,
    total_time_ms: float,
    image_url: Optional[str] = None,
    anonymous: bool = False,
):
    """Webhook khi mua thành công."""
    account_name = "Anonymous" if anonymous else cookie_info['user_name']
    embed = {
        "title": f"🎯 Bought item {item_id}",
        "url":   f"https://www.roblox.com/catalog/{item_id}",
        "color": 0x00FF00,
        "fields": [
            {
                "name":   "📊 Details",
                "value":  f"Serial: **#{serial}**\nAccount: **{account_name}**",
                "inline": True,
            },
            {
                "name":   "⚡ Speed",
                "value":  f"**{total_time_ms:.0f}ms**\nDetection → Purchase",
                "inline": True,
            },
        ],
        "footer":    {"text": f"Total: {total_time_ms:.0f}ms"},
        "timestamp": _now_iso(),
    }
    if image_url:
        embed["thumbnail"] = {"url": image_url}

    await _post(session, webhook_url, embed)


async def send_buy_failed(
    session: aiohttp.ClientSession,
    webhook_url: str,
    item_id: int,
    cookie_info: dict,
    error_reason: str,
    total_time_ms: float,
    price: int = 0,
    image_url: Optional[str] = None,
    anonymous: bool = False,
):
    """Webhook khi mua thất bại."""
    account_name = "Anonymous" if anonymous else cookie_info['user_name']
    embed = {
        "title": f"❌ Failed to buy item {item_id}",
        "url":   f"https://www.roblox.com/catalog/{item_id}",
        "color": 0xFF0000,
        "fields": [
            {
                "name":   "📊 Details",
                "value":  f"Account: **{account_name}**\nPrice: **{price}** R$",
                "inline": True,
            },
            {
                "name":   "❌ Error",
                "value":  f"**{error_reason}**",
                "inline": True,
            },
            {
                "name":   "⏱️ Speed",
                "value":  f"**{total_time_ms:.0f}ms**",
                "inline": True,
            },
        ],
        "timestamp": _now_iso(),
    }
    if image_url:
        embed["thumbnail"] = {"url": image_url}

    await _post(session, webhook_url, embed)


async def send_not_available(
    session: aiohttp.ClientSession,
    webhook_url: str,
    item_id: int,
    item_name: str,
    reason: str,
    conditions: dict,
    total_time_ms: float,
    fetch_time_ms: float,
    image_url: Optional[str] = None,
):
    """Webhook khi item không đủ điều kiện mua (not for sale, sold out, v.v.)."""
    embed = {
        "title": f"⚠️ Item {item_id} – Conditions Not Met",
        "url":   f"https://www.roblox.com/catalog/{item_id}",
        "color": 0xFFA500,
        "fields": [
            {
                "name":  "📦 Item",
                "value": f"**{item_name}**",
            },
            {
                "name":  "❌ Reason",
                "value": f"**{reason}**",
            },
            {
                "name":  "📊 Details",
                "value": (
                    f"• For Sale: `{conditions.get('is_for_sale')}`\n"
                    f"• Remaining: `{conditions.get('remaining')}`\n"
                    f"• Price: `{conditions.get('price')} R$`\n"
                    f"• Location: `{conditions.get('sale_location') or 'None'}`"
                ),
            },
            {
                "name":  "⏱️ Timing",
                "value": f"Fetch: `{fetch_time_ms:.0f}ms` | Total: `{total_time_ms:.0f}ms`",
            },
        ],
        "timestamp": _now_iso(),
    }
    if image_url:
        embed["thumbnail"] = {"url": image_url}

    await _post(session, webhook_url, embed)


async def send_detect_error(
    session: aiohttp.ClientSession,
    webhook_url: str,
    item_id: int,
    error_reason: str,
    total_time_ms: float,
    fetch_time_ms: float,
    image_url: Optional[str] = None,
):
    """Webhook khi lỗi trong quá trình detect/fetch."""
    embed = {
        "title": f"🔴 Item {item_id} – Detection Failed",
        "url":   f"https://www.roblox.com/catalog/{item_id}",
        "color": 0xFF0000,
        "fields": [
            {
                "name":  "❌ Error",
                "value": f"**{error_reason}**",
            },
            {
                "name":  "⏱️ Timing",
                "value": f"Fetch: `{fetch_time_ms:.0f}ms` | Total: `{total_time_ms:.0f}ms`",
            },
        ],
        "timestamp": _now_iso(),
    }
    if image_url:
        embed["thumbnail"] = {"url": image_url}

    await _post(session, webhook_url, embed)


# ─── Internal helper ──────────────────────────────────────────────────────────

async def send_proxy_error(
    session: aiohttp.ClientSession,
    webhook_url: str,
    context: str,
    error_reason: str,
    item_id: Optional[int] = None,
):
    """Webhook khi request bat buoc proxy nhung khong proxy nao dung duoc."""
    embed = {
        "title": f"Proxy required failed{f' item {item_id}' if item_id is not None else ''}",
        "color": 0xFF0000,
        "fields": [
            {"name": "Context", "value": f"`{context}`"},
            {"name": "Proxy error", "value": f"**{error_reason}**"},
        ],
        "timestamp": _now_iso(),
    }
    if item_id is not None:
        embed["url"] = f"https://www.roblox.com/catalog/{item_id}"

    await _post(session, webhook_url, embed)


async def _post(session: aiohttp.ClientSession, url: str, embed: dict):
    try:
        async with session.post(
            url,
            json={"embeds": [embed]},
            timeout=aiohttp.ClientTimeout(total=3),
        ) as resp:
            if resp.status not in (200, 204):
                logger.warning(f"[Webhook] HTTP {resp.status}")
    except Exception as e:
        logger.error(f"[Webhook] Lỗi gửi: {type(e).__name__}: {e}")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
