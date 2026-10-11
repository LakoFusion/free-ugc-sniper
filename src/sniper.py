"""
src/sniper.py
=============
Bộ điều phối chính – chỉ chứa logic điều phối.
Mọi chi tiết đều được delegate sang các module:
  - config.py          → load_config
  - xcsrf.py           → get_xcsrf_token, auto_refresh_loop
  - roblox_common.py   → bad_sale_location, TCP_CONNECTOR
  - roblox_fetch.py    → get_user_info, fetch_asset_details, ...
  - roblox_buy.py      → buy_item
  - item_checker.py    → is_item_on_sale, should_remove_offsale
  - display.py         → print_status
  - discord_gateway.py → DiscordGateway
  - discord_bot.py     → startdiscord
  - webhook.py         → send_buy_success, send_buy_failed, ...
"""

import asyncio
import logging
import os
import random
import re
import time
from collections import deque
from itertools import cycle, islice
from typing import Optional

import aiohttp

from src.config           import load_config
from src.xcsrf            import get_xcsrf_token, auto_refresh_loop
from src.roblox_common    import (
    ProxyRequiredError,
    bad_sale_location,
    proxy_cache_key,
    proxy_candidates,
    TCP_CONNECTOR,
)
from src.roblox_fetch     import (
    get_user_info,
    fetch_asset_details,
    fetch_asset_details_with_meta,
    fetch_details_and_product_id_with_meta,
    format_fetch_failure,
    fetch_catalog_batch_with_meta,
    fetch_product_ids_batch,
    get_serial,
    get_image_url,
)
from src.roblox_buy       import buy_item
from src.item_checker     import is_item_on_sale, should_remove_offsale, get_item_sale_state
from src.display          import print_status
from src.discord_gateway  import DiscordGateway
from src.discord_bot      import startdiscord
import src.webhook as webhook

if os.name == "nt":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logger = logging.getLogger(__name__)


# ─── Sniper ───────────────────────────────────────────────────────────────────────

class Sniper:
    def __init__(self):
        # ── Load config ───────────────────────────────────────────────────────
        self.config        = load_config()
        raw_items          = self.config.get("items", [])
        self.items: list   = []
        for raw_id in raw_items:
            if isinstance(raw_id, int):
                self.items.append(raw_id)
            elif isinstance(raw_id, str) and raw_id.strip().isdigit():
                self.items.append(int(raw_id))
            else:
                logger.warning(f"[Config] Bỏ qua item sai định dạng: {raw_id!r}")
        self.globalPrice   = self.config.get("global_max_price", 1)
        antiratelimit_cfg  = self.config.get("antiratelimit", {})
        self.v1waitTime    = antiratelimit_cfg.get("v1_wait_time", 1.0)
        self.v2threads     = max(1, int(self.config.get("threads", {}).get("searcherv2_threads", 1)))
        self.buy_threads   = max(1, int(self.config.get("threads", {}).get("buy_threads", 2)))
        self.autov2waitTime= antiratelimit_cfg.get("v2_auto_wait_time", True)
        self.v2waitTime    = 0 if self.autov2waitTime else max(0.05, antiratelimit_cfg.get("v2_wait_time", 0.5))
        self.webhook_cfg   = self.config.get("webhook", {})
        self.performance_cfg = self.config.get("performance", {})
        self.prewarm_connections = self.performance_cfg.get("prewarm_connections", True)
        self.quiet_buy_logs = self.performance_cfg.get("quiet_buy_logs", True)
        self.v2_distribute_items = self.performance_cfg.get("v2_distribute_items", True)
        self.express_fast_fetch = self.performance_cfg.get("express_fast_fetch", True)
        self.proxy_buy_prewarm = self.performance_cfg.get("proxy_buy_prewarm", True)
        self.proxy_failure_cooldown_seconds = float(
            self.performance_cfg.get("proxy_failure_cooldown_seconds", 60)
        )
        self.proxy_timeout_cooldown_seconds = float(
            self.performance_cfg.get("proxy_timeout_cooldown_seconds", 3)
        )
        self.proxy_connect_cooldown_seconds = float(
            self.performance_cfg.get("proxy_connect_cooldown_seconds", 3)
        )
        self.proxy_auth_cooldown_seconds = float(
            self.performance_cfg.get(
                "proxy_auth_cooldown_seconds",
                self.proxy_failure_cooldown_seconds,
            )
        )
        self.prefer_xcsrf_proxy_for_buy = self.performance_cfg.get(
            "prefer_xcsrf_proxy_for_buy", True
        )
        self.status_refresh_seconds = float(self.performance_cfg.get("status_refresh_seconds", 1.0))
        self.defer_post_buy_reporting = self.performance_cfg.get("defer_post_buy_reporting", True)
        self.proxy_error_webhook_cooldown_seconds = float(
            self.performance_cfg.get("proxy_error_webhook_cooldown_seconds", 60)
        )
        self.detect_error_webhook_cooldown_seconds = float(
            self.performance_cfg.get("detect_error_webhook_cooldown_seconds", 120)
        )
        self.detect_error_global_limit_per_minute = int(
            self.performance_cfg.get("detect_error_global_limit_per_minute", 5)
        )
        self.express_fetch_min_interval_seconds = float(
            self.performance_cfg.get("express_fetch_min_interval_seconds", 0.15)
        )
        self.express_429_pause_seconds = float(
            self.performance_cfg.get("express_429_pause_seconds", 4.0)
        )
        self.express_400_scan_cooldown_seconds = float(
            self.performance_cfg.get("express_400_scan_cooldown_seconds", 2.0)
        )
        self.catalog_429_pause_seconds = float(
            self.performance_cfg.get("catalog_429_pause_seconds", 8.0)
        )
        self.tokens        = self.config.get("tokens", {}).get("watchers", [])

        # ── Runtime state ─────────────────────────────────────────────────────
        self.account: Optional[dict] = None
        self.totalSearches  = 0
        self.v1search       = 0
        self.v2search       = 0
        self.bought         = 0
        self.v2avgSpeed     = deque(maxlen=10)

        self.errorLogs           = deque(maxlen=10)
        self.waitLogs            = deque(maxlen=3)
        self.buyLogs             = deque(maxlen=4)
        self.searchLogs          = deque(maxlen=2)
        self.autosearch_errors   = deque(maxlen=10)
        self.autosearch_found_ids= deque(maxlen=5)
        self._last_proxy_error_webhook: dict[str, float] = {}
        self._discord_item_inflight: set[int] = set()
        self._account_ready = asyncio.Event()
        self._pending_discord_items = deque()
        self._pending_discord_item_ids: set[int] = set()
        self._pending_discord_limit = 100
        self._terminal_blocked_items: dict[int, str] = {}
        self._terminal_block_skip_logs: dict[int, float] = {}
        self._removed_until: dict[int, float] = {}
        self._scan_failure_counts: dict[int, int] = {}
        self._scan_cooldowns: dict[int, float] = {}
        self._scan_cooldown_skip_logs: dict[int, float] = {}
        self._detect_error_webhook_logs: dict[tuple[int, str], float] = {}
        self._detect_error_webhook_times = deque()
        self._express_fetch_lock = asyncio.Lock()
        self._express_next_fetch_at = 0.0
        self._economy_rate_limited_until = 0.0
        self._catalog_rate_limited_until = 0.0
        self._runtime_throttle_logs: dict[str, float] = {}
        self._buying_items: set[int] = set()
        self._buying_item_counts: dict[int, int] = {}
        self._buy_sale_state_tasks: dict[int, asyncio.Task] = {}

        # ── Anti-spam ───────────────────────────────────────────────────────────────
        self._verified_offsale: set[int] = set()

        self._items_event        = asyncio.Event()
        self._items_event.set()
        self._shared_session: Optional[aiohttp.ClientSession] = None
        self._critical_session: Optional[aiohttp.ClientSession] = None
        self._session_lock       = asyncio.Lock()
        self._critical_session_lock = asyncio.Lock()

        self.roblox_pattern   = re.compile(r"roblox\.com/catalog/(\d+)")
        self.rolimons_pattern = re.compile(r"rolimons\.com/item/(\d+)")

        # ── Proxy ─────────────────────────────────────────────────────────────
        self.proxies: list  = self._load_proxies()
        self._proxy_index   = 0
        self.proxy_enabled  = self.config.get("proxy", False)
        self._proxy_candidates_cache = []
        self._proxy_cooldowns: dict[str, float] = {}
        self._proxy_cooldown_types: dict[str, str] = {}
        self._preferred_proxy_key: Optional[str] = None
        self._buy_proxy_in_use: set[str] = set()
        self._rebuild_proxy_cache()
        if self.proxy_enabled and self.proxies:
            logger.info(f"✅ Loaded {len(self.proxies)} proxies")
        elif self.proxy_enabled and not self.proxies:
            logger.warning("⚠️ Proxy enabled nhưng proxies.txt trống!")

        logger.info("✅ Sniper initialized")

    # ─── Logging helpers ────────────────────────────────────────────────────────────

    def _now(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    def log_error(self, message: str):
        entry = f"[{self._now()}] {message}"
        self.errorLogs.append(entry)
        self.autosearch_errors.append(entry)
        logger.error(message)

    def log_search(self, message: str):
        self.searchLogs.append(f"[{self._now()}] {message}")
        logger.info(message)

    def log_purchase(self, item_id, serial):
        entry = f"[{self._now()}] Bought item {item_id}, serial {serial}"
        self.buyLogs.append(entry)
        logger.info(entry)

    # ─── Proxy helpers ──────────────────────────────────────────────────────────────

    async def _notify_proxy_required(
        self,
        context: str,
        error: str,
        item_id: Optional[int] = None,
    ):
        self.log_error(f"[ProxyRequired] {context}: {error}")
        if not self.webhook_cfg.get("enabled"):
            return

        key = f"{context}:{item_id or ''}"
        now = time.monotonic()
        last_sent = self._last_proxy_error_webhook.get(key, 0)
        if now - last_sent < self.proxy_error_webhook_cooldown_seconds:
            return
        self._last_proxy_error_webhook[key] = now

        try:
            session = await self._get_shared_session()
            await webhook.send_proxy_error(
                session,
                self.webhook_cfg["url"],
                context,
                error,
                item_id=item_id,
            )
        except Exception as e:
            logger.error(f"[ProxyRequired] Webhook error: {type(e).__name__}: {e}")

    def _load_proxies(self) -> list:
        try:
            with open("proxies.txt", "r", encoding="utf-8") as f:
                proxies = [
                    line for raw in f
                    if (line := raw.strip()) and not line.startswith("#")
                ]
            logger.info(f"[Proxy] Đọc được {len(proxies)} proxy từ proxies.txt")
            return proxies
        except FileNotFoundError:
            logger.warning("[Proxy] Không tìm thấy proxies.txt")
            return []

    # ─── Session ─────────────────────────────────────────────────────────────────────

    def _rebuild_proxy_cache(self):
        self._proxy_candidates_cache = []
        for proxy in self.proxies:
            try:
                self._proxy_candidates_cache.extend(proxy_candidates(proxy))
            except ValueError as e:
                logger.warning(f"[Proxy] Skip proxy format sai: {e}")

    def _remember_proxy_key(self, proxy_key: Optional[str]):
        if not proxy_key or not self.prefer_xcsrf_proxy_for_buy:
            return
        self._preferred_proxy_key = proxy_key
        self._proxy_cooldowns.pop(proxy_key, None)
        self._proxy_cooldown_types.pop(proxy_key, None)

    def _remember_xcsrf_proxy(self, token_data: dict):
        self._remember_proxy_key(token_data.get("proxy_key"))

    def _record_buy_proxy_result(self, result: dict):
        if not self.proxy_enabled or not isinstance(result, dict):
            return
        proxy_key = result.get("proxy_key")
        proxy_line_key = result.get("proxy_line_key")
        if result.get("proxy_failed") and result.get("proxy_error_type") == "auth" and proxy_line_key:
            cooldown = self._proxy_cooldown_seconds(result)
            until = time.monotonic() + cooldown
            line_keys = [key for key in str(proxy_line_key).split("||") if key]
            for key in line_keys:
                self._proxy_cooldowns[key] = until
                self._proxy_cooldown_types[key] = "auth"
            if self._preferred_proxy_key in line_keys:
                self._preferred_proxy_key = None
            return
        if not proxy_key:
            return
        if result.get("proxy_failed"):
            cooldown = self._proxy_cooldown_seconds(result)
            if cooldown <= 0:
                return
            self._proxy_cooldowns[proxy_key] = time.monotonic() + cooldown
            self._proxy_cooldown_types[proxy_key] = result.get("proxy_error_type") or "other"
            if self._preferred_proxy_key == proxy_key:
                self._preferred_proxy_key = None
        else:
            self._proxy_cooldown_types.pop(proxy_key, None)
            self._remember_proxy_key(proxy_key)

    def _proxy_cooldown_seconds(self, result: dict) -> float:
        error_type = result.get("proxy_error_type")
        if error_type == "auth":
            return self.proxy_auth_cooldown_seconds
        if error_type == "timeout":
            return self.proxy_timeout_cooldown_seconds
        if error_type == "connect":
            return self.proxy_connect_cooldown_seconds
        if error_type == "invalid":
            return self.proxy_auth_cooldown_seconds
        return self.proxy_failure_cooldown_seconds

    def _record_fetch_proxy_error(self, proxy_key: Optional[str], error_type: str):
        """Callback cho fetch-layer: proxy transport lỗi → cooldown ngay."""
        if not self.proxy_enabled or not proxy_key:
            return
        cooldown = (
            self.proxy_auth_cooldown_seconds if error_type == "auth"
            else self.proxy_timeout_cooldown_seconds if error_type == "timeout"
            else self.proxy_connect_cooldown_seconds if error_type == "connect"
            else self.proxy_failure_cooldown_seconds
        )
        until = time.monotonic() + cooldown
        self._proxy_cooldowns[proxy_key] = max(
            self._proxy_cooldowns.get(proxy_key, 0), until
        )
        self._proxy_cooldown_types[proxy_key] = error_type

    def _is_auth_proxy_cooldown_active(self, key: Optional[str], now: Optional[float] = None) -> bool:
        if not key:
            return False
        now = time.monotonic() if now is None else now
        until = self._proxy_cooldowns.get(key, 0)
        if until <= 0:
            return False
        if until <= now:
            self._proxy_cooldowns.pop(key, None)
            self._proxy_cooldown_types.pop(key, None)
            return False
        return self._proxy_cooldown_types.get(key) == "auth"

    def _ordered_proxy_candidates(self, include_cooled_down_fallback: bool = True) -> list:
        if not self.proxy_enabled or not self.proxies or not self._proxy_candidates_cache:
            return []

        now = time.monotonic()
        active = []
        cooled = []
        for candidate in self._proxy_candidates_cache:
            key = proxy_cache_key(candidate)
            if self._is_auth_proxy_cooldown_active(key, now):
                continue
            if key and self._proxy_cooldowns.get(key, 0) > now:
                cooled.append(candidate)
            else:
                active.append(candidate)

        ordered = active or (cooled if include_cooled_down_fallback else [])
        if self._preferred_proxy_key:
            preferred = []
            others = []
            for candidate in ordered:
                if proxy_cache_key(candidate) == self._preferred_proxy_key:
                    preferred.append(candidate)
                else:
                    others.append(candidate)
            ordered = preferred + others
        return ordered

    def _active_proxies(self) -> list:
        return self._ordered_proxy_candidates()

    def _buy_proxy_slot_key(self, proxy) -> Optional[str]:
        try:
            candidates = proxy_candidates(proxy)
        except ValueError:
            return None
        if not candidates:
            return None
        keys = [proxy_cache_key(candidate) for candidate in candidates if proxy_cache_key(candidate)]
        return "||".join(keys) if keys else None

    def _ordered_buy_proxy_slots(self) -> list:
        """Return one usable slot per proxy line so concurrent buy tasks never share a proxy."""
        if not self.proxy_enabled or not self.proxies:
            return []

        now = time.monotonic()
        active = []
        cooled = []
        in_use = getattr(self, "_buy_proxy_in_use", set())
        for proxy in self.proxies:
            try:
                candidates = proxy_candidates(proxy)
            except ValueError as e:
                logger.warning(f"[Proxy] Skip buy proxy format sai: {e}")
                continue
            if not candidates:
                continue

            keys = [proxy_cache_key(candidate) for candidate in candidates]
            if any(self._is_auth_proxy_cooldown_active(key, now) for key in keys):
                continue
            slot_key = self._buy_proxy_slot_key(proxy)
            if slot_key and slot_key in in_use:
                continue
            preferred = self._preferred_proxy_key in keys if self._preferred_proxy_key else False
            is_cooled = all(key and self._proxy_cooldowns.get(key, 0) > now for key in keys)
            entry = (proxy, preferred)
            if is_cooled:
                cooled.append(entry)
            else:
                active.append(entry)

        ordered = active + cooled
        ordered.sort(key=lambda entry: 0 if entry[1] else 1)
        return [proxy for proxy, _ in ordered]

    def _reserve_buy_proxy_slots(self, count: int) -> list:
        if not self.proxy_enabled:
            return [None] * count
        self._buy_proxy_in_use = getattr(self, "_buy_proxy_in_use", set())
        slots = []
        for proxy in self._ordered_buy_proxy_slots():
            key = self._buy_proxy_slot_key(proxy)
            if not key or key in self._buy_proxy_in_use:
                continue
            self._buy_proxy_in_use.add(key)
            slots.append(proxy)
            if len(slots) >= count:
                break
        return slots

    def _release_buy_proxy_slot(self, proxy):
        if not self.proxy_enabled or proxy is None:
            return
        key = self._buy_proxy_slot_key(proxy)
        if key:
            getattr(self, "_buy_proxy_in_use", set()).discard(key)

    def _buy_proxy_list(self, assigned_proxy=None) -> list:
        if self.proxy_enabled:
            if assigned_proxy is not None:
                return [assigned_proxy]
            return self._ordered_buy_proxy_slots()
        return [None]

    async def _get_shared_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._shared_session is None or self._shared_session.closed:
                self._shared_session = aiohttp.ClientSession(
                    connector=TCP_CONNECTOR(),
                    timeout=aiohttp.ClientTimeout(total=10, connect=5),
                )
                self.log_search("✓ Created shared session")
            return self._shared_session

    async def _get_critical_session(self) -> aiohttp.ClientSession:
        async with self._critical_session_lock:
            if self._critical_session is None or self._critical_session.closed:
                self._critical_session = aiohttp.ClientSession(
                    connector=TCP_CONNECTOR(),
                    timeout=aiohttp.ClientTimeout(total=6, connect=3),
                )
                self.log_search("✓ Created critical session")
            return self._critical_session

    async def _prewarm_connections(self):
        if not self.prewarm_connections:
            return
        urls = (
            "https://economy.roblox.com/",
            "https://apis.roblox.com/",
            "https://catalog.roblox.com/",
            "https://inventory.roblox.com/",
        )
        for session in (await self._get_shared_session(), await self._get_critical_session()):
            for url in urls:
                try:
                    async with session.get(url, ssl=False, timeout=aiohttp.ClientTimeout(total=2)):
                        pass
                except Exception:
                    pass
        self.log_search("✓ Prewarmed Roblox connections")

    async def _prewarm_buy_proxy(self):
        if not self.proxy_buy_prewarm or not self.proxy_enabled or not self.proxies:
            return
        candidates = self._ordered_proxy_candidates(include_cooled_down_fallback=False)
        if not candidates:
            candidates = self._ordered_proxy_candidates()
        if not candidates:
            return

        candidate = candidates[0]
        proxy_key = proxy_cache_key(candidate)
        session = await self._get_critical_session()
        try:
            async with session.get(
                "https://apis.roblox.com/",
                proxy=candidate.url,
                proxy_auth=candidate.auth,
                ssl=False,
                timeout=aiohttp.ClientTimeout(total=3, connect=2),
            ):
                pass
            self._remember_proxy_key(proxy_key)
            self.log_search(f"[Proxy] Prewarmed buy proxy {candidate.masked}")
        except Exception as e:
            self._record_buy_proxy_result({"proxy_key": proxy_key, "proxy_failed": True})
            logger.warning(
                f"[Proxy] Prewarm buy proxy {candidate.masked} failed: {type(e).__name__}"
            )

    # ─── Setup accounts ─────────────────────────────────────────────────────────────

    async def setup_accounts(self) -> dict:
        cfg           = self.config["cookies"]
        search_cookie = cfg["search_cookie"]
        buy_cookie    = cfg["buy_cookie"]

        token_data = await get_xcsrf_token(
            buy_cookie,
            proxies=self._active_proxies(),
            fallback_direct=not self.proxy_enabled,
            session=await self._get_critical_session(),
        )
        self._remember_xcsrf_proxy(token_data)
        user_info  = await get_user_info(
            buy_cookie, session=await self._get_shared_session()
        )
        buy_cookies = [{
            "cookie":      buy_cookie,
            "xcsrf_token": token_data["xcsrf_token"],
            "user_id":     user_info["id"],
            "user_name":   user_info["name"],
            "created":     token_data["created"],
            "xcsrf_proxy_used": token_data.get("proxy_used"),
            "xcsrf_proxy_key":  token_data.get("proxy_key"),
        }]

        search_token = await get_xcsrf_token(
            search_cookie,
            proxies=self._active_proxies(),
            fallback_direct=not self.proxy_enabled,
            session=await self._get_critical_session(),
        )
        self._remember_xcsrf_proxy(search_token)

        return {
            "buy_cookies":         buy_cookies,
            "search_cookie":       search_cookie,
            "search_xcsrf_token":  search_token["xcsrf_token"],
            "search_created":      search_token["created"],
            "search_xcsrf_proxy_used": search_token.get("proxy_used"),
            "search_xcsrf_proxy_key":  search_token.get("proxy_key"),
        }

    # ─── Auto xCSRF refresh ──────────────────────────────────────────────────────────

    async def autoXToken(self):
        while True:
            try:
                if self.account is None:
                    await asyncio.sleep(1)
                    continue
                await auto_refresh_loop(
                    self.account,
                    log_fn=self.log_search,
                    proxies=self._active_proxies(),
                    fallback_direct=not self.proxy_enabled,
                    error_fn=self._notify_proxy_required,
                    session=await self._get_critical_session(),
                )
                self.log_error("[xCSRF] auto refresh loop stopped unexpectedly; restarting")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.log_error(f"[xCSRF] auto refresh supervisor: {type(e).__name__}: {e}")
            await asyncio.sleep(2)

    # ─── URL helpers ─────────────────────────────────────────────────────────────────

    def _extract_item_id(self, url: str) -> Optional[str]:
        m = self.roblox_pattern.search(url)
        if m:
            return m.group(1)
        m = self.rolimons_pattern.search(url)
        return m.group(1) if m else None

    # ─── Buy / verify helpers ────────────────────────────────────────────────────────

    def _extract_item_ids_from_text(self, text) -> list[int]:
        if not isinstance(text, str) or not text:
            return []
        ids = []
        for pattern in (self.roblox_pattern, self.rolimons_pattern):
            for match in pattern.findall(text):
                # Roblox asset ID thực tế 6-13 chữ số — loại số serial/price 20+ chữ số
                if str(match).isdigit() and 6 <= len(match) <= 13:
                    ids.append(int(match))
        return ids

    def _iter_component_texts(self, component):
        if isinstance(component, dict):
            for key in ("url", "label", "custom_id"):
                value = component.get(key)
                if isinstance(value, str):
                    yield value
            for child in component.get("components", []) or []:
                yield from self._iter_component_texts(child)
        elif isinstance(component, list):
            for child in component:
                yield from self._iter_component_texts(child)

    def _iter_discord_message_texts(self, message: dict):
        content = message.get("content")
        if isinstance(content, str):
            yield content

        for embed in message.get("embeds", []) or []:
            if not isinstance(embed, dict):
                continue
            for key in ("url", "title", "description"):
                value = embed.get(key)
                if isinstance(value, str):
                    yield value
            author = embed.get("author") or {}
            footer = embed.get("footer") or {}
            if not isinstance(author, dict):
                author = {}
            if not isinstance(footer, dict):
                footer = {}
            for value in (author.get("url"), author.get("name"), footer.get("text")):
                if isinstance(value, str):
                    yield value
            for field in embed.get("fields", []) or []:
                if not isinstance(field, dict):
                    continue
                for value in (field.get("name"), field.get("value")):
                    if isinstance(value, str):
                        yield value

        for component in message.get("components", []) or []:
            yield from self._iter_component_texts(component)

        for snap in message.get("message_snapshots", []) or []:
            snap_message = snap.get("message") or {}
            if isinstance(snap_message, dict):
                yield from self._iter_discord_message_texts(snap_message)

    def _extract_item_ids_from_discord_event(self, event: dict) -> list[int]:
        seen = set()
        item_ids = []
        message = event.get("d", {})
        for text in self._iter_discord_message_texts(message):
            for item_id in self._extract_item_ids_from_text(text):
                if item_id not in seen:
                    seen.add(item_id)
                    item_ids.append(item_id)
        return item_ids

    def _begin_discord_item(self, item_id: int) -> bool:
        if item_id in self._discord_item_inflight:
            return False
        self._discord_item_inflight.add(item_id)
        return True

    def _end_discord_item(self, item_id: int):
        self._discord_item_inflight.discard(item_id)

    def _block_terminal_item(self, item_id: int, reason: str):
        item_id = int(item_id)
        if item_id in self._terminal_blocked_items:
            return
        self._clear_item_state(item_id)
        self._terminal_blocked_items[item_id] = reason
        self._remove_item(item_id)
        self.log_search(f"[Block] [{item_id}] Terminal skip: {reason}")

    def _is_terminal_blocked(self, item_id: int) -> bool:
        return int(item_id) in self._terminal_blocked_items

    def _terminal_block_reason(self, item_id: int) -> str:
        return self._terminal_blocked_items.get(int(item_id), "terminal")

    def _log_terminal_skip(self, item_id: int, source: str):
        item_id = int(item_id)
        now = time.monotonic()
        last = self._terminal_block_skip_logs.get(item_id, 0)
        if now - last < 60:
            return
        self._terminal_block_skip_logs[item_id] = now
        reason = self._terminal_block_reason(item_id)
        self.log_search(f"[Block] [{item_id}] Skip {source}: {reason}")

    def _filter_terminal_items(self, item_ids, source: str) -> list[int]:
        active = []
        for item_id in item_ids:
            if self._is_terminal_blocked(item_id):
                self._remove_item(item_id)
                self._log_terminal_skip(item_id, source)
                continue
            active.append(item_id)
        return active

    def _clear_item_state(self, item_id: int):
        """Xoá toàn bộ state theo-dõi item (gọi khi item rời scan list)."""
        item_id = int(item_id)
        self._terminal_blocked_items.pop(item_id, None)
        self._terminal_block_skip_logs.pop(item_id, None)
        self._scan_failure_counts.pop(item_id, None)
        self._scan_cooldowns.pop(item_id, None)
        self._scan_cooldown_skip_logs.pop(item_id, None)
        self._verified_offsale.discard(item_id)
        self._runtime_throttle_logs.pop(f"discord-verified-offsale-{item_id}", None)
        self._runtime_throttle_logs.pop(f"buy-wave-skip-{item_id}", None)
        if self._detect_error_webhook_logs:
            self._detect_error_webhook_logs = {
                key: ts for key, ts in self._detect_error_webhook_logs.items()
                if key[0] != item_id
            }
        self._buy_sale_state_tasks.pop(item_id, None)

    def _add_or_promote_scan_item(self, item_id: int) -> bool:
        item_id = int(item_id)
        if self._is_tombstoned(item_id):
            self._throttled_search_log(
                f"tombstone-skip-{item_id}",
                f"[Block] [{item_id}] Item vừa bị /remove — bỏ qua re-add tự động",
                interval=60,
            )
            return False
        was_present = item_id in self.items
        if was_present:
            self.items.remove(item_id)
        self.items.insert(0, item_id)
        items_event = getattr(self, "_items_event", None)
        if items_event:
            items_event.set()
        return not was_present

    def _is_tombstoned(self, item_id: int) -> bool:
        item_id = int(item_id)
        until = self._removed_until.get(item_id, 0.0)
        if until <= 0:
            return False
        if time.monotonic() >= until:
            self._removed_until.pop(item_id, None)
            return False
        return True

    def _readd_item(self, item_id: int) -> bool:
        """Re-add item (lệnh /add): xoá state cũ + tombstone rồi đưa lên đầu scan list."""
        item_id = int(item_id)
        self._removed_until.pop(item_id, None)
        self._clear_item_state(item_id)
        return self._add_or_promote_scan_item(item_id)

    def _forget_item(self, item_id: int):
        """Remove item (lệnh /remove): tombstone 60s chống task nền re-add + xoá state."""
        item_id = int(item_id)
        self._clear_item_state(item_id)
        if item_id in self.items:
            self.items.remove(item_id)
        items_event = getattr(self, "_items_event", None)
        if items_event:
            items_event.set()
        self._removed_until[item_id] = time.monotonic() + 60.0

    def _select_v2_item(self, items_snapshot: list, worker_id: int):
        if not items_snapshot:
            return None
        if self.v2_distribute_items:
            if worker_id >= len(items_snapshot):
                return None
            return items_snapshot[worker_id]
        return items_snapshot[0]

    def _meta_has_status(self, meta, status: int) -> bool:
        if isinstance(meta, dict):
            if meta.get("status") == status:
                return True
            return any(self._meta_has_status(value, status) for value in meta.values())
        if isinstance(meta, list):
            return any(self._meta_has_status(value, status) for value in meta)
        return False

    def _meta_has_error(self, meta, error: str) -> bool:
        if isinstance(meta, dict):
            if meta.get("error") == error:
                return True
            return any(self._meta_has_error(value, error) for value in meta.values())
        if isinstance(meta, list):
            return any(self._meta_has_error(value, error) for value in meta)
        return False

    def _throttled_search_log(self, key: str, message: str, interval: float = 15.0):
        now = time.monotonic()
        last = self._runtime_throttle_logs.get(key, 0)
        if now - last < interval:
            return
        self._runtime_throttle_logs[key] = now
        self.log_search(message)

    def _jittered_pause(self, base_seconds: float) -> float:
        base_seconds = max(0.0, float(base_seconds))
        if base_seconds <= 0:
            return 0.0
        return base_seconds + random.uniform(0, min(1.0, base_seconds * 0.25))

    def _economy_pause_remaining(self) -> float:
        return max(0.0, self._economy_rate_limited_until - time.monotonic())

    def _catalog_pause_remaining(self) -> float:
        return max(0.0, self._catalog_rate_limited_until - time.monotonic())

    def _record_economy_rate_limit(self, source: str):
        delay = self._jittered_pause(self.express_429_pause_seconds)
        self._economy_rate_limited_until = max(
            self._economy_rate_limited_until,
            time.monotonic() + delay,
        )
        self._throttled_search_log(
            "economy-429",
            f"[RateLimit] Economy HTTP 429 tu {source}; pause fetch ~{delay:.1f}s",
            interval=15,
        )

    def _record_catalog_rate_limit(self, source: str):
        delay = self._jittered_pause(self.catalog_429_pause_seconds)
        self._catalog_rate_limited_until = max(
            self._catalog_rate_limited_until,
            time.monotonic() + delay,
        )
        self._throttled_search_log(
            "catalog-429",
            f"[RateLimit] Catalog HTTP 429 tu {source}; pause V1 ~{delay:.1f}s",
            interval=15,
        )

    async def _wait_for_express_fetch_slot(self):
        async with self._express_fetch_lock:
            target = max(self._express_next_fetch_at, self._economy_rate_limited_until)
            delay = target - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            self._express_next_fetch_at = (
                time.monotonic() + self.express_fetch_min_interval_seconds
            )

    def _cooldown_scan_item(self, item_id: int, seconds: float, reason: str):
        item_id = int(item_id)
        until = time.monotonic() + max(0.0, seconds)
        self._scan_cooldowns[item_id] = max(
            self._scan_cooldowns.get(item_id, 0),
            until,
        )
        self._throttled_search_log(
            f"scan-cooldown-{reason}",
            f"[Cooldown] {reason}; pause related scan item(s) ~{seconds:.1f}s",
            interval=20,
        )

    def _defer_express_fetch_failure(self, item_id: int, fetch_meta: dict, reason: str) -> bool:
        if self._meta_has_status(fetch_meta, 429):
            self._record_economy_rate_limit("express")
            self._add_or_promote_scan_item(item_id)
            self._cooldown_scan_item(item_id, self.express_429_pause_seconds, "express HTTP 429")
            return True

        if self._meta_has_status(fetch_meta, 400) and self._meta_has_error(fetch_meta, "empty"):
            self._add_or_promote_scan_item(item_id)
            self._cooldown_scan_item(
                item_id,
                self.express_400_scan_cooldown_seconds,
                "express HTTP 400/not synced",
            )
            self._throttled_search_log(
                "express-400-empty",
                f"[Express] HTTP 400 + catalog empty; giu item trong scan list de thu lai",
                interval=30,
            )
            return True

        return False

    def _is_scan_cooled(self, item_id: int) -> bool:
        item_id = int(item_id)
        cooldowns = getattr(self, "_scan_cooldowns", {})
        until = cooldowns.get(item_id, 0)
        if until <= 0:
            return False
        if time.monotonic() >= until:
            cooldowns.pop(item_id, None)
            return False
        return True

    def _log_scan_cooldown_skip(self, item_id: int, source: str):
        item_id = int(item_id)
        logs = getattr(self, "_scan_cooldown_skip_logs", {})
        now = time.monotonic()
        last = logs.get(item_id, 0)
        if now - last < 60:
            return
        logs[item_id] = now
        cooldowns = getattr(self, "_scan_cooldowns", {})
        remaining = max(0, int(cooldowns.get(item_id, now) - now))
        self.log_search(f"[Cooldown] [{item_id}] Skip {source} for {remaining}s")

    def _filter_scan_ready_items(self, item_ids, source: str) -> list[int]:
        active = []
        for item_id in item_ids:
            if self._is_scan_cooled(item_id):
                self._log_scan_cooldown_skip(item_id, source)
                continue
            active.append(item_id)
        return active

    def _scan_cooldown_seconds(self, failure_count: int) -> int:
        level = max(1, failure_count // 3)
        return min(300 * level, 900)

    def _record_scan_fetch_failure(self, item_id: int, meta=None, source: str = "scan"):
        item_id = int(item_id)
        counts = getattr(self, "_scan_failure_counts", {})
        cooldowns = getattr(self, "_scan_cooldowns", {})
        self._scan_failure_counts = counts
        self._scan_cooldowns = cooldowns

        counts[item_id] = counts.get(item_id, 0) + 1
        count = counts[item_id]
        if count < 3:
            return

        seconds = self._scan_cooldown_seconds(count)
        cooldowns[item_id] = time.monotonic() + seconds
        reason = "HTTP 429" if self._meta_has_status(meta, 429) else "no details"
        self.log_search(
            f"[Cooldown] [{item_id}] {source} {reason} x{count} -> pause {seconds}s"
        )

    def _reset_scan_fetch_failure(self, item_id: int):
        item_id = int(item_id)
        getattr(self, "_scan_failure_counts", {}).pop(item_id, None)
        getattr(self, "_scan_cooldowns", {}).pop(item_id, None)
        getattr(self, "_scan_cooldown_skip_logs", {}).pop(item_id, None)

    def _should_send_detect_error(self, item_id: int, reason: str) -> bool:
        logs = getattr(self, "_detect_error_webhook_logs", {})
        self._detect_error_webhook_logs = logs
        recent = getattr(self, "_detect_error_webhook_times", deque())
        self._detect_error_webhook_times = recent
        key = (int(item_id), str(reason))
        now = time.monotonic()
        while recent and now - recent[0] > 60:
            recent.popleft()
        if len(recent) >= self.detect_error_global_limit_per_minute:
            self._throttled_search_log(
                "detect-error-webhook-throttle",
                "[Webhook] Detect-error webhook dang bi throttle de tranh HTTP 429",
                interval=30,
            )
            return False
        last = logs.get(key, 0)
        cooldown = getattr(self, "detect_error_webhook_cooldown_seconds", 120)
        if now - last < cooldown:
            return False
        logs[key] = now
        recent.append(now)
        return True

    def _queue_discord_item(self, item_id: int, start_time: float):
        if self._is_terminal_blocked(item_id):
            self._log_terminal_skip(item_id, "discord queue")
            return
        if item_id in self._pending_discord_item_ids:
            return
        if item_id in self._discord_item_inflight:
            return
        if len(self._pending_discord_items) >= self._pending_discord_limit:
            old_item_id, _ = self._pending_discord_items.popleft()
            self._pending_discord_item_ids.discard(old_item_id)
        self._pending_discord_items.append((item_id, start_time))
        self._pending_discord_item_ids.add(item_id)
        self.log_search(
            f"[Gateway] Detected item {item_id} but account not ready; queued"
        )

    async def _process_discord_item(self, item_id: int, start_time: float):
        if self._is_terminal_blocked(item_id):
            self._log_terminal_skip(item_id, "discord")
            return
        if self.account is None:
            self._queue_discord_item(item_id, start_time)
            return
        if self._is_verified_offsale(item_id):
            self._add_or_promote_scan_item(item_id)
            self._throttled_search_log(
                f"discord-verified-offsale-{item_id}",
                f"[Gateway] [{item_id}] da verified offsale -> scan-only",
                interval=30,
            )
            return
        if not self._begin_discord_item(item_id):
            return
        try:
            await self._autosearch_buy_express(item_id, start_time)
        finally:
            self._end_discord_item(item_id)

    async def _drain_pending_discord_items(self):
        if self.account is None or not self._pending_discord_items:
            return
        pending = list(self._pending_discord_items)
        self._pending_discord_items.clear()
        self._pending_discord_item_ids.clear()
        self.log_search(f"[Gateway] Processing {len(pending)} queued Discord item(s)")
        for item_id, start_time in pending:
            if self._is_terminal_blocked(item_id):
                self._log_terminal_skip(item_id, "pending discord")
                continue
            task = asyncio.create_task(self._process_discord_item(item_id, start_time))
            task.add_done_callback(self._log_background_error)

    async def _is_item_on_sale(self, session: aiohttp.ClientSession, item_id: int) -> bool:
        return await is_item_on_sale(session, item_id, self.account["search_cookie"])

    async def _should_remove_offsale_item(self, session: aiohttp.ClientSession, item_id: int) -> bool:
        return await should_remove_offsale(session, item_id, self.account["search_cookie"], self.globalPrice)

    async def _get_item_sale_state(self, session: aiohttp.ClientSession, item_id: int) -> dict:
        state = await get_item_sale_state(
            session,
            item_id,
            self.account["search_cookie"],
            self.globalPrice,
        )
        if state.get("state") in {"rate_limited", "fetch_failed"}:
            self._record_scan_fetch_failure(item_id, state.get("meta"), "buy verify")
        else:
            self._reset_scan_fetch_failure(item_id)
        return state

    async def _get_item_sale_state_for_buy(self, session: aiohttp.ClientSession, item_id: int) -> dict:
        item_id = int(item_id)
        tasks = getattr(self, "_buy_sale_state_tasks", {})
        self._buy_sale_state_tasks = tasks
        task = tasks.get(item_id)
        if task is None or task.done():
            task = asyncio.create_task(self._get_item_sale_state(session, item_id))
            tasks[item_id] = task
        try:
            return await task
        finally:
            if task.done() and tasks.get(item_id) is task:
                tasks.pop(item_id, None)

    def _is_flooded_buy_error(self, error: str) -> bool:
        text = str(error or "").lower()
        return "flooded" in text or "purchase requests exceeds limit" in text

    def _begin_buy_wave(self, item_id: int) -> bool:
        item_id = int(item_id)
        buying = getattr(self, "_buying_items", set())
        self._buying_items = buying
        if item_id in buying:
            self._throttled_search_log(
                f"buy-wave-skip-{item_id}",
                f"[BuyWave] [{item_id}] buy wave dang chay, skip wave trung",
                interval=10,
            )
            return False
        buying.add(item_id)
        return True

    def _set_buy_wave_task_count(self, item_id: int, count: int):
        item_id = int(item_id)
        if count <= 0:
            self._end_buy_wave(item_id)
            return
        self._buying_item_counts[item_id] = count

    def _release_buy_wave_task(self, item_id: int):
        item_id = int(item_id)
        counts = getattr(self, "_buying_item_counts", {})
        remaining = counts.get(item_id, 0) - 1
        if remaining > 0:
            counts[item_id] = remaining
            return
        counts.pop(item_id, None)
        self._end_buy_wave(item_id)

    def _end_buy_wave(self, item_id: int):
        item_id = int(item_id)
        getattr(self, "_buying_item_counts", {}).pop(item_id, None)
        getattr(self, "_buying_items", set()).discard(item_id)

    def _apply_sale_state_after_buy_error(
        self,
        item_id: int,
        state_info: dict,
        error: str,
        status: int,
    ):
        state = state_info.get("state")
        reason = state_info.get("reason") or error or f"HTTP {status}"

        if state == "onsale":
            self.log_search(
                f"⚠️ [{item_id}] {error} nhưng verify = vẫn ONSALE -> lỗi tạm thời"
            )
            return

        if state == "offsale":
            self._mark_offsale(item_id, do_remove=bool(state_info.get("remove")))
            return

        if state == "sold_out":
            self._block_terminal_item(item_id, reason)
            self.log_search(f"🗑️ [{item_id}] {reason} -> xoá khỏi scan list")
            return

        if state == "bad_location":
            self._block_terminal_item(item_id, reason)
            self.log_search(f"🗑️ [{item_id}] {reason} -> xoá khỏi scan list")
            return

        if state in {"rate_limited", "fetch_failed"}:
            self.log_search(
                f"⚠️ [{item_id}] {error} + verify {reason}; scanner cooldown, chưa kết luận offsale"
            )
            return

        self.log_search(f"⚠️ [{item_id}] {error} verify state={state or 'unknown'}")

    def _remove_item(self, item_id: int):
        if item_id in self.items:
            self.items.remove(item_id)

    def _mark_offsale(self, item_id: int, do_remove: bool = False):
        """Đánh dấu item đã xác nhận offsale. Xoá khỏi list nếu do_remove=True."""
        self._verified_offsale.add(item_id)
        if do_remove:
            self._remove_item(item_id)
            self.log_search(f"🗑️ [{item_id}] Verified OFFSALE + điều kiện xấu → xoá khỏi list")
        else:
            self._add_or_promote_scan_item(item_id)
            self.log_search(f"⏳ [{item_id}] Verified OFFSALE nhưng đáp ứng điều kiện → giữ trong scan list")

    def _unmark_offsale(self, item_id: int):
        """Xoá đánh dấu offsale (khi verify onsale OK hoặc buy thành công)."""
        self._verified_offsale.discard(item_id)

    def _is_verified_offsale(self, item_id: int) -> bool:
        """Kiểm tra item đã bị xác nhận offsale chưa."""
        return item_id in self._verified_offsale

    def _create_buy_tasks(
        self,
        session: aiohttp.ClientSession,
        buy_info: dict,
        start_time: float,
    ) -> list:
        return self._create_buy_tasks_for_candidates(session, [buy_info], start_time)

    def _create_buy_tasks_for_candidates(
        self,
        session: aiohttp.ClientSession,
        buy_infos: list[dict],
        start_time: float,
    ) -> list:
        active_buy_infos = []
        started_item_ids = []
        for buy_info in buy_infos:
            item_id = int(buy_info["item_id"])
            if not self._begin_buy_wave(item_id):
                continue
            active_buy_infos.append(buy_info)
            started_item_ids.append(item_id)

        desired = []
        for buy_info in active_buy_infos:
            for cookie_info in self.account["buy_cookies"]:
                for _ in range(self.buy_threads):
                    desired.append((buy_info, cookie_info))

        if not desired:
            for item_id in started_item_ids:
                self._end_buy_wave(item_id)
            return []

        if self.proxy_enabled:
            proxy_slots = self._reserve_buy_proxy_slots(len(desired))
            if not proxy_slots:
                item_ids = ", ".join(str(info["item_id"]) for info, _ in desired[:3])
                self.log_error(f"[ProxyRequired] buy: no usable buy proxy for item(s) {item_ids}")
                first_cookie = self.account["buy_cookies"][0]
                for item_id in started_item_ids:
                    self._end_buy_wave(item_id)
                return [
                    asyncio.create_task(
                        self._buy_proxy_required_result(info, first_cookie, start_time)
                    )
                    for info in active_buy_infos
                ]

            if len(proxy_slots) < len(desired):
                logger.warning(
                    "[BuyProxy] proxy slots %s < desired buy tasks %s; "
                    "capping concurrent buy tasks to unique proxies",
                    len(proxy_slots),
                    len(desired),
                )
            desired = desired[:len(proxy_slots)]
            assigned = proxy_slots[:len(desired)]
        else:
            assigned = [None] * len(desired)

        assigned_counts = {}
        for buy_info, _ in desired:
            item_id = int(buy_info["item_id"])
            assigned_counts[item_id] = assigned_counts.get(item_id, 0) + 1
        for item_id in started_item_ids:
            self._set_buy_wave_task_count(item_id, assigned_counts.get(item_id, 0))

        tasks = []
        for (buy_info, cookie_info), assigned_proxy in zip(desired, assigned):
            tasks.append(
                asyncio.create_task(
                    self._attempt_buy_with_proxy_release(
                        session,
                        buy_info,
                        cookie_info,
                        start_time,
                        assigned_proxy=assigned_proxy,
                    )
                )
            )
        return tasks

    async def _attempt_buy_with_proxy_release(
        self,
        session: aiohttp.ClientSession,
        buy_info: dict,
        cookie_info: dict,
        start_time: float,
        assigned_proxy=None,
    ) -> dict:
        item_id = int(buy_info["item_id"])
        try:
            return await self._attempt_buy(
                session,
                buy_info,
                cookie_info,
                start_time,
                assigned_proxy=assigned_proxy,
            )
        finally:
            self._release_buy_proxy_slot(assigned_proxy)
            self._release_buy_wave_task(item_id)

    async def _buy_proxy_required_result(
        self,
        buy_info: dict,
        cookie_info: dict,
        start_time: float,
    ) -> dict:
        return {
            "purchased": False,
            "item_id": buy_info["item_id"],
            "serial": None,
            "error": "ProxyRequired: no usable buy proxy",
            "total_ms": (time.time() - start_time) * 1000,
            "price": buy_info.get("price", 0),
            "cookie_info": cookie_info,
            "suppress_buy_failed_webhook": False,
        }

    async def _attempt_buy(
        self,
        session: aiohttp.ClientSession,
        buy_info: dict,
        cookie_info: dict,
        start_time: float,
        assigned_proxy=None,
    ) -> dict:
        """
        Gọi buy_item với proxy retry → nếu mua được thì fetch lại details
        → còn hàng thì tiếp tục mua cho đến khi sold out.

        Returns dict:
            {"purchased": bool, "item_id": int, "serial": str|None,
             "error": str|None, "total_ms": float, "price": int,
             "cookie_info": dict}

        Webhook được gửi bởi CALLER sau khi TẤT CẢ buy threads hoàn tất.
        """
        item_id         = buy_info["item_id"]
        proxy_list      = self._buy_proxy_list(assigned_proxy)
        if self.proxy_enabled and not proxy_list:
            return await self._buy_proxy_required_result(buy_info, cookie_info, start_time)
        attempt         = 0
        retry_429_count = 0
        xcsrf_retry_count = 0
        first_success   = None
        last_error      = "Unknown"
        suppress_buy_failed_webhook = False

        while True:
            attempt += 1
            total_ms = (time.time() - start_time) * 1000
            if not self.quiet_buy_logs:
                self.log_search(f"🛒 [{item_id}] Attempt #{attempt} | {total_ms:.0f}ms")

            result = await buy_item(session, buy_info, cookie_info, proxy_list, quiet=self.quiet_buy_logs)
            self._record_buy_proxy_result(result)

            total_ms = (time.time() - start_time) * 1000

            if result.get("purchased"):
                self._unmark_offsale(item_id)
                self.bought += 1

                if first_success is None:
                    first_success = {
                        "purchased": True, "item_id": item_id,
                        "serial": None, "error": None,
                        "total_ms": total_ms, "price": buy_info.get("price", 0),
                        "cookie_info": cookie_info,
                        "asset_type": buy_info["asset_type"],
                    }

                if not self.quiet_buy_logs:
                    self.log_search(f"🔄 [{item_id}] Fetching details để kiểm tra remaining...")
                updated_item = await fetch_asset_details(
                    session, item_id, self.account["search_cookie"]
                )

                if updated_item is None:
                    self.log_search(f"⚠️ [{item_id}] Không fetch được details sau mua, dừng loop.")
                    break

                remaining     = updated_item.get("Remaining", 0)
                is_for_sale   = updated_item.get("IsForSale", False)
                price         = updated_item.get("PriceInRobux", 999999) or 999999
                sale_location = updated_item.get("SaleLocation")

                if (
                    not is_for_sale
                    or remaining <= 0
                    or price > self.globalPrice
                    or bad_sale_location(sale_location)
                ):
                    if remaining <= 0:
                        self._block_terminal_item(item_id, f"Sold out (Remaining={remaining})")
                    elif bad_sale_location(sale_location):
                        self._block_terminal_item(item_id, "DevAPI/bad location")
                    self.log_search(f"🏁 [{item_id}] Sold out hoặc không còn điều kiện sau {attempt} lần mua.")
                    break

                if not self.quiet_buy_logs:
                    self.log_search(f"♻️ [{item_id}] Còn {remaining} item, tiếp tục mua lần {attempt + 1}...")
                buy_info = {
                    **buy_info,
                    "price":          price,
                    "productid_data": updated_item.get("CollectibleProductId") or buy_info["productid_data"],
                }
                continue

            elif result.get("error") == "xcsrf_expired":
                if xcsrf_retry_count >= 1:
                    last_error = "xcsrf_expired"
                    self.log_error(f"[{item_id}] xCSRF still invalid after 1 refresh")
                    break
                xcsrf_retry_count += 1
                self.log_search(f"🔑 [{item_id}] xCSRF hết hạn, refresh token và thử lại...")
                try:
                    token_data = await get_xcsrf_token(
                        cookie_info["cookie"],
                        proxies=self._active_proxies(),
                        fallback_direct=not self.proxy_enabled,
                        session=session,
                    )
                    self._remember_xcsrf_proxy(token_data)
                    cookie_info["xcsrf_token"] = token_data["xcsrf_token"]
                    cookie_info["created"]     = token_data["created"]
                    cookie_info["xcsrf_proxy_used"] = token_data.get("proxy_used")
                    cookie_info["xcsrf_proxy_key"]  = token_data.get("proxy_key")
                    proxy_list = self._buy_proxy_list(assigned_proxy)
                except ProxyRequiredError as e:
                    await self._notify_proxy_required("buy xCSRF refresh", str(e), item_id)
                    break
                except Exception as e:
                    self.log_error(f"❌ [{item_id}] Không refresh được xCSRF: {e}")
                    break
                continue

            else:
                error  = result.get("error", "Unknown")
                status = result.get("status", 0)

                _PERMANENT_ERRORS = {"QuantityExhausted", "InsufficientBalance", "InvalidSaleLocation"}
                if error in _PERMANENT_ERRORS:
                    if error in {"QuantityExhausted", "InvalidSaleLocation"}:
                        self._block_terminal_item(item_id, error)
                    else:
                        self._remove_item(item_id)
                        self._unmark_offsale(item_id)
                    self.log_search(f"🗑️ [{item_id}] {error} → xoá khỏi scan list")

                elif error == "NotForSale":
                    state_info = await self._get_item_sale_state_for_buy(session, item_id)
                    self._apply_sale_state_after_buy_error(item_id, state_info, error, status)
                    suppress_buy_failed_webhook = state_info.get("state") == "offsale"

                elif self._is_flooded_buy_error(error):
                    self._cooldown_scan_item(
                        item_id,
                        self.express_429_pause_seconds,
                        "buy flooded",
                    )
                    state_info = await self._get_item_sale_state_for_buy(session, item_id)
                    self._apply_sale_state_after_buy_error(item_id, state_info, error, status)
                    suppress_buy_failed_webhook = True

                elif status == 429:
                    retry_429_count += 1
                    if retry_429_count < 8:
                        self.log_search(f"🔄 [{item_id}] HTTP 429 retry {retry_429_count}/8...")
                        continue
                    state_info = await self._get_item_sale_state_for_buy(session, item_id)
                    self._apply_sale_state_after_buy_error(item_id, state_info, error, status)

                elif status == 403:
                    state_info = await self._get_item_sale_state_for_buy(session, item_id)
                    self._apply_sale_state_after_buy_error(item_id, state_info, error, status)

                last_error = error
                proxy_info = f" | proxy={result.get('proxy_used')}" if self.proxy_enabled else ""
                self.log_error(
                    f"❌ [{item_id}] Buy failed sau {attempt} attempt: {error}"
                    f"{proxy_info} | {total_ms:.0f}ms"
                )
                break

        if first_success is not None:
            if self.defer_post_buy_reporting:
                return first_success
            serial = await get_serial(
                first_success["cookie_info"],
                first_success["asset_type"],
                item_id,
                session=session,
            )
            first_success["serial"] = str(serial)
            self.log_purchase(item_id, serial)
            return first_success

        return first_success or {
            "purchased": False, "item_id": item_id,
            "serial": None, "error": last_error,
            "total_ms": (time.time() - start_time) * 1000,
            "price": buy_info.get("price", 0),
            "cookie_info": cookie_info,
            "suppress_buy_failed_webhook": suppress_buy_failed_webhook,
        }

    # ─── Webhook helper ──────────────────────────────────────────────────────────

    async def _handle_buy_results(self, session: aiohttp.ClientSession, item_id: int, results: list):
        """
        Gom kết quả từ tất cả buy threads và gửi MỘT webhook duy nhất.
        Gọi sau asyncio.gather(*buy_tasks).
        """
        valid = [r for r in results if isinstance(r, dict)]
        if not valid:
            return

        any_purchased = any(r.get("purchased") for r in valid)
        suppress_failed = (
            not any_purchased
            and all(r.get("suppress_buy_failed_webhook") for r in valid)
        )
        if suppress_failed:
            return

        self.autosearch_found_ids.append(item_id)

        if self.defer_post_buy_reporting:
            task = asyncio.create_task(self._post_buy_results(session, item_id, valid))
            task.add_done_callback(self._log_background_error)
            return

        await self._post_buy_results(session, item_id, valid)

    def _log_background_error(self, task: asyncio.Task):
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.log_error(f"Background report error: {e}")

    async def _post_buy_results(self, session: aiohttp.ClientSession, item_id: int, valid: list):
        any_purchased = any(r.get("purchased") for r in valid)
        if any_purchased:
            success = next(r for r in valid if r.get("purchased"))
            if not success.get("serial"):
                serial = await get_serial(
                    success["cookie_info"],
                    success.get("asset_type"),
                    item_id,
                    session=session,
                )
                success["serial"] = str(serial)
                self.log_purchase(item_id, serial)

        if not self.webhook_cfg.get("enabled"):
            return

        img = await get_image_url(item_id, session=session)
        anon = self.webhook_cfg.get("anonymous", False)

        if any_purchased:
            success = next(r for r in valid if r.get("purchased"))
            await webhook.send_buy_success(
                session, self.webhook_cfg["url"],
                item_id, success.get("serial", "?"), success["cookie_info"],
                success["total_ms"], img, anonymous=anon,
            )
        else:
            visible_failures = [
                r for r in valid
                if not r.get("suppress_buy_failed_webhook")
            ]
            first_fail = (visible_failures or valid)[0]
            await webhook.send_buy_failed(
                session, self.webhook_cfg["url"],
                item_id, first_fail["cookie_info"],
                first_fail.get("error", "Unknown"),
                first_fail["total_ms"],
                first_fail.get("price", 0), img, anonymous=anon,
            )

    # ─── Autosearch buy express ──────────────────────────────────────────────────

    async def _autosearch_buy_express(self, item_id: int, start_time: float):
        """
        Nhận item_id từ Discord → fetch details → validate → buy song song.
        """
        session    = await self._get_critical_session()
        fetch_start = time.time()

        await self._wait_for_express_fetch_slot()
        item, product_id, fetch_meta = await fetch_details_and_product_id_with_meta(
            session, item_id,
            self.account["search_cookie"],
            search_xcsrf=self.account.get("search_xcsrf_token"),
            fast=self.express_fast_fetch,
            proxies=self._active_proxies(),
            proxy_required=self.proxy_enabled,
        )
        fetch_ms = (time.time() - fetch_start) * 1000
        total_ms = (time.time() - start_time) * 1000

        if item is not None:
            self.log_search(f"[Express] [{item_id}] Fetched: {item.get('Name', '?')} | {fetch_ms:.0f}ms")

        if item is None:
            fetch_reason = format_fetch_failure(fetch_meta)
            if self._defer_express_fetch_failure(item_id, fetch_meta, fetch_reason):
                return
            self.log_error(f"❌ [{item_id}] Fetch failed: {fetch_reason} | {total_ms:.0f}ms")
            if self.webhook_cfg.get("enabled") and self._should_send_detect_error(item_id, fetch_reason):
                await webhook.send_detect_error(
                    session, self.webhook_cfg["url"],
                    item_id, f"Fetch failed: {fetch_reason}", total_ms, fetch_ms,
                )
            return

        is_for_sale    = item.get("IsForSale")
        remaining      = item.get("Remaining")
        price          = item.get("PriceInRobux") or 0
        sale_location  = item.get("SaleLocation")
        item_name      = item.get("Name", "Unknown")

        conditions = {
            "is_for_sale":    is_for_sale,
            "remaining":      remaining,
            "price":          price,
            "sale_location":  sale_location,
        }

        if bad_sale_location(sale_location) or (remaining is not None and remaining <= 0):
            reason = "DevAPI/bad location" if bad_sale_location(sale_location) else "Sold out"
            self._block_terminal_item(item_id, reason)
            self.log_search(f"🗑️ [{item_id}] {reason} → xoá khỏi scan list")
            if self.webhook_cfg.get("enabled"):
                img = await get_image_url(item_id, session=session)
                self.autosearch_found_ids.append(item_id)
                await webhook.send_not_available(
                    session, self.webhook_cfg["url"],
                    item_id, item.get("Name", "Unknown"), reason,
                    conditions, 0, 0, img,
                )
            return

        if not is_for_sale:
            added = self._add_or_promote_scan_item(item_id)
            if added:
                self.log_search(f"➕ [{item_id}] '{item_name}' chưa sale → thêm scan list")
            else:
                self.log_search(f"⏳ [{item_id}] Chưa sale – đưa lên đầu scan list")
            if self.webhook_cfg.get("enabled"):
                img = await get_image_url(item_id, session=session)
                self.autosearch_found_ids.append(item_id)
                await webhook.send_not_available(
                    session, self.webhook_cfg["url"],
                    item_id, item_name, "Not for sale yet – đang scan...",
                    conditions, total_ms, fetch_ms, img,
                )
            return

        remaining_ok = (remaining is None) or (remaining > 0)
        price_ok     = price <= self.globalPrice
        location_ok  = not bad_sale_location(sale_location)

        if not (remaining_ok and price_ok and location_ok):
            if not remaining_ok:
                reason = "Sold out"
            elif not price_ok:
                reason = f"Price {price} R$ > max {self.globalPrice} R$"
            else:
                reason = f"Sale location không hợp lệ: {sale_location}"

            self._remove_item(item_id)
            self.log_search(f"🗑️ [{item_id}] {reason} → đã xoá khỏi scan list")
            if self.webhook_cfg.get("enabled"):
                img = await get_image_url(item_id, session=session)
                self.autosearch_found_ids.append(item_id)
                await webhook.send_not_available(
                    session, self.webhook_cfg["url"],
                    item_id, item_name, reason,
                    conditions, total_ms, fetch_ms, img,
                )
            return

        buy_info = {
            "item_id":           item_id,
            "collectibleItemId": item.get("CollectibleItemId"),
            "price":             price,
            "creator":           (item.get("Creator") or {}).get("CreatorTargetId"),
            "productid_data":    product_id or item.get("CollectibleProductId"),
            "asset_type":        item.get("AssetTypeId"),
        }

        tasks = self._create_buy_tasks(session, buy_info, start_time)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        await self._handle_buy_results(session, item_id, results)

    # ─── process_discord_message ─────────────────────────────────────────────────

    async def process_discord_message(self, event: dict):
        start_time = time.time()
        try:
            item_ids = self._extract_item_ids_from_discord_event(event)
            if not item_ids:
                return
            if self.account is None:
                for item_id in item_ids:
                    self._queue_discord_item(item_id, start_time)
                return
            for item_id in item_ids:
                await self._process_discord_item(item_id, start_time)
        except Exception as e:
            self.log_error(f"❌ process_discord_message: {e}")

    # ─── Search V2 ───────────────────────────────────────────────────────────────

    async def _searchv2_one(self, session: aiohttp.ClientSession, item_id: int):
        """Fetch + validate + buy 1 item trong vòng lặp V2.

        Anti-spam: Nếu item đang trong cooldown (buy fail gần đây),
        phải verify onsale thật sự mới cho buy lại.
        """
        if self._is_terminal_blocked(item_id):
            self._remove_item(item_id)
            self._log_terminal_skip(item_id, "V2")
            return
        if self._is_scan_cooled(item_id):
            self._log_scan_cooldown_skip(item_id, "V2")
            return
        economy_pause = self._economy_pause_remaining()
        if economy_pause > 0:
            await asyncio.sleep(min(economy_pause, 0.5))
            return

        item, fetch_meta = await fetch_asset_details_with_meta(
            session, item_id, self.account["search_cookie"]
        )
        if item is None:
            if self._meta_has_status(fetch_meta, 429):
                self._record_economy_rate_limit("V2")
            self._record_scan_fetch_failure(item_id, fetch_meta, "V2")
            return
        self._reset_scan_fetch_failure(item_id)

        self.totalSearches += 1
        is_for_sale   = item.get("IsForSale")
        remaining     = item.get("Remaining")
        price         = item.get("PriceInRobux", 0) or 0
        sale_location = item.get("SaleLocation")

        location_ok   = not bad_sale_location(sale_location)

        if remaining is not None and remaining <= 0:
            self._block_terminal_item(item_id, f"Sold out (Remaining={remaining})")
            self.log_search(f"🗑️ V2 [{item_id}] Sold out (Remaining={remaining}) → xoá")
            return

        if not location_ok:
            self._block_terminal_item(item_id, "DevAPI/bad location")
            self.log_search(f"🗑️ V2 [{item_id}] Bad sale location → xoá")
            return

        if not is_for_sale:
            return

        if price > self.globalPrice:
            self._remove_item(item_id)
            self._unmark_offsale(item_id)
            return

        if self._is_verified_offsale(item_id):
            self._unmark_offsale(item_id)
            self.log_search(f"✅ [{item_id}] Item ĐÃ ONSALE lại → buy ngay!")

        buy_info = {
            "item_id":           item_id,
            "collectibleItemId": item.get("CollectibleItemId"),
            "price":             price,
            "creator":           (item.get("Creator") or {}).get("CreatorTargetId"),
            "productid_data":    item.get("CollectibleProductId"),
            "asset_type":        item.get("AssetTypeId"),
        }
        start_time = time.time()
        buy_session = await self._get_critical_session()
        tasks = self._create_buy_tasks(buy_session, buy_info, start_time)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        await self._handle_buy_results(buy_session, item_id, results)

    async def searchv2(self, worker_id: int = 0):
        session = await self._get_shared_session()
        while True:
            try:
                if not self.items:
                    self._items_event.clear()
                    await self._items_event.wait()
                    continue

                start = time.time()
                items_snapshot = list(self.items)
                newest_item = self._select_v2_item(items_snapshot, worker_id)
                if newest_item is None:
                    await asyncio.sleep(self.v2waitTime or 0.05)
                    continue
                await self._searchv2_one(session, newest_item)

                self.v2search = round(time.time() - start, 3)
                self.v2avgSpeed.append(self.v2search)

                if self.autov2waitTime:
                    recent = list(self.v2avgSpeed)
                    avg    = sum(recent) / len(recent)
                    self.v2waitTime = max((200 / 1000) - avg, 0) * self.v2threads
                
                await asyncio.sleep(self.v2waitTime)

            except Exception as e:
                logger.error(f"searchv2 error: {e}", exc_info=True)

    # ─── Search V1 ───────────────────────────────────────────────────────────────

    def _print_status(self):
        """Delegate sang display.py."""
        print_status(self)

    async def searchv1(self):
        session      = await self._get_shared_session()
        current_items = list(self.items)
        cycler        = cycle(current_items)
        while True:
            start = time.time()
            try:
                if not self.items:
                    await asyncio.sleep(1)
                    continue

                if list(self.items) != current_items:
                    current_items = list(self.items)
                    cycler        = cycle(current_items)
                raw_batch = list(dict.fromkeys(islice(cycler, 120)))
                terminal_batch = self._filter_terminal_items(raw_batch, "V1")
                if len(terminal_batch) != len(raw_batch):
                    current_items = list(self.items)
                    cycler = cycle(current_items)
                items_batch = self._filter_scan_ready_items(terminal_batch, "V1")
                if not items_batch:
                    continue
                catalog_pause = self._catalog_pause_remaining()
                if catalog_pause > 0:
                    await asyncio.sleep(min(catalog_pause, self.v1waitTime or 1.0))
                    continue

                data, batch_meta = await fetch_catalog_batch_with_meta(
                    session,
                    items_batch,
                    self.account["search_xcsrf_token"],
                    self.account["search_cookie"],
                    proxies=self._active_proxies(),
                    proxy_required=self.proxy_enabled,
                    on_proxy_error=self._record_fetch_proxy_error,
                )

                if data is None:
                    if self._meta_has_status(batch_meta, 429):
                        self._record_catalog_rate_limit("V1")
                        continue
                    for item_id in items_batch:
                        self._record_scan_fetch_failure(item_id, batch_meta, "V1")
                    continue

                if data.get("token_expired"):
                    token = await get_xcsrf_token(
                        self.account["search_cookie"],
                        proxies=self._active_proxies(),
                        fallback_direct=not self.proxy_enabled,
                        session=session,
                    )
                    self._remember_xcsrf_proxy(token)
                    self.account["search_xcsrf_token"] = token["xcsrf_token"]
                    self.account["search_created"]     = token["created"]
                    self.account["search_xcsrf_proxy_used"] = token.get("proxy_used")
                    self.account["search_xcsrf_proxy_key"]  = token.get("proxy_key")
                    continue

                buy_candidates = []
                removed_ids    = []

                for item in data.get("data", []):
                    item_id       = item.get("id")
                    if item_id is None:
                        continue
                    self._reset_scan_fetch_failure(item_id)
                    price         = item.get("price", 0) or 0
                    remaining     = item.get("unitsAvailableForConsumption")
                    sale_location = item.get("saleLocationType") or ""
                    price_status  = item.get("priceStatus", "")
                    remaining_ok  = (remaining is None) or (remaining > 0)
                    location_ok   = not bad_sale_location(sale_location)
                    off_sale      = price_status == "Off Sale"

                    if not remaining_ok:
                        self._block_terminal_item(item_id, f"Sold out (Remaining={remaining})")
                        removed_ids.append(item_id)
                        continue

                    if not location_ok:
                        self._block_terminal_item(item_id, "DevAPI/bad location")
                        removed_ids.append(item_id)
                        continue

                    if off_sale:
                        continue

                    if price > self.globalPrice:
                        removed_ids.append(item_id)
                        self.log_search(f"🗑️ V1 [{item_id}] Price {price} R$ > max {self.globalPrice} R$ → xoá")
                        continue

                    if self._is_verified_offsale(item_id):
                        self._unmark_offsale(item_id)
                        self.log_search(f"✅ V1 [{item_id}] Item ĐÃ ONSALE lại → buy!")

                    buy_candidates.append({
                        "item_id":           item_id,
                        "collectibleItemId": item.get("collectibleItemId"),
                        "price":             price,
                        "creator":           item.get("creatorTargetId"),
                        "productid_data":    item.get("collectibleProductId"),
                        "asset_type":        item.get("assetType"),
                    })

                if removed_ids:
                    for rid in removed_ids:
                        self._remove_item(rid)
                    current_items = list(self.items)
                    cycler        = cycle(current_items)

                # Bỏ candidate đã bị remove khỏi list giữa chừng (race V2/express/bot)
                buy_candidates = [c for c in buy_candidates if c["item_id"] in self.items]

                if buy_candidates:
                    cids = [c["collectibleItemId"] for c in buy_candidates if c["collectibleItemId"]]
                    product_map = await fetch_product_ids_batch(
                        session, cids,
                        self.account["search_xcsrf_token"],
                        self.account["search_cookie"],
                        proxies=self._active_proxies(),
                        proxy_required=self.proxy_enabled,
                    )
                    start_time = time.time()
                    buy_session = await self._get_critical_session()
                    for buy_info in buy_candidates:
                        cid = buy_info["collectibleItemId"]
                        if cid and cid in product_map:
                            buy_info["productid_data"] = product_map[cid] or buy_info["productid_data"]
                    buy_tasks = self._create_buy_tasks_for_candidates(
                        buy_session,
                        buy_candidates,
                        start_time,
                    )
                    if buy_tasks:
                        all_results = await asyncio.gather(*buy_tasks, return_exceptions=True)
                        item_results: dict[int, list] = {}
                        for r in all_results:
                            if isinstance(r, dict):
                                iid = r.get("item_id")
                                item_results.setdefault(iid, []).append(r)
                        for iid, irs in item_results.items():
                            await self._handle_buy_results(buy_session, iid, irs)

            except ProxyRequiredError as e:
                await self._notify_proxy_required("V1 xCSRF Roblox API", str(e))
            except Exception as e:
                self.log_error(f"V1 Error: {e}")
            finally:
                self.totalSearches += 1
                self.v1search = round(time.time() - start, 3)
                self._sweep_runtime_state()
                self._print_status()
                await asyncio.sleep(self.v1waitTime)

    # ─── Status loop ───────────────────────────────────────────────────────────────

    def _sweep_runtime_state(self):
        """Dọn state hết hạn (throttle logs > 1h, tombstone hết hạn) — gọi định kỳ."""
        now = time.monotonic()
        if self._runtime_throttle_logs:
            self._runtime_throttle_logs = {
                key: ts for key, ts in self._runtime_throttle_logs.items()
                if now - ts < 3600.0
            }
        if self._removed_until:
            self._removed_until = {
                item_id: until for item_id, until in self._removed_until.items()
                if until > now
            }

    # ─── run / start / main ────────────────────────────────────────────────────────

    async def run(self):
        """Chế độ scanner (V1 + V2 threads + auto token refresh)."""
        tasks = [self.searchv2(worker_id) for worker_id in range(self.v2threads)]
        tasks.append(self.searchv1())
        tasks.append(self.autoXToken())
        await asyncio.gather(*tasks, return_exceptions=True)

    def _log_startup_diagnostics(self):
        gw_cfg = self.config.get("gateway", {})
        self.log_search(f"[Startup] cwd={os.path.abspath(os.getcwd())}")
        self.log_search(f"[Startup] sniper_file={os.path.abspath(__file__)}")
        self.log_search(
            f"[Startup] guild={gw_cfg.get('target_guild')} "
            f"channels={gw_cfg.get('target_channels', [])} "
            f"watcher_tokens={len(self.tokens)} "
            f"proxy={'ON' if self.proxy_enabled else 'OFF'}"
        )

    async def start_gateway_tasks(self) -> list:
        """Start Discord Gateway/Bot only. No Roblox proxy is used here."""
        tasks = []

        bot_cfg = self.config.get("tokenbot_discord", {})
        if bot_cfg.get("enabled") and bot_cfg.get("token"):
            task = asyncio.create_task(startdiscord(self))
            task.add_done_callback(self._log_background_error)
            tasks.append(task)
            self.log_search("[DiscordBot] Task created")

        if self.tokens:
            self.log_search(f"[Gateway] Khoi dong {len(self.tokens)} watcher token(s)")
            for idx, token in enumerate(self.tokens, start=1):
                task = asyncio.create_task(DiscordGateway(self, token).connect())
                task.add_done_callback(self._log_background_error)
                tasks.append(task)
                self.log_search(f"[Gateway] Gateway task created #{idx}")
        else:
            self.log_search("[Gateway] Khong co watcher token - bo qua autosearch")

        return tasks

    async def _account_scanner_bootstrap(self):
        while True:
            try:
                self.account = await self.setup_accounts()
                self._account_ready.set()
                await self._drain_pending_discord_items()
                await self._prewarm_connections()
                await self._prewarm_buy_proxy()
                await self._validate_items_on_start()
                self.log_search("Scanner V1/V2 ON - cho item trong list...")
                await self.run()
                return
            except asyncio.CancelledError:
                raise
            except ProxyRequiredError as e:
                self._account_ready.clear()
                await self._notify_proxy_required("startup xCSRF", str(e))
                self.log_search("[Startup] Roblox account setup will retry in 5s")
                await asyncio.sleep(5)
            except Exception as e:
                self._account_ready.clear()
                self.log_error(f"[Startup] Roblox account setup error: {e}")
                await asyncio.sleep(5)

    async def start_account_scanner_task(self):
        task = asyncio.create_task(self._account_scanner_bootstrap())
        task.add_done_callback(self._log_background_error)
        return task

    async def start(self):
        """Start Gateway immediately; Roblox setup/scanner runs independently."""
        tasks = []
        tasks.extend(await self.start_gateway_tasks())
        tasks.append(await self.start_account_scanner_task())
        await asyncio.gather(*tasks, return_exceptions=True)

    async def cleanup(self):
        if self._shared_session and not self._shared_session.closed:
            await self._shared_session.close()
            self.log_search("✓ Closed shared session")
        if self._critical_session and not self._critical_session.closed:
            await self._critical_session.close()
            self.log_search("✓ Closed critical session")

    async def _validate_items_on_start(self):
        """Kiểm tra tất cả items pre-loaded và xoá những item đã sold out.
        Chạy 1 lần duy nhất khi khởi động để tránh scan item chết → 429."""
        if not self.items:
            return
        self.log_search(f"🔍 Validating {len(self.items)} pre-loaded items...")
        session = await self._get_shared_session()
        to_remove = []
        for item_id in list(self.items):
            try:
                item, fetch_meta = await fetch_asset_details_with_meta(
                    session, item_id, self.account["search_cookie"]
                )
                if item is None:
                    self._record_scan_fetch_failure(item_id, fetch_meta, "startup validate")
                    self.log_search(f"⚠️ [{item_id}] Không fetch được details → giữ lại")
                    continue
                self._reset_scan_fetch_failure(item_id)

                is_for_sale   = item.get("IsForSale", False)
                remaining     = item.get("Remaining")
                remaining_ok  = (remaining is None) or (remaining > 0)
                sale_location = item.get("SaleLocation")
                price         = item.get("PriceInRobux", 0) or 0

                if not remaining_ok:
                    self._block_terminal_item(item_id, f"Sold out (Remaining={remaining})")
                    to_remove.append(item_id)
                    self.log_search(f"🗑️ [{item_id}] Sold out (Remaining={remaining}) → xoá")
                elif bad_sale_location(sale_location):
                    self._block_terminal_item(item_id, "DevAPI/bad location")
                    to_remove.append(item_id)
                    self.log_search(f"🗑️ [{item_id}] Bad sale location → xoá")
                elif price > self.globalPrice and is_for_sale:
                    to_remove.append(item_id)
                    self.log_search(f"🗑️ [{item_id}] Price {price} > max {self.globalPrice} → xoá")
                else:
                    status = "ONSALE" if is_for_sale else "offsale (chờ sale)"
                    self.log_search(f"✅ [{item_id}] {status} – giữ trong list")

                await asyncio.sleep(0.1)

            except Exception as e:
                self.log_search(f"⚠️ [{item_id}] Validate error: {e} → giữ lại")

        for rid in to_remove:
            self._remove_item(rid)

        self.log_search(
            f"✅ Validation xong: {len(to_remove)} items xoá, "
            f"{len(self.items)} items còn lại"
        )

    async def main(self):
        try:
            self._log_startup_diagnostics()
            await self.start()
        finally:
            await self.cleanup()
