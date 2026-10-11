"""
src/roblox_common.py
====================
Shared constants, connection pool, và proxy helper
dùng chung cho roblox_fetch.py và roblox_buy.py.
"""

import socket
import logging
from dataclasses import dataclass
from typing import Optional, Union
from urllib.parse import unquote, urlsplit

import aiohttp

logger = logging.getLogger(__name__)

# ─── API URLs ────────────────────────────────────────────────────────────────
USER_INFO_URL     = "https://users.roblox.com/v1/users/authenticated"
ASSET_DETAILS_URL = "https://economy.roblox.com/v2/assets/{item_id}/details"
CATALOG_DETAILS   = "https://catalog.roblox.com/v1/catalog/items/details"
MARKETPLACE_ITEMS = "https://apis.roblox.com/marketplace-items/v1/items/details"
BUY_URL           = "https://apis.roblox.com/marketplace-sales/v1/item/{collectible_id}/purchase-item"
INVENTORY_URL     = "https://inventory.roblox.com/v2/users/{user_id}/inventory/{asset_type}?limit=10&sortOrder=Desc"
THUMBNAIL_URL     = "https://thumbnails.roblox.com/v1/assets?assetIds={item_id}&size=250x250&format=png"

# ─── TCP Connector ─────────────────────────────────────────────────────────────
TCP_CONNECTOR = lambda: aiohttp.TCPConnector(
    family=socket.AF_INET,
    ssl=False,
    limit=200,
    limit_per_host=50,
    ttl_dns_cache=600,
    keepalive_timeout=120,
    enable_cleanup_closed=True,
)


# ─── Proxy helper ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ParsedProxy:
    url: str
    auth: Optional[aiohttp.BasicAuth]
    masked: str
    variant: str = "normal"


class ProxyRequiredError(Exception):
    """Raised when a Roblox request must use proxy but no proxy attempt worked."""

    def __init__(self, message: str, last_error: Optional[Exception] = None):
        super().__init__(message)
        self.last_error = last_error


def _validate_scheme(scheme: str) -> str:
    scheme = (scheme or "http").lower()
    if scheme not in {"http", "https"}:
        raise ValueError("Proxy chi ho tro HTTP/HTTPS")
    return scheme


def _validate_port(port) -> int:
    try:
        port = int(port)
    except (TypeError, ValueError):
        raise ValueError("Proxy port khong hop le") from None
    if not 1 <= port <= 65535:
        raise ValueError("Proxy port khong hop le")
    return port


def _host_for_url(host: str) -> str:
    host = host.strip()
    if not host:
        raise ValueError("Proxy host khong hop le")
    if ":" in host and not (host.startswith("[") and host.endswith("]")):
        return f"[{host}]"
    return host


def _build_proxy(
    scheme: str,
    host: str,
    port,
    user: Optional[str] = None,
    password: Optional[str] = None,
    variant: str = "normal",
    hide_login: bool = False,
) -> ParsedProxy:
    scheme = _validate_scheme(scheme)
    port = _validate_port(port)
    host = _host_for_url(host)

    auth = None
    masked = f"{scheme}://{host}:{port}"
    if user is not None:
        user = unquote(user)
        password = unquote(password or "")
        auth = aiohttp.BasicAuth(user, password)
        login = "***" if hide_login else user
        suffix = f" ({variant} auth)" if variant != "normal" else ""
        masked = f"{scheme}://{login}:***@{host}:{port}{suffix}"

    return ParsedProxy(url=f"{scheme}://{host}:{port}", auth=auth, masked=masked, variant=variant)


def _split_proxy_scheme(raw: str) -> tuple[str, str]:
    scheme = "http"
    body = raw
    if "://" in raw:
        scheme, body = raw.split("://", 1)
        scheme = _validate_scheme(scheme)
    return scheme, body


def _is_four_part_proxy(raw: str) -> bool:
    scheme, body = _split_proxy_scheme(raw)
    return "@" not in body and not body.startswith("[") and body.count(":") == 3


ProxyInput = Union[str, ParsedProxy, None]


def normalize_proxy(proxy: ProxyInput) -> Optional[ParsedProxy]:
    """
    Normalize proxy HTTP/HTTPS ve dang aiohttp dung duoc.

    Ho tro:
      - ip:port / host:port
      - host:port:user:pass
      - user:pass@host:port
      - http(s)://host:port
      - http(s)://user:pass@host:port
    """
    if isinstance(proxy, ParsedProxy):
        return proxy

    if not proxy:
        return None

    raw = proxy.strip()
    if not raw:
        return None

    scheme, body = _split_proxy_scheme(raw)

    if _is_four_part_proxy(raw):
        host, port, user, password = body.split(":", 3)
        return _build_proxy(scheme, host, port, user, password)

    parsed = urlsplit(raw if "://" in raw else f"{scheme}://{body}")
    scheme = _validate_scheme(parsed.scheme)
    host = parsed.hostname
    if not host:
        raise ValueError("Proxy host khong hop le")
    try:
        port = parsed.port
    except ValueError:
        raise ValueError("Proxy port khong hop le") from None
    if port is None:
        raise ValueError("Proxy port khong hop le")

    return _build_proxy(scheme, host, port, parsed.username, parsed.password)


def proxy_candidates(proxy: ProxyInput) -> list[ParsedProxy]:
    """
    Return proxy auth attempts. For ambiguous host:port:a:b format, try
    normal a:b first, then swapped b:a if the proxy rejects authentication.
    """
    if isinstance(proxy, ParsedProxy):
        return [proxy]

    parsed = normalize_proxy(proxy)
    if parsed is None:
        return []

    raw = proxy.strip()
    if not _is_four_part_proxy(raw):
        return [parsed]

    scheme, body = _split_proxy_scheme(raw)
    host, port, user, password = body.split(":", 3)
    swapped = _build_proxy(
        scheme,
        host,
        port,
        password,
        user,
        variant="swapped",
        hide_login=True,
    )
    if parsed.auth and swapped.auth and (
        parsed.auth.login,
        parsed.auth.password,
    ) == (
        swapped.auth.login,
        swapped.auth.password,
    ):
        return [parsed]
    return [parsed, swapped]


def proxy_cache_key(proxy: ProxyInput) -> Optional[str]:
    """Stable in-memory key for health tracking. Do not log this value."""
    parsed = normalize_proxy(proxy)
    if parsed is None:
        return None
    login = parsed.auth.login if parsed.auth else ""
    password = parsed.auth.password if parsed.auth else ""
    return f"{parsed.url}|{login}|{password}|{parsed.variant}"


def mask_proxy(proxy: ProxyInput) -> str:
    """Tra ve proxy da che mat khau de log/hien thi an toan."""
    if not proxy:
        return "direct"
    try:
        parsed = normalize_proxy(proxy)
        return parsed.masked if parsed else "direct"
    except ValueError:
        raw = proxy.strip()
        prefix = ""
        body = raw
        if "://" in raw:
            scheme, body = raw.split("://", 1)
            prefix = f"{scheme}://"
        if "@" in body:
            return f"{prefix}***:***@{body.rsplit('@', 1)[1]}"
        if not body.startswith("[") and body.count(":") == 3:
            host, port, *_ = body.split(":", 3)
            return f"{prefix}{host}:{port}:***:***"
        return raw


def parse_proxy(proxy: ProxyInput):
    """
    Parse proxy string thành (url, auth).
    Hỗ trợ format: ip:port hoặc ip:port:user:pass
    """
    parsed = normalize_proxy(proxy)
    if parsed is None:
        return None, None
    return parsed.url, parsed.auth


def is_proxy_auth_error(exc: Exception) -> bool:
    return isinstance(exc, aiohttp.ClientHttpProxyError) and getattr(exc, "status", None) == 407


# ─── Sale location helper ────────────────────────────────────────────────────
_BAD_SALE_STRINGS = frozenset({"ExperiencesDevApiOnly", "AvatarAnimationImported"})
_BAD_SALE_TYPES   = frozenset({6})

def bad_sale_location(sale_location) -> bool:
    """Trả True nếu item chỉ bán qua Experience/DevAPI → không mua được."""
    if not sale_location:
        return False
    if isinstance(sale_location, dict):
        return sale_location.get("SaleLocationType") in _BAD_SALE_TYPES
    return str(sale_location) in _BAD_SALE_STRINGS
