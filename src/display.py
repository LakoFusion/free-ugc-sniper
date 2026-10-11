"""
src/display.py
==============
Hien thi trang thai sniper len console (ANSI colors).

Rule UI: tuyet doi KHONG dung box/khung vien.
Chi can le bang khoang trang + mau ANSI, gach phan cach ASCII don gian.
"""

import sys
import time

# -- ANSI colors (giu nguyen palette cu, khong them dep) --
_R = "\033[0m"
_W = "\033[1;97m"
_G = "\033[1;92m"
_C = "\033[1;96m"
_Y = "\033[1;93m"
_RE = "\033[1;91m"
_GR = "\033[90m"

# Gach phan cach ASCII thuan tuy -> khong bao gio vo khung
_SEP = "-" * 56

_MAX_ACTIVITY_LINES = 4
_MAX_LINE_LEN = 100


def _safe_len(value, default=0):
    try:
        return len(value)
    except Exception:
        return default


def _fmt_int(value, default=0):
    try:
        return f"{int(value):,}"
    except Exception:
        return f"{default:,}"


def _truncate(text, limit=_MAX_LINE_LEN):
    try:
        s = str(text)
    except Exception:
        s = "?"
    # Cat bot xuong dong le de khong vo layout
    s = s.replace("\r", " ").replace("\n", " ")
    if len(s) > limit:
        return s[: limit - 3] + "..."
    return s


def _collect_recent(s, limit=_MAX_ACTIVITY_LINES):
    """Gom 3-4 su kien moi nhat tu buyLogs/searchLogs/errorLogs.

    Moi deque da co prefix "[timestamp] ...", sort theo chuoi la du
    de uoc luong thu tu thoi gian. Khong bao gio raise.
    """
    candidates = []
    try:
        for entry in list(getattr(s, "buyLogs", []) or [])[-2:]:
            candidates.append((str(entry), "buy"))
    except Exception:
        pass
    try:
        for entry in list(getattr(s, "searchLogs", []) or [])[-2:]:
            candidates.append((str(entry), "info"))
    except Exception:
        pass
    try:
        for entry in list(getattr(s, "errorLogs", []) or [])[-1:]:
            candidates.append((str(entry), "err"))
    except Exception:
        pass

    # Sort theo prefix timestamp "[YYYY-MM-DD HH:MM:SS]" (so sanh chuoi la du)
    try:
        candidates.sort(key=lambda x: x[0])
    except Exception:
        pass

    recent = candidates[-limit:] if len(candidates) > limit else candidates
    return recent


def print_status(s) -> None:
    """In trang thai sniper ra console (1 lan write, chong giat).

    Nhan vao doi tuong Sniper (s). Khong raise bat ky loi hien thi nao
    de khong lam chet vong scanner V1.
    """
    try:
        interval = getattr(s, "status_refresh_seconds", 1.0)
        try:
            interval = float(interval)
        except Exception:
            interval = 1.0
        now = time.monotonic()
        last = getattr(s, "_last_status_print", 0.0)
        try:
            last = float(last)
        except Exception:
            last = 0.0
        if interval > 0 and now - last < interval:
            return
        s._last_status_print = now
    except Exception:
        pass

    try:
        W, G, C, Y, RE, GR, R = _W, _G, _C, _Y, _RE, _GR, _R

        # -- Header / status --
        account = getattr(s, "account", None)
        try:
            if account and account.get("buy_cookies"):
                first = account["buy_cookies"][0]
                name = first.get("user_name", "?")
                uid = first.get("user_id", "?")
            else:
                name, uid = ("?", "?")
        except Exception:
            name, uid = ("?", "?")

        try:
            has_gateway = bool(getattr(s, "tokens", None))
        except Exception:
            has_gateway = False
        mode = "Gateway + Scanner" if has_gateway else "Scanner"
        status = "Active" if account else "Starting..."

        # -- Network --
        try:
            proxy_enabled = bool(getattr(s, "proxy_enabled", False))
            proxies = getattr(s, "proxies", []) or []
            n_proxies = len(proxies)
        except Exception:
            proxy_enabled, n_proxies = False, 0
        if proxy_enabled and n_proxies:
            proxy_str = f"{G}{n_proxies} proxies{R}"
        elif proxy_enabled:
            proxy_str = f"{RE}enabled / empty{R}"
        else:
            proxy_str = f"{GR}off{R}"

        try:
            webhook_cfg = getattr(s, "webhook_cfg", {}) or {}
            webhook_on = bool(webhook_cfg.get("enabled"))
        except Exception:
            webhook_on = False
        wh_str = f"{G}on{R}" if webhook_on else f"{RE}off{R}"

        # -- Target / queue --
        try:
            items = list(getattr(s, "items", []) or [])
        except Exception:
            items = []
        try:
            target = str(items[0]) if items else "-"
        except Exception:
            target = "-"
        try:
            if items:
                preview = ", ".join(str(i) for i in items[:4])
                if len(items) > 4:
                    preview += f"  +{len(items) - 4} more"
            else:
                preview = "-"
        except Exception:
            preview = "-"

        # -- Metrics --
        try:
            searches = _fmt_int(getattr(s, "totalSearches", 0))
        except Exception:
            searches = "0"
        try:
            v1 = getattr(s, "v1search", 0)
            v2 = getattr(s, "v2search", 0)
            loop_str = f"{W}{v1}s{R} | {W}{v2}s{R}"
        except Exception:
            loop_str = "-"
        try:
            threads_str = (
                f"{W}V2:{getattr(s, 'v2threads', '?')}  "
                f"buy:{getattr(s, 'buy_threads', '?')}{R}"
            )
        except Exception:
            threads_str = "-"
        try:
            max_price = getattr(s, "globalPrice", "?")
            price_str = f"{W}{max_price} R${R}"
        except Exception:
            price_str = "-"
        try:
            bought = getattr(s, "bought", 0)
            bought_str = f"{G}{bought}{R}"
        except Exception:
            bought_str = "-"

        def row(label, value):
            return f"  {C}{label:<10}{R}{value}"

        lines = []
        lines.append("")
        lines.append(f"{W}LAKO UGC Sniper  {GR}v2.3.0{R}")
        lines.append(f"  {GR}Status: {R}{G if account else Y}{status}{R}"
                     f"{GR}   Mode: {R}{Y}{mode}{R}")
        lines.append("")
        lines.append(row("ACCOUNT", f"{G}{name}{GR} #{uid}{R}"))
        lines.append(row("BOUGHT", f"{bought_str} items"))
        if items:
            lines.append(row("TARGET", f"{Y}{target}{R}{GR} (V2 hot-slot){R}"))
        else:
            lines.append(row("TARGET", f"{GR}-{R}"))
        lines.append(row("QUEUE", f"{W}{len(items)} items{R}{GR}  [{preview}]{R}"))
        lines.append("")
        lines.append(f"  {C}METRICS{R}")
        lines.append(row("Searches", f"{W}{searches}{R}"))
        lines.append(row("Loop", loop_str))
        lines.append(row("Threads", threads_str))
        lines.append(row("Max price", price_str))
        lines.append(row("Network", f"Proxy: {proxy_str}{GR}  |  {R}Webhook: {wh_str}"))
        lines.append("")
        lines.append(f"  {C}RECENT ACTIVITY{R}")

        recent = _collect_recent(s)
        if not recent:
            lines.append(f"  {GR}- idle, waiting for items...{R}")
        else:
            for text, kind in recent:
                short = _truncate(text)
                if kind == "buy":
                    lines.append(f"  {G}>{R} {short}")
                elif kind == "err":
                    lines.append(f"  {RE}!{R} {short}")
                else:
                    lines.append(f"  {GR}-{R} {short}")

        lines.append("")
        lines.append(f"  {GR}[Ctrl+C to stop]{R}")

        out = "\n".join(lines) + "\n"
        # Ve lai tu dau + xoa den cuoi (it giat hon 2J tren Windows)
        sys.stdout.write("\033[H\033[J" + out)
        sys.stdout.flush()
    except Exception:
        # Tuyet doi khong de UI lam chet scanner
        pass
