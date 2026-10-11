# LAKO UGC Limited Sniper v2.3.0

[![Python](https://img.shields.io/badge/Python-3.12%2B-blue?style=flat-square&logo=python)](https://www.python.org/downloads/)
[![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey?style=flat-square)](https://github.com)
[![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=flat-square)](https://github.com)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](./LICENSE)
[![Discord](https://img.shields.io/badge/Discord-Join-5865F2?style=flat-square&logo=discord)](https://discord.gg/rxw8Hwk3qA)

An ultra-fast, asynchronous Roblox Free UGC Limited sniper with Discord Gateway listening, multi-layer scanning, and intelligent rate-limit protection.

<div align="center">
  <img src="showcase.png" alt="LAKO Sniper Terminal UI Showcase" width="800"/>
</div>

## 1. Supported Platforms

| Platform | Support | Notes |
|---|---|---|
| Windows 10 / 11 | Recommended | Fully optimized ANSI terminal UI |
| Linux / VPS (Ubuntu, Debian) | Supported | Headless mode compatible |
| macOS | Supported | Requires Python 3.12+ |

Requirements: **Python 3.12+** (Python 3.14 recommended). Dependencies auto-install on first launch via `requirements.txt`: `aiohttp`, `websockets`, `orjson`, `discord.py`, `aiofiles`.

## 2. Why LAKO Sniper?

LAKO Sniper is built to monitor, validate, and instantly purchase Roblox Free UGC Limited items within milliseconds of release.

Most free snipers poll a static list on a fixed interval. LAKO uses a triple-tier pipeline instead:

- **Sub-100ms Express trigger** the moment a catalog link appears on Discord.
- **Dedicated V2 hot-slot** looping the newest candidate every `200ms`.
- **V1 batch scanner** covering up to `120 items` per request for full-list coverage.
- **Pre-warmed TCP/TLS pools** so there is no handshake delay at drop time.
- **Dual-thread buy engine** with isolated proxy slots to maximize race wins.

Result: new drops are bought via the Express path in ~300-500ms (detect to purchase), while older tracked items are still re-checked every 1-2 seconds.

## 3. Key Features

### Triple-Tier Detection Pipeline

- **Express Trigger (Discord Gateway):** listens to `MESSAGE_CREATE / MESSAGE_UPDATE` via selfbot WebSocket. Any `roblox.com/catalog/{id}` or `rolimons.com/item/{id}` in content, embeds, buttons, or snapshots triggers an immediate fetch and buy. No waiting for scanner cycles.
- **V2 Scanner (Hot-Slot):** 1 worker focuses only on `items[0]` every `200ms` via Economy `GET` (direct, fastest). New items from Gateway or `/add` are `insert(0)` so V2 sees them on the next loop.
- **V1 Scanner (Batch Scan):** every ~1-2s, `POST` up to 120 items to Catalog API via proxy. Handles the rest of the list V2 does not watch.

### Advanced Anti-Rate-Limit Shield

- Economy `429` pauses fetches ~4s (configurable), Catalog `429` pauses V1 ~8-15s, with jitter to avoid synchronized retries.
- Per-item failure cooldown: 3 consecutive fetch failures triggers `300s -> 900s` escalating cooldown.
- Terminal block (permanent skip) for `sold-out` and `bad sale location` items to stop wasting requests.
- Detect-error webhook throttled to 5/minute to avoid Discord `429`.

### Smart Proxy Management

Health tracking with per-proxy cooldowns:

| Error type | Cooldown |
|---|---|
| Auth 407 / invalid | 60s |
| Timeout / connect | 3s |
| General failure | 60s |

- Buy threads never share the same proxy (slot reservation).
- Prefers the proxy that last fetched xCSRF successfully to keep one session IP.
- Supports `ip:port`, `ip:port:user:pass`, `user:pass@host:port`, `http(s)://...` in `proxies.txt`.
- `/check_proxies` tests liveness and ping via `users.roblox.com`.

Proxied requests: buy `POST`, V1 batch `POST`, Marketplace `POST`, Catalog fallback `POST`, xCSRF `POST /v2/logout`.
Direct requests (real IP): Economy `GET`, user info, inventory serial, thumbnail.

### Remote Discord Slash Bot

| Command | Description |
|---|---|
| `/add <id>` | Add item to top of scan list (wins over auto tombstone) |
| `/remove <id>` | Remove item + block auto re-add for 60s |
| `/status` | Account, searches, V1/V2 loop time, max price |
| `/items` | First 20 tracked items with catalog links |
| `/bought` | Session buy history |
| `/proxy` | Proxy count and on/off status |
| `/check_proxies` | Liveness + ping test for `proxies.txt` |

### Clean Minimalist Terminal UI

No-box design: whitespace alignment + ANSI colors only, never breaks on resize. Single `sys.stdout.write` per refresh for zero flicker. Always shows `TARGET (V2 hot-slot)`, queue preview, loop metrics, and last 4 recent events.

### Rich Discord Webhooks

- `Bought`: serial number, account (or Anonymous), total ms, thumbnail.
- `Failed`: error reason, price, total ms.
- `Not available`: sale conditions (for sale / remaining / price / location).
- `Detection failed` and `Proxy required` alerts with throttling.

## 4. Workflow

```text
[ Discord Gateway ] --(link detected)--> [ Express Validator ] --(on sale)--> [ Buy Engine ] --> [ Webhook ]
                                            ^                                                        ^
[ Target List ] --> [ V2 Hot-Slot 200ms ] --+                                                        |
       ^                                                                                            |
       +----- [ V1 Batch Scan 120 items ] ----------------------------------------------------------+
```

1. Gateway or `/add` pushes new ID to top of list.
2. Express fetches Economy first, falls back to Catalog + Marketplace if needed.
3. If on sale and under max price, fire parallel buy tasks.
4. If not for sale yet, keep it at hot-slot for V2/V1 to retry.
5. If sold out or bad location, terminal-block and remove.

## 5. Installation

```bash
# 1. Clone the repo
git clone https://github.com/your-username/free-ugc-sniper.git
cd free-ugc-sniper

# 2. Install dependencies (or let main.py auto-install on first run)
py -3.14 -m pip install -r requirements.txt

# 3. Configure
copy config.example.json config.json

# 4. Run
py -3.14 main.py
```

Log file: `logs/sniper_logs.txt` (rotated 10MB x 3). Console shows the live status table.

> If you have multiple Python versions installed, always launch with `py -3.14 main.py`.

## 6. Configuration

Minimal `config.json` (all tuning keys have optimized defaults, only add what you want to override):

```json
{
  "global_max_price": 1,
  "gateway": {
    "target_guild": "415246288779608064",
    "target_channels": ["1094291863332192376"]
  },
  "webhook": {
    "enabled": true,
    "url": "https://discord.com/api/webhooks/...",
    "anonymous": true
  },
  "tokens": {
    "watchers": ["YOUR_DISCORD_USER_TOKEN"]
  },
  "cookies": {
    "search_cookie": "YOUR_SEARCH_ROBLOSECURITY",
    "buy_cookie": "YOUR_BUY_ROBLOSECURITY"
  },
  "proxy": false,
  "tokenbot_discord": {
    "token": "YOUR_BOT_TOKEN",
    "enabled": true,
    "guild_id": "1403753057954824354",
    "authorized_users": [1216388407337680936]
  }
}
```

### Required Settings

| Key | Description |
|---|---|
| `global_max_price` | Max price in R$ to accept. Higher-priced items are removed from the list. Use `0` or `1` for free UGC. |
| `gateway.target_guild` | Discord guild ID containing the UGC announcement channels. |
| `gateway.target_channels` | Channel IDs to listen to. Must belong to `target_guild`. |
| `webhook.enabled` + `url` | Send result embeds to Discord webhook. `url` required if enabled. |
| `webhook.anonymous` | Hide Roblox account name in webhook (`true` / `false`). |
| `tokens.watchers` | Discord user tokens (selfbot) for Gateway listening. 1 token = 1 watcher. |
| `cookies.search_cookie` | `.ROBLOSECURITY` used for fetching and scanning. |
| `cookies.buy_cookie` | `.ROBLOSECURITY` used for buying. Use a different account from search when possible. |
| `proxy` | `true` / `false`. When `true`, proxy list is read from `proxies.txt`. |
| `tokenbot_discord` | Slash bot config: `token`, `enabled`, `guild_id` (instant command sync), `authorized_users` (Discord user IDs allowed to run commands). |

### Tuning Settings (Optional)

| Key | Default | Description |
|---|---|---|
| `items` | `[]` | Item IDs to scan on startup. |
| `antiratelimit.v1_wait_time` | `1.0` | Pause between V1 batch loops (seconds). Use `2.5` for small test lists. |
| `antiratelimit.v2_auto_wait_time` | `true` | Auto-tune V2 pause toward the 200ms/loop target. |
| `antiratelimit.v2_wait_time` | `0.5` | Fixed V2 pause (seconds). Only used when auto is off. |
| `threads.searcherv2_threads` | `1` | V2 workers. `1` is optimal: single worker always hits the hot-slot. |
| `threads.buy_threads` | `2` | Parallel buy requests per account. `2` wins races more often. |
| `performance.express_fetch_min_interval_seconds` | `0.15` | Min gap between Express fetches. Raise to `0.8` to reduce 429 on weak networks. |
| `performance.express_429_pause_seconds` | `4.0` | Economy 429 pause length. |
| `performance.catalog_429_pause_seconds` | `8.0` | Catalog 429 pause length. |
| `performance.status_refresh_seconds` | `1.0` | Console refresh rate. |

### How to Get `.ROBLOSECURITY`

1. Open Chrome, log in to Roblox.
2. Press `F12` -> `Application` -> `Cookies` -> `https://www.roblox.com`.
3. Copy the value of `.ROBLOSECURITY`. Never share it with anyone.

> Security tip: use two accounts if you can. A search alt for `search_cookie` (fetch/scan) and your main for `buy_cookie` (purchase). Never commit `config.json` with real secrets.

### Proxy File (`proxies.txt`)

One proxy per line. Lines starting with `#` are ignored. Supported formats: `ip:port`, `ip:port:user:pass`, `user:pass@host:port`, `http(s)://...`.

## 7. Project Structure

```text
free-ugc-limited-sniper/
├── main.py               # Entry point + logging setup
├── config.json           # Real config (secrets, gitignored)
├── proxies.txt           # Proxy list
├── requirements.txt
├── showcase.png          # Terminal UI showcase
├── logs/sniper_logs.txt  # Rotating log
└── src/
    ├── sniper.py         # Main orchestrator
    ├── config.py         # Config loader
    ├── xcsrf.py          # xCSRF fetch and refresh
    ├── roblox_common.py  # URLs, TCP pool, proxy parser
    ├── roblox_fetch.py   # Economy / Catalog / Marketplace fetch
    ├── roblox_buy.py     # Purchase + proxy retry
    ├── item_checker.py   # On-sale / off-sale verification
    ├── discord_gateway.py# Selfbot Gateway WebSocket
    ├── discord_bot.py    # Slash commands
    ├── webhook.py        # Discord webhook embeds
    └── display.py        # Console status table
```

## 8. Disclaimer

This tool is provided for **educational and research purposes only**. Users are solely responsible for complying with the Terms of Service of Roblox, Discord, and any third-party services. The authors assume no liability for account restrictions, bans, or losses resulting from the use of this software. Use at your own risk.

## 9. Community and Contact

- YouTube: [LAKO Fusion](https://www.youtube.com/@lakofusion2010)
- Discord: [Join our server](https://discord.gg/rxw8Hwk3qA)
- TikTok: [@lakofusion](https://www.tiktok.com/@lakofusion)

If this tool helped you, please leave a star. Bug reports and feature requests are welcome via Discord.

## 10. License

Distributed under the MIT License. See `LICENSE` for more information.
