"""
src/discord_gateway.py
======================
Discord Gateway WebSocket client (selfbot user-token).
Lắng nghe embed/content chứa link catalog Roblox → trigger sniper.
"""

import asyncio
import logging
import time
import zlib

import aiohttp
import websockets

try:
    import orjson as _json_lib
    _USE_ORJSON = True
except ImportError:
    import json as _json_lib  # type: ignore
    _USE_ORJSON = False

logger = logging.getLogger(__name__)

# ─── Discord Gateway Config ────────────────────────────────────────────────────
_GATEWAY_URL   = "wss://gateway.discord.gg/?v=10&encoding=json"
_DEFAULT_GUILD = "415246288779608064"

def _dumps(obj) -> str:
    if _USE_ORJSON:
        return _json_lib.dumps(obj).decode()
    return _json_lib.dumps(obj)


def _loads(raw):
    return _json_lib.loads(raw)


def _token_label(token: str) -> str:
    """Label ổn định, ẩn danh cho token — không lộ prefix (base64 user ID)."""
    return f"watcher#{zlib.crc32(token.encode()) % 10000:04d}"


class DiscordGateway:
    """
    Discord WebSocket client lắng nghe embed/content UGC trong nhiều kênh.
    Khi phát hiện link Roblox trong embed URL hoặc content → trigger sniper.
    """

    def __init__(self, sniper, token: str):
        self.sniper               = sniper
        self.token                = token
        self.websocket            = None
        self.session_id           = None
        self.last_seq             = None
        self.resume_url           = None
        self.heartbeat_interval   = None
        self.heartbeat_task       = None
        self.heartbeat_ack        = True
        self.missed_heartbeats    = 0
        self.connected            = False
        self.reconnect_delay      = 1
        self._max_reconnect_delay = 30
        self._max_missed_hb       = 3

        gw_cfg = sniper.config.get("gateway", {})

        raw_channels = gw_cfg.get("target_channels", [])
        if not raw_channels:
            raw_channels = ["1094291863332192376"]

        self._target_channels: frozenset = frozenset(str(c) for c in raw_channels)
        self._target_guild: str = str(gw_cfg.get("target_guild", _DEFAULT_GUILD))

        self._channel_needles: list = list(self._target_channels)

        self.stats = {
            "messages_received":  0,
            "embeds_received":    0,
            "content_detected":   0,
            "heartbeats_sent":    0,
            "reconnects":         0,
            "filtered_messages":  0,
            "target_events":      0,
            "detected_items":     0,
        }
        self._last_no_item_log: dict[str, float] = {}

        json_lib = "orjson" if _USE_ORJSON else "json (stdlib)"
        self.sniper.log_search(
            f"[Gateway] {_token_label(token)} | JSON: {json_lib} | "
            f"Channels: {list(self._target_channels)}"
        )

    def _log_target_without_item(self, channel_id: str):
        now = time.monotonic()
        last = self._last_no_item_log.get(channel_id, 0)
        if now - last < 60:
            return
        self._last_no_item_log[channel_id] = now
        self.sniper.log_search("[Gateway] Target channel event without Roblox item")

    async def connect(self):
        while True:
            try:
                uri = self.resume_url or _GATEWAY_URL
                async with websockets.connect(
                    uri,
                    max_size=2 ** 24,
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=10,
                ) as ws:
                    self.websocket = ws
                    self.connected = True
                    self.missed_heartbeats = 0
                    self.reconnect_delay = 1
                    self.stats["reconnects"] += 1

                    if self.session_id and self.last_seq is not None:
                        await self._resume()
                    else:
                        await self._identify()

                    await self._listen()

            except (
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK,
            ) as e:
                self.connected = False
                code   = getattr(e, "code",   "N/A")
                reason = getattr(e, "reason", str(e))
                self.sniper.log_error(f"[Gateway] Closed: code={code} reason={reason}")

                if code == 4004:
                    self.sniper.log_error("[Gateway] Token không hợp lệ – dừng")
                    return

                await self._cancel_heartbeat()
                jitter = (zlib.crc32(self.token.encode()) % 1000) / 1000.0
                delay = min(self.reconnect_delay, self._max_reconnect_delay) + jitter
                self.sniper.log_search(f"[Gateway] Reconnecting in {delay:.2f}s...")
                await asyncio.sleep(delay)
                self.reconnect_delay = min(self.reconnect_delay * 2, self._max_reconnect_delay)

            except Exception as e:
                self.connected = False
                self.sniper.log_error(f"[Gateway] Lỗi kết nối: {e}")
                await self._cancel_heartbeat()
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(self.reconnect_delay * 2, self._max_reconnect_delay)

    # ─── Gateway handshake ────────────────────────────────────────────────────

    async def _identify(self):
        payload = {
            "op": 2,
            "d": {
                "token":      self.token,
                "intents":    33281,
                "properties": {"os": "windows", "browser": "chrome", "device": "pc"},
                "compress":   False,
            },
        }
        await self.websocket.send(_dumps(payload))
        self.sniper.log_search(f"[Gateway] Sent IDENTIFY ({_token_label(self.token)})")

    async def _resume(self):
        payload = {
            "op": 6,
            "d": {
                "token":      self.token,
                "session_id": self.session_id,
                "seq":        self.last_seq,
            },
        }
        await self.websocket.send(_dumps(payload))
        self.sniper.log_search(f"[Gateway] Sent RESUME session={self.session_id[:8]}...")

    # ─── OP 14 Lazy Guild Loading ─────────────────────────────────────────────

    async def _lazy_guild_loading(self, guild_id: str):
        if guild_id != self._target_guild:
            return
        payload = {
            "op": 14,
            "d": {
                "guild_id":   guild_id,
                "typing":     True,
                "activities": True,
                "threads":    True,
                "presences":  True,
            }
        }
        try:
            if self.websocket and self.connected:
                await self.websocket.send(_dumps(payload))
                self.sniper.log_search(f"[Gateway] OP14 Lazy Guild Loading: {guild_id}")
        except Exception as e:
            self.sniper.log_error(f"[Gateway] OP14 lỗi: {e}")

    # ─── Heartbeat ────────────────────────────────────────────────────────────

    async def _heartbeat_loop(self):
        while True:
            try:
                await asyncio.sleep(self.heartbeat_interval / 1000)
                if not self.heartbeat_ack:
                    self.missed_heartbeats += 1
                    if self.missed_heartbeats >= self._max_missed_hb:
                        self.sniper.log_error("[Gateway] Quá nhiều heartbeat bị bỏ qua – reconnect")
                        await self.websocket.close(1008)
                        return
                else:
                    self.missed_heartbeats = 0

                payload = _dumps({"op": 1, "d": self.last_seq})
                await self.websocket.send(payload)
                self.heartbeat_ack = False
                self.stats["heartbeats_sent"] += 1
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.sniper.log_error(f"[Gateway] Heartbeat lỗi: {e}")
                break

    async def _cancel_heartbeat(self):
        if self.heartbeat_task and not self.heartbeat_task.done():
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
        self.heartbeat_task = None

    # ─── Main listen loop ─────────────────────────────────────────────────────

    async def _listen(self):
        needles = self._channel_needles  

        try:
            async for raw in self.websocket:
                try:
                    is_message_event = "MESSAGE_CREATE" in raw or "MESSAGE_UPDATE" in raw
                    if not is_message_event:
                        if '"op":11' in raw or '"op": 11' in raw:
                            self.heartbeat_ack     = True
                            self.missed_heartbeats = 0
                            continue
                        if '"op":1,' in raw or '"op": 1,' in raw:
                            await self.websocket.send(_dumps({"op": 1, "d": self.last_seq}))
                            continue
                        await self._handle_non_message_event(_loads(raw))
                        continue

                    if not any(ch in raw for ch in needles):
                        self.stats["filtered_messages"] += 1
                        continue

                    self.stats["messages_received"] += 1
                    event = _loads(raw)
                    d     = event.get("d", {})

                    if d.get("channel_id") not in self._target_channels:
                        self.stats["filtered_messages"] += 1
                        continue

                    self.stats["target_events"] += 1
                    raw_lower = raw.lower()
                    if "roblox.com/catalog" not in raw_lower and "rolimons.com/item" not in raw_lower:
                        self._log_target_without_item(str(d.get("channel_id")))
                        continue

                    if d.get("embeds") or d.get("message_snapshots"):
                        self.stats["embeds_received"] += 1
                    if d.get("content"):
                        self.stats["content_detected"] += 1
                    self.stats["detected_items"] += 1
                    asyncio.create_task(
                        self.sniper.process_discord_message(event)
                    )

                except Exception as e:
                    self.sniper.log_error(f"[Gateway] Lỗi xử lý event: {e}")

        except websockets.exceptions.ConnectionClosed as e:
            self.connected = False
            code = getattr(e, "code", "N/A")
            if code == 4004:
                self.sniper.log_error("[Gateway] Token không hợp lệ")
                return
            raise

    # ─── Non-message event handler ────────────────────────────────────────────

    async def _handle_non_message_event(self, event: dict):
        try:
            op  = event.get("op")
            seq = event.get("s")
            t   = event.get("t")

            if seq:
                self.last_seq = seq

            if op == 10:
                self.heartbeat_interval = event["d"]["heartbeat_interval"]
                resume_from_hello = event["d"].get("resume_gateway_url")
                if resume_from_hello:
                    self.resume_url = resume_from_hello
                await self._cancel_heartbeat()
                self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())

            elif op == 11:
                self.heartbeat_ack     = True
                self.missed_heartbeats = 0

            elif op == 1:
                await self.websocket.send(_dumps({"op": 1, "d": self.last_seq}))

            elif op == 7:
                self.sniper.log_search("[Gateway] OP 7 – reconnect")
                await self.websocket.close()

            elif op == 9:
                self.sniper.log_search("[Gateway] OP 9 – invalid session, re-identify")
                self.session_id = None
                self.last_seq   = None
                self.resume_url = None
                await asyncio.sleep(2)
                await self._identify()

            elif op == 0:
                if t == "READY":
                    self.session_id = event["d"]["session_id"]
                    self.resume_url = event["d"].get("resume_gateway_url", _GATEWAY_URL)
                    self.sniper.log_search(f"[Gateway] READY session={self.session_id[:8]}...")
                    channel_check = getattr(self, "_check_target_channel_access", None)
                    if channel_check:
                        asyncio.create_task(channel_check())
                    for guild in event["d"].get("guilds", []):
                        if guild.get("id") == self._target_guild:
                            await self._lazy_guild_loading(guild["id"])
                            break

                elif t == "RESUMED":
                    self.sniper.log_search("[Gateway] Session RESUMED thành công")

                elif t == "GUILD_CREATE":
                    guild_id = event["d"].get("id")
                    if guild_id:
                        await self._lazy_guild_loading(guild_id)

        except Exception as e:
            self.sniper.log_error(f"[Gateway] Lỗi _handle_non_message_event: {e}")

    # ─── Channel access check (REST) ──────────────────────────────────────────

    async def _check_target_channel_access(self):
        """Sau READY: verify từng kênh target visible + thuộc đúng guild."""
        headers = {"Authorization": self.token}
        try:
            async with aiohttp.ClientSession() as session:
                for channel_id in self._target_channels:
                    try:
                        async with session.get(
                            f"https://discord.com/api/v10/channels/{channel_id}",
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=5),
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                guild_id = data.get("guild_id")
                                if guild_id == self._target_guild:
                                    self.sniper.log_search(
                                        f"[Gateway] channel_access=ok channel={channel_id}"
                                    )
                                else:
                                    self.sniper.log_error(
                                        f"[Gateway] channel_access=wrong_guild "
                                        f"channel={channel_id} guild={guild_id}"
                                    )
                            else:
                                self.sniper.log_error(
                                    f"[Gateway] channel_access=not_visible "
                                    f"channel={channel_id} HTTP {resp.status}"
                                )
                    except Exception as e:
                        self.sniper.log_error(
                            f"[Gateway] channel_access check error "
                            f"channel={channel_id}: {type(e).__name__}"
                        )
        except Exception as e:
            self.sniper.log_error(f"[Gateway] channel access check failed: {type(e).__name__}: {e}")

    # ─── Stats & cleanup ──────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        total = self.stats["messages_received"] + self.stats["filtered_messages"]
        return {
            **self.stats,
            "connected":         self.connected,
            "missed_heartbeats": self.missed_heartbeats,
            "session_id":        (self.session_id[:8] + "...") if self.session_id else None,
            "efficiency":        f"{(self.stats['messages_received'] / total * 100):.1f}%" if total else "0%",
            "orjson":            _USE_ORJSON,
            "target_channels":   list(self._target_channels),
        }

    async def cleanup(self):
        self.connected = False
        await self._cancel_heartbeat()
        try:
            if self.websocket:
                await self.websocket.close()
        except Exception:
            pass
        self.sniper.log_search(f"[Gateway] Cleanup {_token_label(self.token)}")
