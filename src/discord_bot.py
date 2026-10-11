"""
src/discord_bot.py
==================
Discord Bot dùng discord.py với slash commands.

Slash commands:
  /status  /items  /add  /remove
  /proxy   /bought /check_proxies
"""

import asyncio
import logging
import time
from typing import Optional

import aiohttp
import aiofiles
import discord
from discord import app_commands
from discord.ext import commands

from src.roblox_common import is_proxy_auth_error, mask_proxy, proxy_candidates

logger = logging.getLogger(__name__)

_PROXY_FILE   = "proxies.txt"
_TEST_URL     = "https://users.roblox.com/v1/users/authenticated"
_TEST_TIMEOUT = 5
_CHECK_CONCUR = 10


class SniperBot(commands.Bot):

    def __init__(self, sniper, authorized_users: list[int]):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(
            command_prefix="\x00",   # prefix không bao giờ match → vô hiệu prefix
            help_command=None,       # tắt !help mặc định
            intents=intents,
        )

        self.sniper           = sniper
        self.authorized_users = set(authorized_users)

        cfg = sniper.config
        self._ugc_channel_id: Optional[str] = str(
            cfg.get("ugc_channel_id") or
            cfg.get("tokens", {}).get("ugc_channel_id") or
            "1094291863332192376"
        )

    # ══════════════════════════════════════════════════════════════════════════
    # Auth check
    # ══════════════════════════════════════════════════════════════════════════

    def _is_authorized(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id in self.authorized_users

    async def _deny(self, interaction: discord.Interaction):
        await interaction.response.send_message(
            "🚫 Bạn không có quyền dùng lệnh này.", ephemeral=True
        )

    # ══════════════════════════════════════════════════════════════════════════
    # Events
    # ══════════════════════════════════════════════════════════════════════════

    async def on_ready(self):
        # ── Sync to specific guild first (tức thì), sau đó sync global ──────
        guild_id = self.sniper.config.get("tokenbot_discord", {}).get("guild_id")
        if guild_id:
            guild_obj = discord.Object(id=int(guild_id))
            self.tree.copy_global_to(guild=guild_obj)
            await self.tree.sync(guild=guild_obj)
            self.sniper.log_search(
                f"[Bot] Slash commands synced to guild {guild_id} (tức thì)"
            )
        else:
            # Không có guild_id → sync global (chờ ~1h mới có hiệu lực)
            await self.tree.sync()
            self.sniper.log_search(
                "[Bot] Slash commands synced globally – "
                "⚠️ Thêm 'guild_id' vào tokenbot_discord trong config để sync tức thì!"
            )
        self.sniper.log_search(
            f"[Bot] Logged in as {self.user} (ID: {self.user.id})"
        )

    async def setup_hook(self):
        # Đăng ký commands trước khi bot connect
        self._register_commands()

    # ══════════════════════════════════════════════════════════════════════════
    # Slash commands
    # ══════════════════════════════════════════════════════════════════════════

    def _register_commands(self):

        @self.tree.command(name="help", description="Danh sách lệnh bot")
        async def help_cmd(interaction: discord.Interaction):
            if not self._is_authorized(interaction):
                return await self._deny(interaction)
            embed = discord.Embed(title="📖 Danh sách lệnh", color=0x5865F2)
            embed.add_field(name="/status",        value="Trạng thái sniper",               inline=False)
            embed.add_field(name="/items",         value="Xem danh sách item đang scan",     inline=False)
            embed.add_field(name="/add [id]",      value="Thêm item vào scan list",          inline=False)
            embed.add_field(name="/remove [id]",   value="Xoá item khỏi scan list",          inline=False)
            embed.add_field(name="/bought",        value="Lịch sử mua trong session",        inline=False)
            embed.add_field(name="/proxy",         value="Thông tin proxy",                  inline=False)
            embed.add_field(name="/check_proxies", value="Kiểm tra proxy trong proxies.txt", inline=False)
            await interaction.response.send_message(embed=embed, ephemeral=True)

        # ──────────────────────────────────────────────────────────────────────

        @self.tree.command(name="status", description="Xem trạng thái sniper")
        async def status(interaction: discord.Interaction):
            if not self._is_authorized(interaction):
                return await self._deny(interaction)

            s    = self.sniper
            name = s.account["buy_cookies"][0]["user_name"] if s.account else "?"
            embed = discord.Embed(title="📊 Sniper Status", color=0x5865F2)
            embed.add_field(name="👤 Account",        value=f"`{name}`",              inline=True)
            embed.add_field(name="🔎 Total Searches", value=f"`{s.totalSearches}`",   inline=True)
            embed.add_field(name="🛒 Bought",         value=f"`{s.bought}`",          inline=True)
            embed.add_field(name="📋 Items tracking", value=f"`{len(s.items)}`",      inline=True)
            embed.add_field(name="⏱ V1 / V2",        value=f"`{s.v1search}s` / `{s.v2search}s`", inline=True)
            embed.add_field(name="💰 Max price",      value=f"`{s.globalPrice} R$`",  inline=True)
            embed.add_field(name="📺 UGC Channel",    value=f"`{self._ugc_channel_id}`", inline=False)
            await interaction.response.send_message(embed=embed, ephemeral=True)

        # ──────────────────────────────────────────────────────────────────────

        @self.tree.command(name="items", description="Danh sách items đang track")
        async def items(interaction: discord.Interaction):
            if not self._is_authorized(interaction):
                return await self._deny(interaction)

            item_list = self.sniper.items
            if not item_list:
                return await interaction.response.send_message("📭 Không có item nào đang track.", ephemeral=True)

            lines  = "\n".join(
                f"`{i+1}.` [{iid}](https://www.roblox.com/catalog/{iid})"
                for i, iid in enumerate(item_list[:20])
            )
            suffix = f"\n_...và {len(item_list)-20} item nữa_" if len(item_list) > 20 else ""
            embed  = discord.Embed(
                title       = f"🧾 Items đang track ({len(item_list)})",
                description = lines + suffix,
                color       = 0x5865F2,
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)

        # ──────────────────────────────────────────────────────────────────────

        @self.tree.command(name="add", description="Thêm item vào track")
        @app_commands.describe(item_id="ID của item Roblox")
        async def add(interaction: discord.Interaction, item_id: str):
            if not self._is_authorized(interaction):
                return await self._deny(interaction)

            if not item_id.isdigit():
                return await interaction.response.send_message("❌ item_id phải là số.", ephemeral=True)

            iid = int(item_id)
            was_new = self.sniper._readd_item(iid)
            if was_new:
                await interaction.response.send_message(f"✅ Đã thêm item `{iid}` vào đầu scan list.", ephemeral=True)
            else:
                await interaction.response.send_message(f"⬆️ Item `{iid}` đã có — đã đưa lên đầu scan list.", ephemeral=True)

        # ──────────────────────────────────────────────────────────────────────

        @self.tree.command(name="remove", description="Xoá item khỏi track")
        @app_commands.describe(item_id="ID của item cần xoá")
        async def remove(interaction: discord.Interaction, item_id: str):
            if not self._is_authorized(interaction):
                return await self._deny(interaction)

            if not item_id.isdigit():
                return await interaction.response.send_message("❌ item_id phải là số.", ephemeral=True)

            iid = int(item_id)
            if iid not in self.sniper.items:
                return await interaction.response.send_message(f"⚠️ Item `{iid}` không có.", ephemeral=True)

            self.sniper._forget_item(iid)
            await interaction.response.send_message(
                f"🗑️ Đã xoá item `{iid}` (chặn re-add tự động trong 60s).", ephemeral=True
            )

        # ──────────────────────────────────────────────────────────────────────

        @self.tree.command(name="proxy", description="Thông tin proxy hiện tại")
        async def proxy(interaction: discord.Interaction):
            if not self._is_authorized(interaction):
                return await self._deny(interaction)

            s     = self.sniper
            total = len(s.proxies) if hasattr(s, "proxies") else 0
            embed = discord.Embed(title="🌐 Proxy Info", color=0x5865F2)
            embed.add_field(name="📋 Tổng proxy",     value=f"`{total}`",                                    inline=True)
            embed.add_field(name="⚙️ Proxy enabled",  value=f"`{'ON' if s.proxy_enabled else 'OFF'}`",        inline=True)
            embed.add_field(name="📁 File",           value=f"`{_PROXY_FILE}`",                              inline=True)
            await interaction.response.send_message(embed=embed, ephemeral=True)

        # ──────────────────────────────────────────────────────────────────────

        @self.tree.command(name="bought", description="Lịch sử mua trong session")
        async def bought(interaction: discord.Interaction):
            if not self._is_authorized(interaction):
                return await self._deny(interaction)

            logs  = list(self.sniper.buyLogs)
            embed = discord.Embed(
                title       = f"🛒 Tổng đã mua: {self.sniper.bought}",
                description = "\n".join(f"• {l}" for l in logs[-5:]) if logs else "_Chưa mua được item nào._",
                color       = 0x00FF7F,
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)

        # ──────────────────────────────────────────────────────────────────────

        @self.tree.command(name="check_proxies", description="Kiểm tra proxy trong proxies.txt")
        async def check_proxies(interaction: discord.Interaction):
            if not self._is_authorized(interaction):
                return await self._deny(interaction)

            await interaction.response.defer(ephemeral=True, thinking=True)

            try:
                async with aiofiles.open(_PROXY_FILE, "r", encoding="utf-8") as f:
                    lines = await f.readlines()
                proxies = [l.strip() for l in lines if l.strip()]
            except FileNotFoundError:
                return await interaction.followup.send("❌ Không tìm thấy file `proxies.txt`.")

            if not proxies:
                return await interaction.followup.send("📭 File `proxies.txt` trống.")

            semaphore = asyncio.Semaphore(_CHECK_CONCUR)

            async def test_one(p: str):
                async with semaphore:
                    start = time.perf_counter()
                    try:
                        candidates = proxy_candidates(p)
                    except ValueError:
                        return mask_proxy(p), "dead", 0.0

                    for cidx, candidate in enumerate(candidates):
                        proxy_display = candidate.masked
                        try:
                            proxy_url, proxy_auth = candidate.url, candidate.auth
                            async with aiohttp.ClientSession() as s:
                                async with s.get(
                                    _TEST_URL,
                                    proxy=proxy_url,
                                    proxy_auth=proxy_auth,
                                    timeout=aiohttp.ClientTimeout(total=_TEST_TIMEOUT),
                                    ssl=False,
                                ) as resp:
                                    ping = (time.perf_counter() - start) * 1000
                                    # Không gửi cookie → Roblox trả 401; nhận được
                                    # HTTP response nào = proxy sống & tới được Roblox.
                                    return proxy_display, "alive", ping
                        except aiohttp.ClientHttpProxyError as e:
                            if is_proxy_auth_error(e):
                                if cidx + 1 < len(candidates):
                                    continue
                                return proxy_display, "auth", 0.0
                            return proxy_display, "dead", 0.0
                        except Exception:
                            return proxy_display, "dead", 0.0

                    return mask_proxy(p), "dead", 0.0

            results = await asyncio.gather(*[test_one(p) for p in proxies], return_exceptions=True)
            alive = [
                (label, ping)
                for r in results if not isinstance(r, Exception)
                for label, state, ping in [r]
                if state == "alive"
            ]
            auth_failed = [
                label
                for r in results if not isinstance(r, Exception)
                for label, state, ping in [r]
                if state == "auth"
            ]
            dead = [
                label
                for r in results if not isinstance(r, Exception)
                for label, state, ping in [r]
                if state == "dead"
            ]

            embed = discord.Embed(
                title       = "🔍 Kết quả kiểm tra proxy",
                description = f"✅ **{len(alive)}** sống  |  ❌ **{len(dead)}** chết  |  📋 Tổng **{len(proxies)}**",
                color       = 0x5865F2,
            )
            embed.description = (
                f"Alive **{len(alive)}**  |  "
                f"Auth 407 **{len(auth_failed)}**  |  "
                f"Dead **{len(dead)}**  |  "
                f"Total **{len(proxies)}**"
            )
            if alive:
                top = "\n".join(f"`{p}` — {ping:.0f}ms" for p, ping in sorted(alive, key=lambda x: x[1])[:10])
                embed.add_field(name="⚡ Top proxy nhanh nhất", value=top, inline=False)
            if dead:
                dead_list = "\n".join(f"`{p}`" for p in dead[:10])
                if len(dead) > 10:
                    dead_list += f"\n_...và {len(dead)-10} proxy chết khác_"
                embed.add_field(name=f"💀 Proxy chết ({len(dead)})", value=dead_list, inline=False)

            if auth_failed:
                auth_list = "\n".join(f"`{p}`" for p in auth_failed[:10])
                if len(auth_failed) > 10:
                    auth_list += f"\n_...va {len(auth_failed)-10} proxy auth 407 khac_"
                embed.add_field(name=f"Proxy auth 407 ({len(auth_failed)})", value=auth_list, inline=False)

            await interaction.followup.send(embed=embed)

# ─── Entry point ──────────────────────────────────────────────────────────────

async def startdiscord(sniper):
    cfg  = sniper.config.get("tokenbot_discord", {})
    tok  = cfg.get("token", "")
    auth = cfg.get("authorized_users", [])

    if not tok:
        sniper.log_error("[Bot] Không có bot token trong config!")
        return

    bot = SniperBot(sniper, auth)
    try:
        await bot.start(tok)
    except discord.LoginFailure:
        sniper.log_error(
            "[Bot] Token không hợp lệ — kiểm tra tokenbot_discord.token trong config.json"
        )
