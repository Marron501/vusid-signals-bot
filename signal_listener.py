"""
Discord Signal Listener.
Connects to Discord, listens to #daily-signals for trade signals,
applies 70% win rate filter, executes trades, and sends DM alerts.
"""
import asyncio
import logging
import re
import time
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import discord

import config
from trade_executor import TradeExecutor
from trade_optimizer import TradeOptimizer

logger = logging.getLogger(__name__)

STATS_FILE = Path(__file__).parent / "trade_stats.json"

# Channel historical win rate (38W/6L from vusidesigner's community)
# Used as fallback when bot has less than 5 trades of its own
CHANNEL_WINS = 38
CHANNEL_TOTAL = 44
MIN_WIN_RATE = 0.70  # 70%


# ─────────────────────────────────────────────
# Win Rate Filter
# ─────────────────────────────────────────────

def get_win_rate() -> tuple:
    """
    Returns (win_rate, wins, total) based on bot's own history,
    falling back to channel's known historical rate if < 5 trades.
    """
    if STATS_FILE.exists():
        try:
            with open(STATS_FILE) as f:
                stats = json.load(f)
            total = stats.get("total_trades", 0)
            wins = stats.get("wins", 0)
            if total >= 5:
                return (wins / total, wins, total)
        except Exception:
            pass

    # Fallback: use channel's known win rate
    return (CHANNEL_WINS / CHANNEL_TOTAL, CHANNEL_WINS, CHANNEL_TOTAL)


def passes_win_rate_filter() -> tuple:
    """Returns (passed: bool, win_rate: float, wins: int, total: int)"""
    win_rate, wins, total = get_win_rate()
    return (win_rate >= MIN_WIN_RATE, win_rate, wins, total)


# ─────────────────────────────────────────────
# Signal Parser
# ─────────────────────────────────────────────

class SignalParser:
    SIDE_MAP = {
        "buy": "Buy", "long": "Buy", "bullish": "Buy",
        "sell": "Sell", "short": "Sell", "bearish": "Sell",
    }

    CLOSE_PATTERNS = [
        r"(?:close|exit|flatten)\s+all",
        r"(?:close|exit|flatten)\s+(\w+)",
        r"(?:tp|take\s*profit)\s+(?:hit|reached)\s+(\w+)",
        r"(?:sl|stop\s*loss)\s+(?:hit|triggered)\s+(\w+)",
    ]

    SIGNAL_PATTERN = re.compile(
        r"(?P<side>buy|sell|long|short)\s+"
        r"(?P<symbol>[A-Za-z]+(?:USDT)?)\s*"
        r"(?:@\s*)?(?P<entry>[\d.]+)?\s*"
        r"(?:.*?(?:sl|stop\s*loss)[:\s]*(?P<sl>[\d.]+))?\s*"
        r"(?:.*?(?:tp|take\s*profit|target)[:\s]*(?P<tp>[\d.]+))?\s*"
        r"(?:.*?(?:lev|leverage)[:\s]*(?P<leverage>[\d.]+)x?)?\s*",
        re.IGNORECASE
    )

    @classmethod
    def parse(cls, message: str):
        text = message.strip()

        for pattern in cls.CLOSE_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                groups = match.groups()
                if "all" in text.lower():
                    return {"action": "close_all"}
                symbol = groups[0] if groups else None
                if symbol:
                    symbol = symbol.upper()
                    if not symbol.endswith("USDT"):
                        symbol += "USDT"
                    return {"action": "close", "symbol": symbol}

        match = cls.SIGNAL_PATTERN.search(text)
        if not match:
            return None

        side_raw = match.group("side").lower()
        if side_raw not in cls.SIDE_MAP:
            return None

        symbol = match.group("symbol").upper()
        if not symbol.endswith("USDT"):
            symbol += "USDT"

        result = {
            "action": "open",
            "symbol": symbol,
            "side": cls.SIDE_MAP[side_raw],
            "entry": Decimal(match.group("entry")) if match.group("entry") else None,
            "stop_loss": Decimal(match.group("sl")) if match.group("sl") else None,
            "take_profit": Decimal(match.group("tp")) if match.group("tp") else None,
            "leverage": Decimal(match.group("leverage")) if match.group("leverage") else None,
        }

        if result["leverage"] is None:
            lev_match = re.search(r"(\d+)\s*x\b", text, re.IGNORECASE)
            if lev_match:
                result["leverage"] = Decimal(lev_match.group(1))

        return result


# ─────────────────────────────────────────────
# Signal Executor
# ─────────────────────────────────────────────

class SignalExecutor:
    def __init__(self):
        self.executor = TradeExecutor()
        self.optimizer = TradeOptimizer(self.executor)

    def execute_signal(self, signal: dict) -> bool:
        action = signal["action"]
        if action == "close_all":
            return self._close_all()
        elif action == "close":
            return self._close(signal["symbol"])
        elif action == "open":
            return self._open(signal)
        return False

    def _open(self, signal: dict) -> bool:
        symbol = signal["symbol"]
        side = signal["side"]
        leverage = Decimal(str(config.DEFAULT_LEVERAGE))
        equity = self.executor.get_equity()
        cost = equity * Decimal(str(config.EQUITY_FRACTION))

        logger.info(f"Sizing: equity={equity:.2f} USDT, {config.EQUITY_FRACTION*100:.0f}% = {cost:.2f} USDT, {leverage}x cross")
        logger.info(f"SIGNAL EXECUTE: {side} {symbol} | cost={cost:.2f} USDT | {leverage}x")

        success = self.executor.open_position(symbol, side, cost, leverage)
        if success:
            self.optimizer.record_trade(symbol, side, "open", cost, leverage)
            logger.info(f"OPENED: {side} {symbol}")
            if signal.get("stop_loss"):
                logger.info(f"  Stop Loss: {signal['stop_loss']}")
            if signal.get("take_profit"):
                logger.info(f"  Take Profit: {signal['take_profit']}")
        else:
            logger.error(f"FAILED to open {side} {symbol}")
        return success

    def _close(self, symbol: str) -> bool:
        positions = self.executor.get_my_positions()
        closed = False
        for key, pos in positions.items():
            if pos["symbol"] == symbol:
                success = self.executor.close_position(symbol, pos["side"])
                if success:
                    self.optimizer.record_trade(symbol, pos["side"], "close", Decimal("0"), Decimal("0"))
                    logger.info(f"CLOSED: {pos['side']} {symbol}")
                    closed = True
        if not closed:
            logger.warning(f"No open position found for {symbol}")
        return closed

    def _close_all(self) -> bool:
        positions = self.executor.get_my_positions()
        if not positions:
            logger.info("No positions to close")
            return True
        all_ok = True
        for key, pos in positions.items():
            success = self.executor.close_position(pos["symbol"], pos["side"])
            if success:
                self.optimizer.record_trade(pos["symbol"], pos["side"], "close", Decimal("0"), Decimal("0"))
                logger.info(f"CLOSED: {pos['side']} {pos['symbol']}")
            else:
                logger.error(f"FAILED to close {pos['side']} {pos['symbol']}")
                all_ok = False
        return all_ok


# ─────────────────────────────────────────────
# Discord Client
# ─────────────────────────────────────────────

class DiscordSignalClient(discord.Client):

    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        self.signal_executor = SignalExecutor()
        self.signal_channel_name = config.SIGNAL_CHANNEL
        self.owner_id = int(config.OWNER_DISCORD_ID) if config.OWNER_DISCORD_ID else None

    async def on_ready(self):
        logger.info(f"Discord connected as {self.user}")
        logger.info(f"Listening for ALL signals in #{self.signal_channel_name}")
        for guild in self.guilds:
            for channel in guild.text_channels:
                if channel.name == self.signal_channel_name:
                    logger.info(f"Found #{channel.name} in {guild.name} (id={channel.id})")

        # Send startup DM to owner
        await self._dm_owner(
            "✅ **VusiD Signals Bot is ONLINE**\n"
            f"Listening to `#{self.signal_channel_name}`\n"
            f"Win rate filter: ≥{MIN_WIN_RATE*100:.0f}%\n"
            f"Per trade: {float(config.EQUITY_FRACTION)*100:.0f}% equity | {config.DEFAULT_LEVERAGE}x cross\n"
            f"📅 Daily status report scheduled at **5:00 AM** every day"
        )

        # Start the 5am daily status task
        self.loop.create_task(self._daily_status_task())

    async def on_message(self, message: discord.Message):
        if not hasattr(message.channel, "name") or message.channel.name != self.signal_channel_name:
            return

        is_self = message.author == self.user
        logger.info(f"MESSAGE from {message.author}: {message.content[:80]}")

        signal = SignalParser.parse(message.content)

        if signal is None:
            logger.info("Not a trade signal, skipping")
            return

        if is_self:
            logger.info("Signal from self — skipping")
            return

        logger.info(f"PARSED SIGNAL: {signal}")

        if not config.AUTO_EXECUTE:
            logger.info("AUTO_EXECUTE is off — signal logged but not executed")
            return

        # ── Win Rate Filter (only for open trades) ──
        if signal["action"] == "open":
            passed, win_rate, wins, total = passes_win_rate_filter()
            win_pct = f"{win_rate*100:.1f}%"

            if not passed:
                msg = (
                    f"⚠️ **Signal SKIPPED — Win Rate Too Low**\n"
                    f"Signal: {signal['side']} {signal.get('symbol','')}\n"
                    f"Current win rate: **{win_pct}** ({wins}/{total}) — below 70% threshold\n"
                    f"Trade not executed."
                )
                logger.warning(f"Win rate {win_pct} below 70% — skipping trade")
                await self._dm_owner(msg)
                return

            logger.info(f"Win rate check passed: {win_pct} ({wins}/{total}) ✅")

            # Execute trade
            success = self.signal_executor.execute_signal(signal)

            # Build DM alert
            if success:
                executor = self.signal_executor.executor
                equity = executor.get_equity()
                mark = executor.get_mark_price(signal["symbol"])
                msg = (
                    f"🟢 **Trade Executed**\n"
                    f"────────────────────\n"
                    f"Signal: **{signal['side']} {signal['symbol']}**\n"
                    f"Entry: `{mark}`\n"
                    f"TP: `{signal.get('take_profit', 'N/A')}`\n"
                    f"SL: `{signal.get('stop_loss', 'N/A')}`\n"
                    f"Leverage: `{config.DEFAULT_LEVERAGE}x cross`\n"
                    f"Margin: `{float(config.EQUITY_FRACTION)*100:.0f}%` of equity\n"
                    f"Win Rate: `{win_pct}` ({wins}/{total} trades)\n"
                    f"Equity: `{equity:.2f} USDT`"
                )
            else:
                msg = (
                    f"🔴 **Trade FAILED**\n"
                    f"Signal: {signal['side']} {signal['symbol']}\n"
                    f"Check Railway logs for details."
                )
            await self._dm_owner(msg)

        else:
            # Close signals — execute without win rate check
            success = self.signal_executor.execute_signal(signal)
            action = signal["action"].upper()
            symbol = signal.get("symbol", "ALL")
            status = "✅ Done" if success else "❌ Failed"
            await self._dm_owner(f"🔔 **{action} {symbol}** — {status}")

    async def _daily_status_task(self):
        """Send a status DM every day at 5:00 AM (SAST = UTC+2)."""
        logger.info("Daily 5AM status task started")
        while not self.is_closed():
            now = datetime.now()
            # Calculate seconds until next 5:00 AM
            target = now.replace(hour=5, minute=0, second=0, microsecond=0)
            if now >= target:
                # Already past 5am today — schedule for tomorrow
                from datetime import timedelta
                target += timedelta(days=1)
            wait_seconds = (target - now).total_seconds()
            logger.info(f"Next status DM in {wait_seconds/3600:.1f} hours (at 05:00 AM)")
            await asyncio.sleep(wait_seconds)

            try:
                msg = await self._build_status_msg()
                await self._dm_owner(msg)
                logger.info("Daily 5AM status DM sent")
            except Exception as e:
                logger.error(f"Failed to send daily status: {e}")

            # Sleep 61 seconds to avoid firing twice in the same minute
            await asyncio.sleep(61)

    async def _build_status_msg(self) -> str:
        """Build the full status message."""
        executor = self.signal_executor.executor
        equity = executor.get_equity()
        positions = executor.get_my_positions()
        win_rate, wins, total = get_win_rate()

        pos_lines = ""
        total_pnl = Decimal("0")
        if positions:
            for key, pos in positions.items():
                pnl = pos["unrealisedPnl"]
                total_pnl += pnl
                icon = "🟢" if pnl >= 0 else "🔴"
                pnl_str = f"+{pnl:.2f}" if pnl >= 0 else f"{pnl:.2f}"
                pos_lines += (
                    f"{icon} **{pos['symbol']}** | {pos['side']} | "
                    f"Size: {pos['size']} | Entry: {pos['avgPrice']} | "
                    f"{pos['leverage']}x | PnL: `{pnl_str} USDT`\n"
                )
        else:
            pos_lines = "_No open positions_\n"

        total_pnl_str = f"+{total_pnl:.2f}" if total_pnl >= 0 else f"{total_pnl:.2f}"
        filter_icon = "✅" if win_rate >= 0.70 else "❌"
        filter_status = "PASSING" if win_rate >= 0.70 else "FAILING"
        today = datetime.now().strftime("%B %d, %Y")

        return (
            f"📊 **VusiD Signals Bot — Daily Status**\n"
            f"📅 {today} | 05:00 AM\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🤖 **Bot:** ONLINE on Railway\n"
            f"📡 **Listening:** `#daily-signals`\n"
            f"💰 **Equity:** `{equity:.2f} USDT` (LIVE)\n"
            f"📈 **Win Rate:** `{win_rate*100:.1f}%` ({wins}/{total} trades)\n"
            f"⚙️ **Per Trade:** `{float(config.EQUITY_FRACTION)*100:.0f}%` equity | `{config.DEFAULT_LEVERAGE}x` cross\n"
            f"🎯 **Win Rate Filter:** `≥70%` — {filter_icon} {filter_status}\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📂 **Open Positions ({len(positions)})**\n"
            f"{pos_lines}"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💹 **Total Unrealized PnL:** `{total_pnl_str} USDT`"
        )

    async def _dm_owner(self, message: str):
        """Send a DM to the bot owner."""
        if not self.owner_id:
            return
        try:
            user = await self.fetch_user(self.owner_id)
            await user.send(message)
            logger.info(f"DM sent to owner")
        except Exception as e:
            logger.warning(f"Could not send DM: {e}")


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────

def start_signal_listener():
    if not config.DISCORD_TOKEN:
        logger.error("DISCORD_TOKEN not set in .env")
        return

    RECONNECT_DELAYS = [5, 10, 30, 60, 120]
    attempt = 0

    while True:
        try:
            logger.info(f"Starting Discord signal listener (attempt #{attempt + 1})...")
            client = DiscordSignalClient()
            client.run(config.DISCORD_TOKEN, log_handler=None, reconnect=True)
        except discord.errors.LoginFailure:
            logger.critical("Invalid Discord token — check DISCORD_TOKEN in .env")
            break
        except Exception as e:
            delay = RECONNECT_DELAYS[min(attempt, len(RECONNECT_DELAYS) - 1)]
            logger.error(f"Discord connection failed: {e}")
            logger.info(f"Reconnecting in {delay}s...")
            time.sleep(delay)
            attempt += 1
            continue

        logger.info("Discord listener stopped cleanly.")
        break
