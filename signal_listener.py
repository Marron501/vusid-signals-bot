"""
Discord Signal Listener.
Connects to Discord, listens to #daily-signals for messages from CopyBot#8959,
parses trade signals, and executes them via TradeExecutor.
"""
import logging
import re
import time
from decimal import Decimal

import discord

import config
from trade_executor import TradeExecutor
from trade_optimizer import TradeOptimizer

logger = logging.getLogger(__name__)


class SignalParser:
    """
    Parses trade signals from CopyBot#8959 messages.

    Supports common signal formats:
        BUY BTCUSDT @ 65000 | SL 64000 | TP 67000 | LEV 10x
        SELL ETHUSDT 3500 SL 3600 TP 3200 10x
        LONG BTC 65000 leverage 10
        SHORT ETH 3500 lev 5x
        CLOSE BTCUSDT
        CLOSE ALL
        TP HIT BTCUSDT
    """

    # Patterns for signal parsing
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
        """
        Parse a signal message and return structured trade data.

        Returns:
            {
                "action": "open" | "close" | "close_all",
                "symbol": "BTCUSDT",
                "side": "Buy" | "Sell",
                "entry": Decimal or None,
                "stop_loss": Decimal or None,
                "take_profit": Decimal or None,
                "leverage": Decimal or None,
            }
            or None if message isn't a valid signal.
        """
        text = message.strip()

        # Check for close signals first
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

        # Try to parse open signal
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

        # Also check for leverage anywhere in the message (e.g. "10x" standalone)
        if result["leverage"] is None:
            lev_match = re.search(r"(\d+)\s*x\b", text, re.IGNORECASE)
            if lev_match:
                result["leverage"] = Decimal(lev_match.group(1))

        return result


class SignalExecutor:
    """Executes parsed signals via TradeExecutor."""

    def __init__(self):
        self.executor = TradeExecutor()
        self.optimizer = TradeOptimizer(self.executor)

    def execute_signal(self, signal: dict) -> bool:
        """Execute a parsed trade signal."""
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

        # Always use 5x leverage and 10% equity as configured — ignore signal's leverage
        leverage = Decimal(str(config.DEFAULT_LEVERAGE))
        equity = self.executor.get_equity()
        cost = equity * Decimal(str(config.EQUITY_FRACTION))

        logger.info(f"Sizing: equity={equity:.2f} USDT, 10% = {cost:.2f} USDT, leverage={leverage}x cross")

        # Set leverage and open
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


class DiscordSignalClient(discord.Client):
    """Discord client that listens to #daily-signals for trade signals."""

    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(intents=intents)
        self.signal_executor = SignalExecutor()
        self.signal_channel_name = config.SIGNAL_CHANNEL
        self.signal_bot_name = config.SIGNAL_BOT_NAME
        self.signal_bot_id = config.SIGNAL_BOT_ID

    async def on_ready(self):
        logger.info(f"Discord connected as {self.user}")
        logger.info(f"Listening for ALL signals in #{self.signal_channel_name}")

        # Find and log the target channel
        for guild in self.guilds:
            for channel in guild.text_channels:
                if channel.name == self.signal_channel_name:
                    logger.info(f"Found #{channel.name} in {guild.name} (id={channel.id})")

    async def on_message(self, message: discord.Message):
        # Only listen to the right channel
        if not hasattr(message.channel, "name") or message.channel.name != self.signal_channel_name:
            return

        # Skip our own messages (prevent feedback loops) UNLESS
        # we are CopyBot#8959 reading signals we posted ourselves
        # In that case, we still want to process them
        # So: only skip if the message is from us AND not a valid signal
        is_self = message.author == self.user

        logger.info(f"MESSAGE in #{self.signal_channel_name} from {message.author} (bot={message.author.bot}): {message.content}")

        # Parse the signal
        signal = SignalParser.parse(message.content)

        if signal is None:
            logger.info(f"Not a trade signal, skipping")
            return

        # If the message is from ourselves and we already executed it, skip
        if is_self:
            logger.info(f"Signal from self — skipping to prevent loop")
            return

        logger.info(f"PARSED SIGNAL: {signal}")

        # Execute if auto-execute is on
        if config.AUTO_EXECUTE:
            success = self.signal_executor.execute_signal(signal)
            status = "SUCCESS" if success else "FAILED"
            logger.info(f"Signal execution: {status}")
        else:
            logger.info(f"AUTO_EXECUTE is off — signal logged but not executed")


def start_signal_listener():
    """Start the Discord signal listener with auto-reconnect on any failure."""
    if not config.DISCORD_TOKEN:
        logger.error("DISCORD_TOKEN not set in .env — cannot connect to Discord")
        print("\n  ERROR: Set DISCORD_TOKEN in your .env file")
        print("  Get your bot token from https://discord.com/developers/applications\n")
        return

    import asyncio

    RECONNECT_DELAYS = [5, 10, 30, 60, 120]  # backoff steps in seconds

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

        # Clean exit — no reconnect needed
        logger.info("Discord listener stopped cleanly.")
        break
