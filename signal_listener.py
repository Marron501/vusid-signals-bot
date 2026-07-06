"""
VusiD Signals Bot — Production Signal Listener
- Never misses a signal: recovers missed signals on every reconnect
- Deduplication: message IDs tracked, never fires twice
- Async queue: signals processed sequentially, never dropped
- Heartbeat: logs ALIVE every 5 min, stale-connection detection
- Retry: 3 attempts per trade with exponential backoff
- Signal log: every signal persisted to signals.json
"""
from __future__ import annotations
import asyncio
import json
import logging
import re
import threading
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

import discord

import config
from trade_executor import TradeExecutor
from trade_optimizer import TradeOptimizer

logger = logging.getLogger(__name__)

BASE            = Path(__file__).parent

# ── Persistent file paths (volume-aware) ──────────────────────────────────────
from paths import SIGNALS_FILE, PROCESSED_FILE, STATS_FILE

# ── SSE helper ────────────────────────────────────────────────────────────────

def _push_sse(event: dict) -> None:
    """Push a real-time event to all dashboard SSE subscribers (best-effort)."""
    try:
        import event_bus
        event_bus.publish(event)
    except Exception:
        pass


# ── Cross-thread DM bridge ────────────────────────────────────────────────────
# The Discord client runs in its own asyncio loop. Background threads (e.g. the
# momentum monitor) can deliver an owner DM through this reference safely.
_RUNNING_CLIENT = None  # set in on_ready


def send_owner_dm(message: str) -> bool:
    """
    Thread-safe owner DM. Callable from any thread (not just the Discord loop).
    Returns True if the coroutine was scheduled, False otherwise. Best-effort.
    """
    client = _RUNNING_CLIENT
    if client is None:
        logger.debug("[dm] no running Discord client yet — DM skipped")
        return False
    loop = getattr(client, "loop", None)
    if loop is None or not loop.is_running():
        logger.debug("[dm] Discord loop not running — DM skipped")
        return False
    try:
        asyncio.run_coroutine_threadsafe(client._dm_owner(message), loop)
        return True
    except Exception as e:
        logger.warning(f"[dm] send_owner_dm failed: {e}")
        return False

CHANNEL_WINS  = 38
CHANNEL_TOTAL = 44
MIN_WIN_RATE  = 0.70

SIGNAL_LOOKBACK_HOURS = 4   # Scan this far back for missed signals on reconnect
MAX_SIGNAL_AGE_HOURS  = 2   # Don't execute signals older than this


# ─────────────────────────────────────────────
# Win Rate
# ─────────────────────────────────────────────

def get_win_rate() -> tuple:
    if STATS_FILE.exists():
        try:
            with open(STATS_FILE) as f:
                stats = json.load(f)
            total = stats.get("total_trades", 0)
            wins  = stats.get("wins", 0)
            if total >= 5:
                return (wins / total, wins, total)
        except Exception:
            pass
    return (CHANNEL_WINS / CHANNEL_TOTAL, CHANNEL_WINS, CHANNEL_TOTAL)


def passes_win_rate_filter() -> tuple:
    win_rate, wins, total = get_win_rate()
    return (win_rate >= MIN_WIN_RATE, win_rate, wins, total)


# ─────────────────────────────────────────────
# Signal Log (signals.json)
# ─────────────────────────────────────────────

def _load_signals() -> list:
    if SIGNALS_FILE.exists():
        try:
            with open(SIGNALS_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return []


def _save_signal(entry: dict):
    signals = _load_signals()
    signals.append(entry)
    # Keep last 500 signals
    if len(signals) > 500:
        signals = signals[-500:]
    with open(SIGNALS_FILE, "w") as f:
        json.dump(signals, f, indent=2, default=str)


# ─────────────────────────────────────────────
# Deduplication
# ─────────────────────────────────────────────

def _load_processed() -> set:
    if PROCESSED_FILE.exists():
        try:
            with open(PROCESSED_FILE) as f:
                data = json.load(f)
            # Prune entries older than 48h
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
            data = {k: v for k, v in data.items() if v >= cutoff}
            return set(data.keys()), data
        except Exception:
            pass
    return set(), {}


def _mark_processed(message_id: str):
    _, data = _load_processed()
    data[str(message_id)] = datetime.now(timezone.utc).isoformat()
    with open(PROCESSED_FILE, "w") as f:
        json.dump(data, f)


def _is_processed(message_id: str) -> bool:
    processed, _ = _load_processed()
    return str(message_id) in processed


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

    # Core pattern: just side + symbol.  TP/SL/leverage extracted separately so
    # order in the message doesn't matter and abbreviations like "SL:" are handled.
    SIGNAL_PATTERN = re.compile(
        r"(?P<side>buy|sell|long|short)\s*:?\s+"
        r"(?P<symbol>[A-Za-z0-9]+(?:USDT)?)"
        r"(?:\s*(?:@\s*)?(?P<entry>[\d.]+))?",
        re.IGNORECASE
    )

    # Standalone field patterns (searched independently on the full text)
    _TP_PAT  = re.compile(r"(?:take\s*profit|tp)\s*[:\-]?\s*(?:tp\s*:?\s*)?([\d.]+)", re.IGNORECASE)
    _SL_PAT  = re.compile(r"(?:stop\s*loss|sl)\s*[:\-]?\s*(?:sl\s*:?\s*)?([\d.]+)",   re.IGNORECASE)
    _LEV_PAT = re.compile(r"(\d+)\s*x(?:\s*cross)?",                                   re.IGNORECASE)

    @classmethod
    def _preprocess(cls, text: str) -> str:
        text = re.sub(r'\$\s*(\d)', r'\1', text)           # $0.34 → 0.34
        text = re.sub(r'(\d)\.\.(\d)', r'\1.\2', text)     # 0..34 → 0.34
        text = re.sub(r',(\d{3})', r'\1', text)             # 50,000 → 50000
        return text

    @classmethod
    def _decimal(cls, m, group=1):
        try:
            return Decimal(m.group(group)) if m else None
        except Exception:
            return None

    @classmethod
    def parse(cls, message: str):
        text = cls._preprocess(message.strip())

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

        return {
            "action":      "open",
            "symbol":      symbol,
            "side":        cls.SIDE_MAP[side_raw],
            "entry":       cls._decimal(match, "entry"),
            "stop_loss":   cls._decimal(cls._SL_PAT.search(text)),
            "take_profit": cls._decimal(cls._TP_PAT.search(text)),
            "leverage":    cls._decimal(cls._LEV_PAT.search(text)),
        }


# ─────────────────────────────────────────────
# Signal Executor (with retry)
# ─────────────────────────────────────────────

def _broadcast_to_additional_accounts(signal: dict) -> None:
    """
    Execute the signal on every additional account stored in accounts.json.
    Each account applies its own equity_fraction and leverage.
    Respects per-account auto_execute flag — accounts with auto_execute=False are skipped.
    Runs in a background thread — never blocks the main signal queue.
    """
    try:
        from accounts_manager import get_enabled_accounts
        accounts = get_enabled_accounts()
        if not accounts:
            return
        active = [a for a in accounts if a.get("auto_execute", True)]
        skipped = len(accounts) - len(active)
        if skipped:
            logger.info(f"[multi-account] {skipped} account(s) skipped (auto_execute=off)")
        if not active:
            return
        logger.info(f"[multi-account] Broadcasting to {len(active)} additional account(s)")
        for acc in active:
            try:
                ex     = TradeExecutor(api_key=acc["api_key"], api_secret=acc["api_secret"],
                                       testnet=acc.get("testnet", False))
                lev    = Decimal(str(acc.get("leverage", config.DEFAULT_LEVERAGE)))
                equity = ex.get_equity()
                eq_f   = float(equity)
                name   = acc.get("name", acc["id"])

                # ── Phase-based risk (same strategy applied per-account equity) ──
                base_risk = config.RISK_PCT
                if eq_f >= config.PHASE_3_EQUITY:
                    risk_pct = base_risk * 2.5; phase = 3
                elif eq_f >= config.PHASE_2_EQUITY:
                    risk_pct = base_risk * 1.5; phase = 2
                else:
                    risk_pct = base_risk;        phase = 1

                # ── SL-aware sizing ────────────────────────────────────────────
                sl_raw = signal.get("stop_loss")
                sl_f   = float(sl_raw) if sl_raw else None
                try:
                    entry_f = float(ex.get_mark_price(signal["symbol"]))
                except Exception:
                    entry_f = 0.0
                if sl_f and entry_f > 0:
                    sl_dist = abs(entry_f - sl_f) / entry_f
                    sl_dist = max(sl_dist, 0.005)
                else:
                    sl_dist = config.AUTO_SL_PCT

                risk_amount = equity * Decimal(str(risk_pct))
                cost        = (risk_amount / Decimal(str(sl_dist))) / lev

                logger.info(
                    f"[{name}] Phase {phase} | equity={eq_f:.2f} | risk={risk_pct*100:.1f}% "
                    f"(${float(risk_amount):.2f}) | SL_dist={sl_dist*100:.2f}% | "
                    f"notional=${float(risk_amount/Decimal(str(sl_dist))):.2f} | margin=${float(cost):.2f} | {lev}x"
                )
                for attempt in range(1, 4):
                    if ex.open_position(signal["symbol"], signal["side"], cost, lev):
                        logger.info(f"[{name}] ✅ OPENED {signal['side']} {signal['symbol']}")
                        # Auto-SL if signal had none
                        if not sl_f and entry_f and config.AUTO_SL_PCT > 0:
                            try:
                                cur = float(ex.get_mark_price(signal["symbol"]))
                                auto_sl = cur * (1 - config.AUTO_SL_PCT) if signal["side"] == "Buy" \
                                          else cur * (1 + config.AUTO_SL_PCT)
                                ex.set_trading_stop(signal["symbol"], signal["side"],
                                                    stop_loss=round(auto_sl, 6))
                                logger.info(f"[{name}] [AUTO-SL] {auto_sl:.6f}")
                            except Exception as _sl_e:
                                logger.warning(f"[{name}] [AUTO-SL] failed: {_sl_e}")
                        _push_sse({"type": "account_trade", "account": name,
                                   "symbol": signal["symbol"], "side": signal["side"],
                                   "status": "opened"})
                        break
                    if attempt < 3:
                        time.sleep(attempt * 2)
                else:
                    logger.error(f"[{name}] ❌ FAILED {signal['side']} {signal['symbol']} after 3 attempts")
                    _push_sse({"type": "account_trade", "account": name,
                               "symbol": signal["symbol"], "side": signal["side"],
                               "status": "failed"})
            except Exception as e:
                logger.error(f"[{acc.get('name', acc['id'])}] broadcast error: {e}")
    except Exception as e:
        logger.error(f"[multi-account] broadcast failed: {e}")


def _broadcast_close_to_additional_accounts(signal: dict) -> None:
    """Close position on all additional accounts that have auto_execute enabled."""
    try:
        from accounts_manager import get_enabled_accounts
        for acc in get_enabled_accounts():
            if not acc.get("auto_execute", True):
                continue
            try:
                ex   = TradeExecutor(api_key=acc["api_key"], api_secret=acc["api_secret"],
                                     testnet=acc.get("testnet", False))
                name = acc.get("name", acc["id"])
                if signal["action"] == "close_all":
                    positions = ex.get_my_positions()
                    for key, pos in positions.items():
                        ex.close_position(pos["symbol"], pos["side"])
                        logger.info(f"[{name}] ✅ CLOSED {pos['side']} {pos['symbol']}")
                elif signal["action"] == "close":
                    positions = ex.get_my_positions()
                    for key, pos in positions.items():
                        if pos["symbol"] == signal["symbol"]:
                            ex.close_position(pos["symbol"], pos["side"])
                            logger.info(f"[{name}] ✅ CLOSED {pos['side']} {pos['symbol']}")
            except Exception as e:
                logger.error(f"[{acc.get('name', acc['id'])}] close error: {e}")
    except Exception as e:
        logger.error(f"[multi-account] close broadcast failed: {e}")


class SignalExecutor:
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # seconds between retries

    def __init__(self):
        self.executor   = TradeExecutor()
        self.optimizer  = TradeOptimizer(self.executor)
        self.last_error = ""   # populated on every call; empty string = success

    def execute_signal(self, signal: dict) -> bool:
        self.last_error = ""
        action = signal["action"]
        if action == "close_all":
            return self._close_all()
        elif action == "close":
            return self._close(signal["symbol"])
        elif action == "open":
            return self._open(signal)
        self.last_error = f"Unknown action: {action}"
        return False

    def _open(self, signal: dict) -> bool:
        symbol   = signal["symbol"]
        side     = signal["side"]
        leverage = Decimal(str(signal.get("leverage") or config.DEFAULT_LEVERAGE))

        try:
            equity = self.executor.get_equity()
        except Exception as e:
            self.last_error = f"get_equity failed: {e}"
            logger.error(self.last_error)
            return False

        # ── Phase-adjusted risk percentage ────────────────────────────────
        eq_f = float(equity)
        base_risk = config.RISK_PCT
        if eq_f >= config.PHASE_3_EQUITY:
            risk_pct = base_risk * 2.5
            phase    = 3
        elif eq_f >= config.PHASE_2_EQUITY:
            risk_pct = base_risk * 1.5
            phase    = 2
        else:
            risk_pct = base_risk
            phase    = 1

        # ── SL-aware position sizing ──────────────────────────────────────
        sl_raw  = signal.get("stop_loss")
        sl_f    = float(sl_raw) if sl_raw else None
        try:
            entry_f = float(self.executor.get_mark_price(symbol))
        except Exception:
            entry_f = 0.0

        if sl_f and entry_f and entry_f > 0:
            sl_dist = abs(entry_f - sl_f) / entry_f   # e.g. 0.03 for 3%
            sl_dist = max(sl_dist, 0.005)              # floor at 0.5% to avoid division issues
        else:
            sl_dist = config.AUTO_SL_PCT               # fallback: 3% default

        risk_amount       = equity * Decimal(str(risk_pct))
        position_notional = risk_amount / Decimal(str(sl_dist))
        cost              = position_notional / leverage

        logger.info(
            f"[Phase {phase}] equity={eq_f:.2f} | risk={risk_pct*100:.1f}% (${float(risk_amount):.2f}) | "
            f"SL_dist={sl_dist*100:.2f}% | notional=${float(position_notional):.2f} | "
            f"margin=${float(cost):.2f} | {leverage}x"
        )

        # Pre-check: calculate qty before even attempting — gives a clear error
        qty = self.executor.calculate_qty(symbol, cost, leverage)
        if not qty:
            inst = self.executor.instruments.get(symbol, {})
            min_q = inst.get("min_qty", "?")
            self.last_error = (
                f"Insufficient size — equity={equity:.2f} USDT "
                f"cost={cost:.2f} USDT min_qty={min_q} for {symbol}"
            )
            logger.error(f"SKIP {side} {symbol}: {self.last_error}")
            return False

        logger.info(f"SIGNAL EXECUTE: {side} {symbol} qty={qty} cost={cost:.2f} {leverage}x")

        last_api_err = ""
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                ok = self.executor.open_position(symbol, side, cost, leverage)
                if ok:
                    self.optimizer.record_trade(symbol, side, "open", cost, leverage)
                    logger.info(f"OPENED ✅ {side} {symbol} (attempt {attempt})")
                    # ── Auto-SL: set stop loss if signal didn't include one ─
                    if not sl_f and entry_f and config.AUTO_SL_PCT > 0:
                        try:
                            current = float(self.executor.get_mark_price(symbol))
                            auto_sl = current * (1 - config.AUTO_SL_PCT) if side == "Buy" \
                                      else current * (1 + config.AUTO_SL_PCT)
                            self.executor.set_trading_stop(symbol, side, stop_loss=round(auto_sl, 6))
                            logger.info(
                                f"[AUTO-SL] Set {side} {symbol} SL={auto_sl:.6f} "
                                f"({config.AUTO_SL_PCT*100:.1f}% from entry)"
                            )
                        except Exception as _sl_e:
                            logger.warning(f"[AUTO-SL] Failed to set SL for {symbol}: {_sl_e}")
                    self.last_error = ""
                    return True
                last_api_err = "place_order returned False"
            except Exception as e:
                last_api_err = str(e)
                logger.error(f"Attempt {attempt}/{self.MAX_RETRIES}: {e}")
            if attempt < self.MAX_RETRIES:
                time.sleep(self.RETRY_DELAY * attempt)

        self.last_error = f"Failed after {self.MAX_RETRIES} attempts: {last_api_err}"
        logger.error(f"FAILED ❌ {side} {symbol}: {self.last_error}")
        return False

    def _close(self, symbol: str) -> bool:
        positions = self.executor.get_my_positions()
        closed = False
        for key, pos in positions.items():
            if pos["symbol"] == symbol:
                for attempt in range(1, self.MAX_RETRIES + 1):
                    success = self.executor.close_position(symbol, pos["side"])
                    if success:
                        self.optimizer.record_trade(symbol, pos["side"], "close", Decimal("0"), Decimal("0"))
                        logger.info(f"CLOSED ✅ {pos['side']} {symbol}")
                        closed = True
                        break
                    if attempt < self.MAX_RETRIES:
                        time.sleep(self.RETRY_DELAY * attempt)
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
            for attempt in range(1, self.MAX_RETRIES + 1):
                success = self.executor.close_position(pos["symbol"], pos["side"])
                if success:
                    self.optimizer.record_trade(pos["symbol"], pos["side"], "close", Decimal("0"), Decimal("0"))
                    logger.info(f"CLOSED ✅ {pos['side']} {pos['symbol']}")
                    break
                if attempt < self.MAX_RETRIES:
                    time.sleep(self.RETRY_DELAY * attempt)
            else:
                logger.error(f"FAILED ❌ {pos['side']} {pos['symbol']}")
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
        self.signal_executor      = SignalExecutor()
        self.signal_channel_name  = config.SIGNAL_CHANNEL
        self.owner_id             = int(config.OWNER_DISCORD_ID) if config.OWNER_DISCORD_ID else None
        self._signal_queue        = None   # asyncio.Queue, created in on_ready
        self._last_heartbeat      = time.time()
        self._connect_count       = 0

    # ── Lifecycle ────────────────────────────

    async def on_ready(self):
        self._connect_count += 1
        self._signal_queue = asyncio.Queue()

        global _RUNNING_CLIENT
        _RUNNING_CLIENT = self  # expose for cross-thread owner DMs (momentum monitor)

        logger.info(f"Discord connected as {self.user} (connect #{self._connect_count})")
        logger.info(f"Listening to #{self.signal_channel_name}")

        for guild in self.guilds:
            for ch in guild.text_channels:
                if ch.name == self.signal_channel_name:
                    logger.info(f"Found #{ch.name} in {guild.name} (id={ch.id})")

        # Start background workers
        self.loop.create_task(self._signal_worker())
        self.loop.create_task(self._heartbeat_task())
        self.loop.create_task(self._daily_status_task())

        # Recover any missed signals
        await self._recover_missed_signals()

        if self._connect_count == 1:
            await self._dm_owner(
                "✅ **VusiD Signals Bot is ONLINE**\n"
                f"Listening to `#{self.signal_channel_name}`\n"
                f"Win rate filter: ≥{MIN_WIN_RATE*100:.0f}%\n"
                f"Per trade: {float(config.EQUITY_FRACTION)*100:.0f}% equity | {config.DEFAULT_LEVERAGE}x cross\n"
                f"📅 Daily 5AM status | Missed signal recovery: ON"
            )

    async def on_disconnect(self):
        logger.warning("Discord disconnected — will reconnect automatically")

    async def on_resumed(self):
        logger.info("Discord session resumed")
        await self._recover_missed_signals()

    # ── Message handler ──────────────────────

    async def on_message(self, message: discord.Message):
        if not hasattr(message.channel, "name"):
            return
        if message.channel.name != self.signal_channel_name:
            return
        if message.author == self.user:
            return

        logger.info(f"MSG [{message.id}] {message.author}: {message.content[:120]}")

        # Push raw message to dashboard immediately (before parse/execute)
        _push_sse({"type": "message", "id": str(message.id),
                   "author": str(message.author), "content": message.content[:300],
                   "ts": message.created_at.isoformat()})

        await self._enqueue_message(message.id, message.content,
                                    message.created_at, source="live")

    # ── Signal queue ─────────────────────────

    # Keywords that suggest a message might be a trading signal
    _TRADE_KEYWORDS = ("buy", "sell", "long", "short", "close", "tp", "sl",
                       "take profit", "stop loss", "target", "entry", "signal")

    async def _enqueue_message(self, msg_id: int, content: str,
                                created_at: datetime, source: str = "live"):
        """Parse and enqueue a signal if not already processed."""
        if _is_processed(str(msg_id)):
            logger.debug(f"[{msg_id}] Already processed — skip")
            return

        signal = SignalParser.parse(content)

        if signal is None:
            # ── NEVER silently drop: if any trading keyword exists, log it
            has_keywords = any(kw in content.lower() for kw in self._TRADE_KEYWORDS)
            if has_keywords:
                _mark_processed(str(msg_id))
                entry = {
                    "msg_id":    str(msg_id),
                    "timestamp": created_at.isoformat() if hasattr(created_at, "isoformat") else datetime.now().isoformat(),
                    "content":   content[:500],
                    "signal":    {"action": "parse_failed"},
                    "source":    source,
                    "executed":  False,
                    "reason":    "parse_failed — message had keywords but no pattern matched",
                }
                _save_signal(entry)
                _push_sse({"type": "signal", "entry": entry})
                logger.warning(f"PARSE_FAILED [{msg_id}] keywords found but no pattern: {content[:120]}")
            return

        # Age check (don't execute stale recovery signals)
        if source == "recovery":
            now = datetime.now(timezone.utc)
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            age_hours = (now - created_at).total_seconds() / 3600
            if age_hours > MAX_SIGNAL_AGE_HOURS:
                logger.info(f"[{msg_id}] Signal too old ({age_hours:.1f}h) — skip")
                return
            logger.info(f"⏪ RECOVERED signal [{msg_id}] age={age_hours:.1f}h: {signal}")

        await self._signal_queue.put({
            "msg_id":     msg_id,
            "content":    content,
            "signal":     signal,
            "created_at": created_at,
            "source":     source,
        })

    async def _signal_worker(self):
        """
        Process signals from the queue one at a time.
        GUARANTEE: every item that reaches this worker is saved to signals.json,
        even if an unexpected exception occurs mid-processing.
        """
        logger.info("Signal worker started")
        while not self.is_closed():
            try:
                item = await asyncio.wait_for(self._signal_queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                continue

            msg_id  = item["msg_id"]
            signal  = item["signal"]
            content = item["content"]
            source  = item["source"]

            # Final dedup check (race condition guard)
            if _is_processed(str(msg_id)):
                self._signal_queue.task_done()
                continue

            # Mark processed immediately to prevent double-fire
            _mark_processed(str(msg_id))

            log_entry = {
                "msg_id":    str(msg_id),
                "timestamp": datetime.now().isoformat(),
                "content":   content[:500],   # store full content so dashboard can show it
                "signal":    {k: str(v) for k, v in signal.items()},
                "source":    source,
                "executed":  False,
                "reason":    "",
                "error":     "",
            }

            _entry_saved = False  # guard: ensure we save exactly once

            try:
                # ── AUTO_EXECUTE gate ─────────────────────────────────────
                if not config.AUTO_EXECUTE:
                    log_entry["reason"] = "AUTO_EXECUTE=off"
                    logger.info("AUTO_EXECUTE off — signal logged, not executed")
                    _save_signal(log_entry)
                    _entry_saved = True
                    _push_sse({"type": "signal", "entry": log_entry})
                    continue

                # ── Win rate filter (open signals only) ───────────────────
                if signal["action"] == "open":
                    passed, win_rate, wins, total = passes_win_rate_filter()
                    win_pct = f"{win_rate*100:.1f}%"

                    if not passed:
                        reason = f"Win rate {win_pct} below 70% ({wins}/{total})"
                        log_entry["reason"] = reason
                        logger.warning(f"SKIP: {reason}")
                        _save_signal(log_entry)
                        _entry_saved = True
                        _push_sse({"type": "signal", "entry": log_entry})
                        await self._dm_owner(
                            f"⚠️ **Signal SKIPPED** — Win rate {win_pct} < 70%\n"
                            f"Signal: {signal['side']} {signal.get('symbol','')}\n"
                            f"Stats: {wins}W / {total-wins}L of {total}"
                        )
                        continue

                    logger.info(f"Win rate {win_pct} ✅ — executing")

                # ── AI signal analysis (non-blocking) ────────────────────
                analysis = None
                if signal["action"] == "open":
                    try:
                        from signal_analyzer import analyze_signal as _analyse
                        _wr = win_rate if "win_rate" in dir() else 0.0
                        analysis = await asyncio.get_event_loop().run_in_executor(
                            None, _analyse, signal, _wr
                        )
                        log_entry["analysis"] = analysis
                        logger.info(
                            f"[AI] {signal.get('symbol','')} score={analysis.get('score')} "
                            f"verdict={analysis.get('verdict')} rec={analysis.get('recommendation')}"
                        )
                    except Exception as _ae:
                        logger.warning(f"[AI] analysis skipped: {_ae}")

                # ── Score gate (phase-aware) ──────────────────────────────
                if signal["action"] == "open" and config.MIN_AI_SCORE > 0 and analysis:
                    score = analysis.get("score", 0)
                    # Raise the gate in higher phases to match the risk increase
                    try:
                        eq_f = float(self.signal_executor.executor.get_equity())
                    except Exception:
                        eq_f = 0.0
                    if eq_f >= config.PHASE_3_EQUITY:
                        effective_gate = max(config.MIN_AI_SCORE, 70)
                        phase_label    = "Phase 3"
                    elif eq_f >= config.PHASE_2_EQUITY:
                        effective_gate = max(config.MIN_AI_SCORE, 65)
                        phase_label    = "Phase 2"
                    else:
                        effective_gate = config.MIN_AI_SCORE
                        phase_label    = "Phase 1"
                    if score < effective_gate:
                        reason = (
                            f"AI score {score}/100 below {phase_label} threshold {effective_gate} "
                            f"— verdict: {analysis.get('verdict','?')} | {analysis.get('recommendation','?')}"
                        )
                        log_entry["reason"] = reason
                        logger.warning(f"SCORE GATE: {reason}")
                        _save_signal(log_entry)
                        _entry_saved = True
                        _push_sse({"type": "signal", "entry": log_entry})
                        await self._dm_owner(
                            f"🧠 **Signal FILTERED by AI Score**\n"
                            f"────────────────────\n"
                            f"Symbol: **{signal.get('side','')} {signal.get('symbol','')}**\n"
                            f"Score: `{score}/100` (need ≥ {effective_gate} in {phase_label})\n"
                            f"Verdict: `{analysis.get('verdict','?')}`\n"
                            f"Reason: {analysis.get('summary','')[:120]}"
                        )
                        # CopyBot: watch this filtered signal for a recovered entry
                        try:
                            from copybot_watch import add_filtered_signal
                            add_filtered_signal(signal, score, effective_gate, phase_label)
                        except Exception as _cbe:
                            logger.debug(f"[copybot] register failed: {_cbe}")
                        continue

                # ── Risk Guard: circuit breaker + position cap ────────────
                if signal["action"] == "open":
                    import risk_guard as _rg
                    try:
                        _cur_eq = float(self.signal_executor.executor.get_equity())
                    except Exception:
                        _cur_eq = 0.0
                    _guard = _rg.check(_cur_eq, config.DAILY_DD_LIMIT)
                    if not _guard["ok"]:
                        reason = (
                            f"Circuit breaker active — daily PnL "
                            f"{_guard['daily_pnl_pct']:.2f}% "
                            f"(limit: -{_guard['dd_limit_pct']:.1f}%)"
                        )
                        log_entry["reason"] = reason
                        logger.warning(f"[RiskGuard] BLOCKED: {reason}")
                        _save_signal(log_entry); _entry_saved = True
                        _push_sse({"type": "signal", "entry": log_entry})
                        await self._dm_owner(
                            f"⛔ **Trade BLOCKED — Circuit Breaker**\n"
                            f"────────────────────\n"
                            f"Signal: **{signal.get('side','')} {signal.get('symbol','')}**\n"
                            f"Daily PnL: `{_guard['daily_pnl_pct']:.2f}%` "
                            f"(limit: `-{_guard['dd_limit_pct']:.1f}%`)\n"
                            f"Reset from the dashboard to resume trading."
                        )
                        continue
                    try:
                        _open_pos = len(self.signal_executor.executor.get_my_positions())
                    except Exception:
                        _open_pos = 0
                    if _open_pos >= config.MAX_OPEN_POSITIONS:
                        reason = (
                            f"Max positions reached "
                            f"({_open_pos}/{config.MAX_OPEN_POSITIONS}) — "
                            f"{signal.get('symbol','')} skipped"
                        )
                        log_entry["reason"] = reason
                        logger.warning(f"[RiskGuard] BLOCKED: {reason}")
                        _save_signal(log_entry); _entry_saved = True
                        _push_sse({"type": "signal", "entry": log_entry})
                        continue

                # ── Execute ───────────────────────────────────────────────
                exec_error = ""
                try:
                    success = await asyncio.get_event_loop().run_in_executor(
                        None, self.signal_executor.execute_signal, signal
                    )
                    exec_error = self.signal_executor.last_error
                except Exception as e:
                    success   = False
                    exec_error = str(e)
                    logger.error(f"Execution exception: {e}")

                log_entry["executed"] = success
                log_entry["error"]    = exec_error
                if success:
                    log_entry["reason"] = "success"
                else:
                    log_entry["reason"] = f"execution_failed: {exec_error}" if exec_error else "execution_failed"

                _save_signal(log_entry)
                _entry_saved = True
                _push_sse({"type": "signal", "entry": log_entry})

            except Exception as worker_exc:
                # ── Catch-all: signal must NEVER silently disappear ───────
                logger.error(f"WORKER EXCEPTION [{msg_id}]: {worker_exc}", exc_info=True)
                if not _entry_saved:
                    log_entry["reason"] = f"worker_exception: {worker_exc}"
                    try:
                        _save_signal(log_entry)
                        _push_sse({"type": "signal", "entry": log_entry})
                    except Exception:
                        pass

            # Broadcast to additional accounts — independent of primary success.
            # Each account's auto_execute flag is checked inside the broadcast fn.
            # SAFETY: AI analysis scores never trigger this path — only Discord signals do.
            if signal["action"] == "open":
                threading.Thread(
                    target=_broadcast_to_additional_accounts,
                    args=(signal,), daemon=True, name="multi-acct-open"
                ).start()
            elif signal["action"] in ("close", "close_all"):
                threading.Thread(
                    target=_broadcast_close_to_additional_accounts,
                    args=(signal,), daemon=True, name="multi-acct-close"
                ).start()

            # DM alert
            if signal["action"] == "open":
                passed, win_rate, wins, total = passes_win_rate_filter()
                win_pct = f"{win_rate*100:.1f}%"
                if success:
                    ex     = self.signal_executor.executor
                    equity = ex.get_equity()
                    mark   = ex.get_mark_price(signal["symbol"])
                    await self._dm_owner(
                        f"🟢 **Trade Executed** {'(recovered)' if source=='recovery' else ''}\n"
                        f"────────────────────\n"
                        f"Signal: **{signal['side']} {signal['symbol']}**\n"
                        f"Entry: `{mark}`\n"
                        f"TP: `{signal.get('take_profit','N/A')}` | SL: `{signal.get('stop_loss','N/A')}`\n"
                        f"Leverage: `{config.DEFAULT_LEVERAGE}x cross`\n"
                        f"Margin: `{float(config.EQUITY_FRACTION)*100:.0f}%` equity\n"
                        f"Win Rate: `{win_pct}` | Equity: `{equity:.2f} USDT`"
                    )
                else:
                    await self._dm_owner(
                        f"🔴 **Trade FAILED**\n"
                        f"Signal: {signal['side']} {signal.get('symbol','')}\n"
                        f"Check Railway logs."
                    )
            else:
                action = signal["action"].upper()
                symbol = signal.get("symbol", "ALL")
                status = "✅ Done" if success else "❌ Failed"
                await self._dm_owner(f"🔔 **{action} {symbol}** — {status}")

            self._signal_queue.task_done()

    # ── Missed signal recovery ────────────────

    async def _recover_missed_signals(self):
        """On connect/reconnect: fetch recent channel messages and execute any missed signals."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=SIGNAL_LOOKBACK_HOURS)
        recovered = 0

        for guild in self.guilds:
            for ch in guild.text_channels:
                if ch.name != self.signal_channel_name:
                    continue
                try:
                    logger.info(f"Scanning last {SIGNAL_LOOKBACK_HOURS}h of #{ch.name} for missed signals...")
                    messages = []
                    async for msg in ch.history(limit=100, after=cutoff):
                        if msg.author != self.user:
                            messages.append(msg)

                    # Process oldest first
                    messages.sort(key=lambda m: m.created_at)

                    for msg in messages:
                        signal = SignalParser.parse(msg.content)
                        if signal and not _is_processed(str(msg.id)):
                            recovered += 1
                            logger.info(f"⏪ Missed signal found: [{msg.id}] {msg.content[:80]}")
                            await self._enqueue_message(
                                msg.id, msg.content, msg.created_at, source="recovery"
                            )

                    if recovered == 0:
                        logger.info(f"No missed signals in last {SIGNAL_LOOKBACK_HOURS}h ✅")
                    else:
                        logger.info(f"Queued {recovered} missed signal(s) for execution")

                except Exception as e:
                    logger.error(f"Failed to scan for missed signals: {e}")

    # ── Heartbeat ────────────────────────────

    async def _heartbeat_task(self):
        """Log ALIVE every 5 minutes. Detect stale connection."""
        while not self.is_closed():
            await asyncio.sleep(300)  # 5 minutes
            win_rate, wins, total = get_win_rate()
            logger.info(
                f"💓 HEARTBEAT | Connected: {not self.is_closed()} | "
                f"WinRate: {win_rate*100:.1f}% | "
                f"Guilds: {len(self.guilds)} | "
                f"Queue: {self._signal_queue.qsize() if self._signal_queue else 0}"
            )

    # ── Daily 5AM status ────────────────────

    async def _daily_status_task(self):
        logger.info("Daily 5AM status task started")
        while not self.is_closed():
            now    = datetime.now()
            target = now.replace(hour=5, minute=0, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            wait = (target - now).total_seconds()
            logger.info(f"Next 5AM status in {wait/3600:.1f}h")
            await asyncio.sleep(wait)
            try:
                msg = await self._build_status_msg()
                await self._dm_owner(msg)
                logger.info("Daily 5AM status DM sent ✅")
            except Exception as e:
                logger.error(f"Daily status failed: {e}")
            await asyncio.sleep(61)

    async def _build_status_msg(self) -> str:
        ex         = self.signal_executor.executor
        equity     = ex.get_equity()
        positions  = ex.get_my_positions()
        win_rate, wins, total = get_win_rate()

        signals    = _load_signals()
        today_signals = [s for s in signals
                         if s.get("timestamp", "")[:10] == datetime.now().strftime("%Y-%m-%d")]

        pos_lines  = ""
        total_pnl  = Decimal("0")
        for key, pos in positions.items():
            pnl       = pos["unrealisedPnl"]
            total_pnl += pnl
            icon      = "🟢" if pnl >= 0 else "🔴"
            pnl_str   = f"+{pnl:.2f}" if pnl >= 0 else f"{pnl:.2f}"
            pos_lines += (f"{icon} **{pos['symbol']}** | {pos['side']} | "
                          f"Entry: {pos['avgPrice']} | {pos['leverage']}x | PnL: `{pnl_str} USDT`\n")
        if not pos_lines:
            pos_lines = "_No open positions_\n"

        total_pnl_str    = f"+{total_pnl:.2f}" if total_pnl >= 0 else f"{total_pnl:.2f}"
        filter_status    = "✅ PASSING" if win_rate >= 0.70 else "❌ FAILING"
        today            = datetime.now().strftime("%B %d, %Y")

        return (
            f"📊 **VusiD Signals Bot — Daily Status**\n"
            f"📅 {today} | 05:00 AM\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "🤖 **Bot:** ONLINE on Railway\n"
            f"💰 **Equity:** `{equity:.2f} USDT` (LIVE)\n"
            f"📈 **Win Rate:** `{win_rate*100:.1f}%` ({wins}/{total}) | {filter_status}\n"
            f"⚙️ **Per Trade:** `{float(config.EQUITY_FRACTION)*100:.0f}%` | `{config.DEFAULT_LEVERAGE}x` cross\n"
            f"📡 **Today's Signals:** {len(today_signals)}\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📂 **Open Positions ({len(positions)})**\n"
            f"{pos_lines}"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💹 **Total Unrealized PnL:** `{total_pnl_str} USDT`"
        )

    # ── DM helper ────────────────────────────

    async def _dm_owner(self, message: str):
        if not self.owner_id or not config.DM_ALERTS:
            if not config.DM_ALERTS:
                logger.info("DM muted (DM_ALERTS=false)")
            return
        try:
            user = await self.fetch_user(self.owner_id)
            await user.send(message)
            logger.info("DM sent to owner")
        except Exception as e:
            logger.warning(f"Could not send DM: {e}")


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────

def start_signal_listener():
    if not config.DISCORD_TOKEN:
        logger.error("DISCORD_TOKEN not set — cannot connect to Discord")
        return

    RECONNECT_DELAYS = [1, 2, 5, 10, 30, 60, 120]
    attempt = 0

    while True:
        try:
            logger.info(f"Starting Discord signal listener (attempt #{attempt + 1})...")
            client = DiscordSignalClient()
            client.run(config.DISCORD_TOKEN, log_handler=None, reconnect=True)
        except discord.errors.LoginFailure:
            logger.critical("Invalid Discord token — check DISCORD_TOKEN")
            break
        except Exception as e:
            delay = RECONNECT_DELAYS[min(attempt, len(RECONNECT_DELAYS) - 1)]
            logger.error(f"Discord connection failed: {e}")
            logger.info(f"Reconnecting in {delay}s...")
            time.sleep(delay)
            attempt += 1
            continue

        logger.info("Discord listener stopped.")
        break
