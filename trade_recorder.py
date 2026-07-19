"""
Trade Recorder — ground-truth performance tracking.

Polls Bybit's closed-PnL endpoint for the primary account and every enabled
extra account, and records each CLOSED trade with its REAL realised PnL.

Why this exists:
  * The old path (trade_optimizer.record_trade) only fired on a manual close and
    estimated PnL from `unrealisedPnl`. Trades closed by SL/TP on the exchange —
    the normal case — were never counted at all.
  * It also wrote to a container-local file, so stats vanished on redeploy and
    get_win_rate() never saw them.

Everything here writes to the volume-backed paths (paths.HISTORY_FILE /
paths.STATS_FILE), which is exactly what signal_listener.get_win_rate() reads.
Once >= 5 real trades are recorded, the bot's win-rate filter automatically
switches from the hardcoded channel figure to measured performance.

SAFETY: read-only against the exchange. Never opens, closes or modifies a trade.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone

from paths import HISTORY_FILE, STATS_FILE

logger = logging.getLogger(__name__)

POLL_SECONDS = 120          # how often to reconcile closed trades
LOOKBACK_MS  = 7 * 24 * 60 * 60 * 1000   # scan last 7 days each poll
_lock        = threading.RLock()


# ── persistence ───────────────────────────────────────────────────────────────
def load_history() -> list[dict]:
    with _lock:
        if not HISTORY_FILE.exists():
            return []
        try:
            data = json.loads(HISTORY_FILE.read_text())
            return data if isinstance(data, list) else []
        except Exception:
            return []


def _save_history(items: list[dict]) -> None:
    with _lock:
        try:
            tmp = HISTORY_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(items, indent=2, default=str))
            import os
            os.replace(tmp, HISTORY_FILE)
        except Exception as e:
            logger.error(f"[recorder] history save failed: {e}")


def _save_stats(stats: dict) -> None:
    with _lock:
        try:
            tmp = STATS_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(stats, indent=2, default=str))
            import os
            os.replace(tmp, STATS_FILE)
        except Exception as e:
            logger.error(f"[recorder] stats save failed: {e}")


# ── stats ─────────────────────────────────────────────────────────────────────
def compute_stats(history: list[dict] | None = None) -> dict:
    """Recompute stats from the full history (source of truth)."""
    hist = load_history() if history is None else history
    wins = losses = breakeven = 0
    total_pnl = 0.0
    best = worst = None
    per_symbol: dict[str, dict] = {}

    for h in hist:
        try:
            pnl = float(h.get("pnl", 0) or 0)
        except Exception:
            pnl = 0.0
        total_pnl += pnl
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
        else:
            breakeven += 1

        if best is None or pnl > float(best.get("pnl", 0) or 0):
            best = h
        if worst is None or pnl < float(worst.get("pnl", 0) or 0):
            worst = h

        sym = h.get("symbol", "?")
        s = per_symbol.setdefault(sym, {"trades": 0, "wins": 0, "pnl": 0.0})
        s["trades"] += 1
        s["pnl"] += pnl
        if pnl > 0:
            s["wins"] += 1

    decided = wins + losses          # break-evens excluded from win rate
    total   = len(hist)
    win_rate = (wins / decided) if decided else 0.0

    return {
        "total_trades":  total,
        "wins":          wins,
        "losses":        losses,
        "breakeven":     breakeven,
        "win_rate":      round(win_rate * 100, 2),
        "total_pnl":     round(total_pnl, 4),
        "best_trade":    best,
        "worst_trade":   worst,
        "symbol_performance": per_symbol,
        "source":        "live",     # marks these as measured, not hardcoded
        "updated":       datetime.now(timezone.utc).isoformat(),
    }


# ── exchange reconciliation ───────────────────────────────────────────────────
def _fetch_closed(executor, account_name: str) -> list[dict]:
    """Pull recent closed trades for one account. Read-only."""
    out: list[dict] = []
    try:
        start_ms = int(time.time() * 1000) - LOOKBACK_MS
        resp = executor.client.get_closed_pnl(
            category="linear", startTime=start_ms, limit=100
        )
        for r in (resp.get("result", {}) or {}).get("list", []) or []:
            try:
                # closed-pnl 'side' is the CLOSING order side → invert for position side
                close_side = r.get("side", "")
                pos_side   = "Buy" if close_side == "Sell" else "Sell"
                out.append({
                    "id":         r.get("orderId") or f"{r.get('symbol')}_{r.get('updatedTime')}",
                    "account":    account_name,
                    "symbol":     r.get("symbol", ""),
                    "side":       pos_side,
                    "qty":        r.get("qty", ""),
                    "entry":      r.get("avgEntryPrice", ""),
                    "exit":       r.get("avgExitPrice", ""),
                    "leverage":   r.get("leverage", ""),     # ACTUAL applied leverage
                    "pnl":        float(r.get("closedPnl", 0) or 0),
                    "closed_ms":  int(r.get("updatedTime", 0) or 0),
                    "closed_iso": datetime.fromtimestamp(
                                      int(r.get("updatedTime", 0) or 0) / 1000,
                                      tz=timezone.utc).isoformat(),
                })
            except Exception:
                continue
    except Exception as e:
        logger.debug(f"[recorder] closed-pnl fetch failed ({account_name}): {e}")
    return out


def reconcile() -> dict:
    """
    Pull closed trades from every account, merge new ones into history,
    recompute stats. Returns the fresh stats dict.
    """
    found: list[dict] = []

    # Primary (env credentials)
    try:
        from trade_executor import TradeExecutor
        found += _fetch_closed(TradeExecutor(), "Primary")
    except Exception as e:
        logger.debug(f"[recorder] primary skipped: {e}")

    # Extra accounts
    try:
        from accounts_manager import get_enabled_accounts, get_executor
        for acc in get_enabled_accounts():
            try:
                found += _fetch_closed(get_executor(acc), acc.get("name", acc.get("id", "acct")))
            except Exception as e:
                logger.debug(f"[recorder] {acc.get('name')} skipped: {e}")
    except Exception as e:
        logger.debug(f"[recorder] extra accounts skipped: {e}")

    with _lock:
        history = load_history()
        known   = {h.get("id") for h in history}
        new     = [f for f in found if f.get("id") and f["id"] not in known]
        if new:
            history.extend(new)
            history.sort(key=lambda h: h.get("closed_ms", 0))
            _save_history(history)
            for n in new:
                logger.info(
                    f"[recorder] NEW closed trade {n['account']} {n['symbol']} "
                    f"{n['side']} pnl={n['pnl']:+.2f} lev={n['leverage']}x"
                )
        stats = compute_stats(history)
        _save_stats(stats)

    if new:
        logger.info(
            f"[recorder] +{len(new)} trade(s) — total {stats['total_trades']}, "
            f"win rate {stats['win_rate']}%"
        )
    return stats


# ── background loop ───────────────────────────────────────────────────────────
def _loop() -> None:
    time.sleep(90)  # let the app settle
    while True:
        try:
            reconcile()
        except Exception as e:
            logger.error(f"[recorder] loop error: {e}")
        time.sleep(POLL_SECONDS)


_started = False
def start_recorder() -> None:
    """Start the background reconciliation thread (idempotent)."""
    global _started
    if _started:
        return
    _started = True
    threading.Thread(target=_loop, daemon=True, name="trade-recorder").start()
    logger.info(f"[recorder] started — reconciling closed trades every {POLL_SECONDS}s")
