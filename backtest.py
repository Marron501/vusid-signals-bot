"""
Signal Backtester — replays historical Discord signals against real Bybit price data.

Flow:
  1. Load signals.json from DATA_DIR
  2. Score each signal (rule-based or Claude AI)
  3. Filter score >= threshold
  4. Fetch 15-min klines from Bybit from signal timestamp onwards
  5. Simulate: entry at market, walk candles until SL or TP hit (max 200 candles = ~50h)
  6. Compute full analytics suite
"""
from __future__ import annotations

import json
import logging
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

from paths import DATA_DIR

logger = logging.getLogger(__name__)

SIGNALS_FILE = DATA_DIR / "signals.json"
KLINE_INTERVAL = "15"          # 15-minute candles
MAX_CANDLES    = 192           # ~48 hours per trade max
RATE_LIMIT_DELAY = 0.22        # ~4.5 req/s, well within Bybit's 120/min limit


# ── Bybit price data ──────────────────────────────────────────────────────────

def _fetch_klines(symbol: str, start_ms: int, interval: str = KLINE_INTERVAL,
                  limit: int = MAX_CANDLES) -> list[list]:
    """
    Fetch OHLCV candles from Bybit.
    Returns list of [startTime, open, high, low, close, volume, turnover]
    sorted ascending by time.
    """
    try:
        r = requests.get(
            "https://api.bybit.com/v5/market/kline",
            params={"category": "linear", "symbol": symbol,
                    "interval": interval, "start": start_ms, "limit": limit},
            timeout=12,
        )
        result = r.json().get("result", {}).get("list", [])
        # Bybit returns newest first; reverse for chronological order
        return list(reversed(result))
    except Exception as e:
        logger.warning(f"[backtest] kline fetch failed {symbol}: {e}")
        return []


def _get_entry_price(symbol: str, signal_ts_ms: int) -> Optional[float]:
    """
    Fetch the close price of the 1-minute candle at signal time.
    This is the realistic market entry price.
    """
    candles = _fetch_klines(symbol, signal_ts_ms, interval="1", limit=2)
    if candles:
        try:
            return float(candles[0][4])  # close price
        except Exception:
            pass
    return None


# ── Trade simulation ──────────────────────────────────────────────────────────

def _simulate_trade(symbol: str, side: str, entry: float,
                    sl: Optional[float], tp: Optional[float],
                    signal_ts_ms: int) -> dict:
    """
    Walk forward through 15-min candles from signal_ts_ms.
    Returns outcome dict.
    """
    if not sl and not tp:
        return {"outcome": "no_levels", "exit_price": entry, "r_achieved": 0,
                "hold_hours": 0, "candles": 0, "exit_ts": signal_ts_ms}

    candles = _fetch_klines(symbol, signal_ts_ms)
    if not candles:
        return {"outcome": "no_data", "exit_price": entry, "r_achieved": 0,
                "hold_hours": 0, "candles": 0, "exit_ts": signal_ts_ms}

    is_long   = side == "Buy"
    sl_dist   = abs(entry - sl) if sl else None
    tp_dist   = abs(tp - entry) if tp else None
    rr_target = (tp_dist / sl_dist) if (sl_dist and tp_dist and sl_dist > 0) else None

    for i, c in enumerate(candles):
        try:
            ts_c   = int(c[0])
            c_high = float(c[2])
            c_low  = float(c[3])
            c_close= float(c[4])
        except Exception:
            continue

        hold_hours = (ts_c - signal_ts_ms) / 3_600_000

        sl_hit = tp_hit = False
        if is_long:
            sl_hit = sl and c_low  <= sl
            tp_hit = tp and c_high >= tp
        else:
            sl_hit = sl and c_high >= sl
            tp_hit = tp and c_low  <= tp

        # Both in same candle → conservative: SL hit first
        if sl_hit and tp_hit:
            sl_hit, tp_hit = True, False

        if sl_hit:
            exit_price = sl
            r_achieved = -1.0
            return {"outcome": "loss", "exit_price": exit_price,
                    "r_achieved": r_achieved, "hold_hours": round(hold_hours, 1),
                    "candles": i + 1, "exit_ts": ts_c}

        if tp_hit:
            exit_price = tp
            r_achieved = rr_target if rr_target else 1.0
            return {"outcome": "win", "exit_price": exit_price,
                    "r_achieved": round(r_achieved, 3), "hold_hours": round(hold_hours, 1),
                    "candles": i + 1, "exit_ts": ts_c}

    # Time-out — position still open at last candle
    last_close = float(candles[-1][4]) if candles else entry
    last_ts    = int(candles[-1][0])   if candles else signal_ts_ms
    if is_long:
        r_achieved = ((last_close - entry) / sl_dist) if sl_dist else 0
    else:
        r_achieved = ((entry - last_close) / sl_dist) if sl_dist else 0
    hold_h = (last_ts - signal_ts_ms) / 3_600_000
    outcome = "open_win" if r_achieved > 0 else "open_loss"
    return {"outcome": outcome, "exit_price": last_close,
            "r_achieved": round(r_achieved, 3), "hold_hours": round(hold_h, 1),
            "candles": len(candles), "exit_ts": last_ts}


# ── Analytics ─────────────────────────────────────────────────────────────────

def _compute_analytics(trades: list[dict], start_equity: float = 10000.0) -> dict:
    """Compute comprehensive trading analytics."""
    closed = [t for t in trades if t["outcome"] in ("win", "loss",
                                                      "open_win", "open_loss")]
    wins   = [t for t in closed if t["r_achieved"] > 0]
    losses = [t for t in closed if t["r_achieved"] <= 0]
    n      = len(closed)

    if n == 0:
        return {"n_trades": 0, "error": "No closed trades to analyse"}

    win_rate    = len(wins) / n * 100
    avg_win_r   = sum(t["r_achieved"] for t in wins)   / len(wins)   if wins   else 0
    avg_loss_r  = sum(t["r_achieved"] for t in losses) / len(losses) if losses else 0
    profit_factor = (sum(t["r_achieved"] for t in wins) /
                     abs(sum(t["r_achieved"] for t in losses))) if losses else float("inf")
    expectancy  = sum(t["r_achieved"] for t in closed) / n  # avg R per trade

    # Equity curve (track running equity assuming fixed 1R = risk_pct of equity)
    # Use 2% risk per trade → 1R = 2% of running equity
    risk_pct    = 0.02
    equity      = start_equity
    equity_curve= [equity]
    peak        = equity
    max_dd      = 0.0
    max_dd_pct  = 0.0
    dd_start    = equity
    dd_depth    = 0.0

    for t in closed:
        r = t["r_achieved"]
        pnl = equity * risk_pct * r
        equity += pnl
        equity_curve.append(round(equity, 2))
        if equity > peak:
            peak = equity
        dd = peak - equity
        dd_pct = dd / peak * 100
        if dd > max_dd:
            max_dd     = dd
            max_dd_pct = dd_pct

    # Consecutive losses
    max_consec_loss = consec = 0
    for t in closed:
        if t["r_achieved"] <= 0:
            consec += 1
            max_consec_loss = max(max_consec_loss, consec)
        else:
            consec = 0

    # By symbol
    by_sym: dict[str, dict] = {}
    for t in closed:
        s = t["symbol"]
        if s not in by_sym:
            by_sym[s] = {"n": 0, "wins": 0, "total_r": 0.0}
        by_sym[s]["n"]       += 1
        by_sym[s]["total_r"] += t["r_achieved"]
        if t["r_achieved"] > 0:
            by_sym[s]["wins"] += 1
    for s, v in by_sym.items():
        v["win_rate"] = round(v["wins"] / v["n"] * 100, 1)
        v["avg_r"]    = round(v["total_r"] / v["n"], 3)

    # By direction
    longs  = [t for t in closed if t["side"] == "Buy"]
    shorts = [t for t in closed if t["side"] == "Sell"]
    by_dir = {
        "Long":  {"n": len(longs),
                  "win_rate": round(len([t for t in longs  if t["r_achieved"]>0])/len(longs)*100,1)  if longs  else 0,
                  "avg_r":    round(sum(t["r_achieved"] for t in longs) /len(longs),3)               if longs  else 0},
        "Short": {"n": len(shorts),
                  "win_rate": round(len([t for t in shorts if t["r_achieved"]>0])/len(shorts)*100,1) if shorts else 0,
                  "avg_r":    round(sum(t["r_achieved"] for t in shorts)/len(shorts),3)              if shorts else 0},
    }

    # Monthly breakdown
    by_month: dict[str, dict] = {}
    for t in closed:
        month = t.get("timestamp", "")[:7]  # "YYYY-MM"
        if not month: continue
        if month not in by_month:
            by_month[month] = {"n": 0, "wins": 0, "total_r": 0.0}
        by_month[month]["n"]       += 1
        by_month[month]["total_r"] += t["r_achieved"]
        if t["r_achieved"] > 0:
            by_month[month]["wins"] += 1
    for m, v in by_month.items():
        v["win_rate"] = round(v["wins"] / v["n"] * 100, 1)

    # Sharpe-like (R-based)
    r_list = [t["r_achieved"] for t in closed]
    mean_r = sum(r_list) / len(r_list)
    std_r  = math.sqrt(sum((r - mean_r)**2 for r in r_list) / len(r_list)) if len(r_list) > 1 else 0
    sharpe = (mean_r / std_r * math.sqrt(252)) if std_r > 0 else 0  # annualised

    # Avg hold time
    avg_hold = sum(t.get("hold_hours", 0) for t in closed) / n

    # Prop firm checks (FTMO-like)
    prop_daily_violations = []
    day_pnl: dict[str, float] = {}
    eq_running = start_equity
    for t in closed:
        day = t.get("timestamp", "")[:10]
        r   = t["r_achieved"]
        pnl = eq_running * risk_pct * r
        eq_running += pnl
        day_pnl[day] = day_pnl.get(day, 0) + pnl
    for day, pnl in day_pnl.items():
        daily_dd_pct = abs(pnl) / start_equity * 100
        if pnl < 0 and daily_dd_pct > 5:
            prop_daily_violations.append({"date": day, "loss_pct": round(daily_dd_pct, 2)})

    final_equity = equity_curve[-1]
    total_return = (final_equity - start_equity) / start_equity * 100

    return {
        "n_trades":           n,
        "n_wins":             len(wins),
        "n_losses":           len(losses),
        "win_rate":           round(win_rate, 1),
        "profit_factor":      round(profit_factor, 2) if profit_factor != float("inf") else 999,
        "expectancy_r":       round(expectancy, 3),
        "avg_win_r":          round(avg_win_r, 3),
        "avg_loss_r":         round(avg_loss_r, 3),
        "max_dd_pct":         round(max_dd_pct, 2),
        "max_consec_losses":  max_consec_loss,
        "sharpe":             round(sharpe, 2),
        "avg_hold_hours":     round(avg_hold, 1),
        "total_return_pct":   round(total_return, 2),
        "final_equity":       round(final_equity, 2),
        "start_equity":       start_equity,
        "equity_curve":       equity_curve,
        "by_symbol":          dict(sorted(by_sym.items(), key=lambda x: x[1]["n"], reverse=True)),
        "by_direction":       by_dir,
        "by_month":           dict(sorted(by_month.items())),
        "prop_daily_violations": prop_daily_violations,
        "prop_pass": (max_dd_pct < 10 and len(prop_daily_violations) == 0),
    }


# ── Main entry point ──────────────────────────────────────────────────────────

def run_backtest(score_threshold: int = 55,
                 start_equity: float = 10000.0,
                 use_ai_scoring: bool = False,
                 progress_cb=None) -> dict:
    """
    Full backtest pipeline. Returns analytics dict + individual trade list.

    progress_cb(pct: int, message: str) — optional progress callback.
    """
    def _progress(pct: int, msg: str):
        if progress_cb:
            try: progress_cb(pct, msg)
            except Exception: pass

    # 1. Load signals
    _progress(2, "Loading signals…")
    try:
        raw_signals = json.loads(SIGNALS_FILE.read_text()) if SIGNALS_FILE.exists() else []
    except Exception as e:
        return {"error": f"Cannot load signals: {e}"}

    # Filter to "open" signals with at least a symbol
    open_sigs = [s for s in raw_signals
                 if s.get("signal", {}).get("action") == "open"
                 and s.get("signal", {}).get("symbol")]

    if not open_sigs:
        return {"error": "No open signals found in signals.json",
                "n_signals_total": len(raw_signals)}

    _progress(5, f"Found {len(open_sigs)} signals — scoring…")

    # 2. Score each signal
    from signal_analyzer import analyze_signal, _rule_based

    scored = []
    for i, entry in enumerate(open_sigs):
        sig   = entry["signal"]
        pct   = 5 + int(i / len(open_sigs) * 20)
        _progress(pct, f"Scoring {sig.get('symbol','?')} ({i+1}/{len(open_sigs)})…")

        if use_ai_scoring:
            analysis = analyze_signal(sig, win_rate=0.0)
            time.sleep(0.5)   # avoid rate-limit on Claude API
        else:
            analysis = analyze_signal(sig, win_rate=0.0)  # will use rule-based if no key

        score = analysis.get("score", 0)
        if score >= score_threshold:
            scored.append({"entry": entry, "signal": sig,
                           "score": score, "analysis": analysis})

    if not scored:
        return {"error": f"No signals passed score threshold ({score_threshold})",
                "n_signals_total": len(raw_signals),
                "n_open_signals": len(open_sigs)}

    _progress(25, f"{len(scored)} signals passed score ≥{score_threshold} — simulating trades…")

    # 3. Simulate each trade
    trades = []
    for i, item in enumerate(scored):
        sig   = item["signal"]
        entry_rec = item["entry"]
        sym   = sig.get("symbol", "")
        side  = sig.get("side", "Buy")
        sl    = float(sig["stop_loss"])   if sig.get("stop_loss")   and sig["stop_loss"]   not in ("None", None) else None
        tp    = float(sig["take_profit"]) if sig.get("take_profit") and sig["take_profit"] not in ("None", None) else None

        pct = 25 + int(i / len(scored) * 65)
        _progress(pct, f"[{i+1}/{len(scored)}] Simulating {sym}…")

        # Parse signal timestamp → ms
        ts_str = entry_rec.get("timestamp", "")
        try:
            ts_dt  = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            ts_ms  = int(ts_dt.timestamp() * 1000)
        except Exception:
            ts_ms = int(time.time() * 1000) - 86_400_000  # fallback: yesterday

        # Entry price
        entry_price = None
        ep_raw = sig.get("entry")
        if ep_raw and ep_raw not in ("None", None, ""):
            try: entry_price = float(ep_raw)
            except Exception: pass
        if not entry_price:
            entry_price = _get_entry_price(sym, ts_ms)
            time.sleep(RATE_LIMIT_DELAY)
        if not entry_price:
            trades.append({"outcome": "no_data", "symbol": sym, "side": side,
                           "score": item["score"], "timestamp": ts_str,
                           "entry_price": 0, "sl_price": sl, "tp_price": tp,
                           "r_achieved": 0, "hold_hours": 0})
            continue

        # Simulate
        sim = _simulate_trade(sym, side, entry_price, sl, tp, ts_ms)
        time.sleep(RATE_LIMIT_DELAY)

        # R:R target
        rr_target = None
        if sl and tp and entry_price:
            sl_d = abs(entry_price - sl)
            tp_d = abs(tp - entry_price)
            rr_target = round(tp_d / sl_d, 2) if sl_d > 0 else None

        trades.append({
            "timestamp":    ts_str,
            "symbol":       sym,
            "side":         side,
            "score":        item["score"],
            "verdict":      item["analysis"].get("verdict", "—"),
            "entry_price":  round(entry_price, 6),
            "sl_price":     sl,
            "tp_price":     tp,
            "rr_target":    rr_target,
            "outcome":      sim["outcome"],
            "exit_price":   round(sim["exit_price"], 6),
            "r_achieved":   sim["r_achieved"],
            "hold_hours":   sim["hold_hours"],
            "candles_used": sim["candles"],
            "exit_ts":      sim.get("exit_ts", ts_ms),
        })

    _progress(90, "Computing analytics…")

    # 4. Analytics
    analytics = _compute_analytics(trades, start_equity=start_equity)
    analytics["n_signals_total"]  = len(raw_signals)
    analytics["n_open_signals"]   = len(open_sigs)
    analytics["n_scored_signals"] = len(scored)
    analytics["score_threshold"]  = score_threshold
    analytics["trades"]           = trades

    _progress(100, "Done.")
    return analytics
