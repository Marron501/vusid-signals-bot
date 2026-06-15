"""
AI-powered signal analysis using Claude.
Scores each signal 0-100 and provides a structured verdict.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time

import requests

logger = logging.getLogger(__name__)

# ── Bybit market data helper ─────────────────────────────────────────────────

_ticker_cache: dict[str, tuple[dict, float]] = {}
_TICKER_TTL = 60


def _get_market_data(symbol: str) -> dict:
    """Fetch 24h ticker for a symbol from Bybit (cached)."""
    now = time.time()
    if symbol in _ticker_cache:
        data, ts = _ticker_cache[symbol]
        if now - ts < _TICKER_TTL:
            return data
    try:
        r = requests.get(
            "https://api.bybit.com/v5/market/tickers",
            params={"category": "linear", "symbol": symbol},
            timeout=8,
        )
        items = r.json().get("result", {}).get("list", [])
        data = items[0] if items else {}
        _ticker_cache[symbol] = (data, now)
        return data
    except Exception as e:
        logger.warning(f"[analyzer] market data fetch failed for {symbol}: {e}")
        return {}


# ── Core analysis ────────────────────────────────────────────────────────────

def analyze_signal(signal: dict, win_rate: float = 0.0) -> dict:
    """
    Run Claude AI analysis on a trading signal.
    Returns a dict with: score, verdict, recommendation, factors, summary, etc.
    Falls back to rule-based score if ANTHROPIC_API_KEY is not set.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    sym     = signal.get("symbol", "UNKNOWN")
    side    = signal.get("side", "")
    tp      = signal.get("take_profit")
    sl      = signal.get("stop_loss")
    lev     = signal.get("leverage")

    md      = _get_market_data(sym)
    price_s = md.get("lastPrice", "")
    chg_s   = md.get("price24hPcnt", "")
    vol_s   = md.get("volume24h", "")

    # ── Risk/Reward calculation ──────────────────────────────────────────
    rr_str  = "N/A"
    rr_val  = None
    if tp and sl and price_s:
        try:
            entry = float(price_s)
            tp_f  = float(tp)
            sl_f  = float(sl)
            if side == "Buy" and (entry - sl_f) > 0:
                rr_val = (tp_f - entry) / (entry - sl_f)
            elif side == "Sell" and (sl_f - entry) > 0:
                rr_val = (entry - tp_f) / (sl_f - entry)
            if rr_val is not None:
                rr_str = f"{rr_val:.2f}:1"
        except Exception:
            pass

    # ── Trend alignment ──────────────────────────────────────────────────
    trend_align = "neutral"
    if chg_s:
        chg = float(chg_s) * 100
        if side == "Buy"  and chg > 1:  trend_align = "with"
        elif side == "Sell" and chg < -1: trend_align = "with"
        elif side == "Buy"  and chg < -1: trend_align = "against"
        elif side == "Sell" and chg > 1:  trend_align = "against"

    # ── Fallback rule-based scorer (no API key) ──────────────────────────
    if not api_key:
        return _rule_based(signal, rr_val, rr_str, trend_align, win_rate)

    # ── Claude API analysis ───────────────────────────────────────────────
    direction  = "LONG (Buy)" if side == "Buy" else "SHORT (Sell)"
    price_disp = f"${float(price_s):.4f}" if price_s else "unknown"
    chg_disp   = f"{float(chg_s)*100:+.2f}%" if chg_s else "unknown"
    vol_disp   = f"${float(vol_s)/1e6:.1f}M" if vol_s else "unknown"
    lev_disp   = f"{lev}x" if lev else "not specified"
    wr_disp    = f"{win_rate*100:.1f}%"

    prompt = f"""You are a professional quantitative crypto futures analyst. Score this signal objectively.

SIGNAL:
  Symbol   : {sym}
  Direction: {direction}
  Leverage : {lev_disp}
  Take Profit: {tp or 'not set'}
  Stop Loss  : {sl or 'not set'}
  Calculated R:R: {rr_str}

MARKET (24h):
  Price     : {price_disp}
  24h Change: {chg_disp}
  24h Volume: {vol_disp}
  Trend vs signal: {trend_align}

CHANNEL STATS:
  Historical win rate: {wr_disp}

SCORING CRITERIA:
  • R:R ≥ 2:1 → +20 pts;  1:1 → +10;  <1:1 → -15
  • SL present → +10; TP present → +10
  • Trend alignment (with) → +15; (against) → -20; neutral → 0
  • Leverage ≤ 5x → +5; 6-15x → 0; >15x → -10
  • High volume (>$50M) → +5
  • Channel win rate >70% → +5; <50% → -5
  Base score starts at 50.

Return ONLY valid JSON (no markdown):
{{"score":72,"verdict":"likely_win","risk_reward":"{rr_str}","trend_alignment":"{trend_align}","leverage_risk":"moderate","confidence":"medium","factors":["R:R of 2.1:1 rewards risk well","Signal aligns with bullish 24h momentum","Leverage is conservative at 5x"],"summary":"Solid setup with good risk management and trend alignment.","recommendation":"take"}}

verdict: strong_win | likely_win | neutral | likely_loss | strong_loss
recommendation: take | caution | skip"""

    try:
        import anthropic
        client  = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            temperature=0.2,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()
        m   = re.search(r"\{.*\}", raw, re.DOTALL)
        obj = json.loads(m.group() if m else raw)
        obj["enabled"] = True
        obj["ai"]      = True
        return obj
    except Exception as e:
        logger.error(f"[analyzer] Claude API error: {e}")
        result = _rule_based(signal, rr_val, rr_str, trend_align, win_rate)
        result["ai"]      = False
        result["enabled"] = True
        return result


# ── Rule-based fallback ───────────────────────────────────────────────────────

def _rule_based(signal: dict, rr_val, rr_str: str, trend_align: str,
                win_rate: float) -> dict:
    """Pure math score — no API call needed."""
    score   = 50
    factors = []
    lev     = signal.get("leverage")
    tp      = signal.get("take_profit")
    sl      = signal.get("stop_loss")

    if rr_val is not None:
        if rr_val >= 2:
            score += 20; factors.append(f"Excellent R:R of {rr_str}")
        elif rr_val >= 1:
            score += 10; factors.append(f"Acceptable R:R of {rr_str}")
        else:
            score -= 15; factors.append(f"Poor R:R of {rr_str} — risk outweighs reward")
    else:
        factors.append("R:R unknown (no TP/SL set)")

    if tp: score += 10
    if sl: score += 10
    if not sl: factors.append("No stop loss set — unlimited downside risk")

    if trend_align == "with":
        score += 15; factors.append("Signal aligns with 24h market trend")
    elif trend_align == "against":
        score -= 20; factors.append("Signal trades against 24h momentum")

    if lev:
        lv = float(lev)
        if lv <= 5:   score += 5;  factors.append(f"Conservative {lv}x leverage")
        elif lv > 15: score -= 10; factors.append(f"High {lv}x leverage increases liquidation risk")

    if win_rate > 0.70: score += 5
    elif win_rate > 0:  score -= 5

    score = max(0, min(100, score))

    if score >= 75:   verdict, rec = "likely_win",  "take"
    elif score >= 60: verdict, rec = "neutral",      "caution"
    elif score >= 40: verdict, rec = "likely_loss",  "caution"
    else:             verdict, rec = "strong_loss",  "skip"

    if score >= 80: verdict = "strong_win"; rec = "take"

    return {
        "score":          score,
        "verdict":        verdict,
        "risk_reward":    rr_str,
        "trend_alignment": trend_align,
        "leverage_risk":  "low" if float(lev or 5) <= 5 else ("high" if float(lev or 5) > 15 else "moderate"),
        "confidence":     "medium",
        "factors":        factors[:3] or ["Insufficient data for full analysis"],
        "summary":        f"Rule-based score: {score}/100 — {verdict.replace('_',' ')}.",
        "recommendation": rec,
        "enabled":        True,
        "ai":             False,
    }
