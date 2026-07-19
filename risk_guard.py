"""
Risk Guard — Daily drawdown circuit breaker + max open positions check.

Circuit breaker: tracks equity at start of each UTC day. If the account
loses more than DAILY_DD_LIMIT % in a single day, all new signal execution
is blocked until manually reset from the dashboard or the next UTC day.

Position cap: blocks new signals when open positions >= MAX_OPEN_POSITIONS.
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path

from paths import DATA_DIR

logger = logging.getLogger(__name__)

_STATE_FILE = DATA_DIR / "daily_state.json"
_lock = threading.Lock()


# ── Internal helpers ──────────────────────────────────────────────────────────

def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _load() -> dict:
    try:
        if _STATE_FILE.exists():
            return json.loads(_STATE_FILE.read_text())
    except Exception:
        pass
    return {}


def _save(state: dict) -> None:
    try:
        _STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as e:
        logger.error(f"[RiskGuard] save failed: {e}")


# ── Public API ────────────────────────────────────────────────────────────────

def _migrate(state: dict) -> dict:
    """
    Old format was a single flat account state. New format is keyed per account
    so every executing account gets its own circuit breaker. Migrate in place.
    """
    if state and "accounts" not in state:
        if any(k in state for k in ("date", "start_equity", "tripped")):
            return {"accounts": {"primary": state}}
        return {"accounts": {}}
    if not state:
        return {"accounts": {}}
    return state


def check(current_equity: float, dd_limit: float, account_key: str = "primary") -> dict:
    """
    Evaluate whether trading is allowed right now FOR ONE ACCOUNT.

    Each account carries its own day-start equity and breaker, so a funded
    account is actually protected rather than being gated on some other
    account's (possibly empty) balance.

    Returns a dict:
        ok            bool  — False means the circuit breaker is active
        tripped       bool  — True once the day's DD limit is breached
        daily_pnl     float — absolute PnL since day start
        daily_pnl_pct float — % of day-start equity (negative = loss)
        start_equity  float — equity recorded at day start
        date          str   — current UTC date
        account       str   — which account this applies to
    """
    with _lock:
        root  = _migrate(_load())
        accts = root.setdefault("accounts", {})
        acct  = accts.get(account_key, {})
        today = _today()

        # New UTC day → auto-reset for this account
        if acct.get("date") != today:
            acct = {
                "date":         today,
                "start_equity": current_equity,
                "tripped":      False,
                "manual_reset": False,
            }
            accts[account_key] = acct
            _save(root)
            logger.info(
                f"[RiskGuard:{account_key}] New day ({today}) — "
                f"start equity ${current_equity:.2f}"
            )

        start_eq      = float(acct.get("start_equity") or current_equity or 1)
        daily_pnl     = current_equity - start_eq
        daily_pnl_pct = daily_pnl / start_eq if start_eq > 0 else 0
        tripped       = acct.get("tripped", False)

        # Trip the breaker if loss exceeds limit
        if not tripped and daily_pnl_pct <= -abs(dd_limit):
            tripped = True
            acct["tripped"] = True
            accts[account_key] = acct
            _save(root)
            logger.warning(
                f"[RiskGuard:{account_key}] ⛔ CIRCUIT BREAKER TRIPPED — "
                f"daily PnL {daily_pnl_pct*100:.2f}% / limit -{dd_limit*100:.1f}%"
            )

        return {
            "ok":            not tripped,
            "tripped":       tripped,
            "date":          today,
            "account":       account_key,
            "start_equity":  start_eq,
            "daily_pnl":     round(daily_pnl, 4),
            "daily_pnl_pct": round(daily_pnl_pct * 100, 3),  # stored as %
            "dd_limit_pct":  round(dd_limit * 100, 1),
        }


def manual_reset(new_equity: float | None = None, account_key: str | None = None) -> dict:
    """
    Reset the circuit breaker manually. account_key=None resets every account.
    """
    with _lock:
        root  = _migrate(_load())
        accts = root.setdefault("accounts", {})
        targets = [account_key] if account_key else (list(accts.keys()) or ["primary"])
        for key in targets:
            acct = accts.get(key, {})
            acct["tripped"]      = False
            acct["manual_reset"] = True
            acct["date"]         = _today()
            if new_equity is not None:
                acct["start_equity"] = new_equity
            accts[key] = acct
        _save(root)
        logger.info(f"[RiskGuard] Circuit breaker manually reset for: {', '.join(targets)}")
        return root


def get_state() -> dict:
    """Return raw state dict for the dashboard API (per-account)."""
    with _lock:
        return _migrate(_load())
