"""
Multi-account manager.

Stores additional trading accounts in accounts.json.
The primary account (config.API_KEY / API_SECRET) is always active by default;
accounts.json holds extra accounts that also receive every signal.

Each account can override equity_fraction and leverage independently,
so you can manage client accounts at different risk levels from the same
Discord signal channel.
"""
from __future__ import annotations

import json
import logging
import threading
import uuid
from pathlib import Path
from typing import Optional

from paths import ACCOUNTS_FILE

logger = logging.getLogger(__name__)

_DEFAULTS: dict = {
    "id":              "",
    "name":            "New Account",
    "api_key":         "",
    "api_secret":      "",
    "equity_fraction": 0.10,   # 10 % of equity per trade
    "leverage":        5,
    "enabled":         True,
    "auto_execute":    True,   # execute signals automatically on this account
    "testnet":         False,   # True → api-testnet.bybit.com (Bybit Testnet)
    "demo":            False,   # True → api.bybit.com with demo flag (Bybit Demo Trading)
    "note":            "",
}

_lock = threading.RLock()  # reentrant: load_accounts() called inside add/update while lock held


# ── CRUD ─────────────────────────────────────────────────────────────────────

def load_accounts() -> list[dict]:
    with _lock:
        if not ACCOUNTS_FILE.exists():
            return []
        try:
            return json.loads(ACCOUNTS_FILE.read_text())
        except Exception as e:
            logger.error(f"[accounts] load failed: {e}")
            return []


def _save(accounts: list[dict]) -> None:
    ACCOUNTS_FILE.write_text(json.dumps(accounts, indent=2))


def add_account(data: dict) -> dict:
    acc = {
        **_DEFAULTS,
        **{k: v for k, v in data.items() if k in _DEFAULTS},
        "id": str(uuid.uuid4())[:8],
    }
    with _lock:
        accounts = load_accounts()
        accounts.append(acc)
        _save(accounts)
    logger.info(f"[accounts] Added '{acc['name']}' (id={acc['id']})")
    return acc


def update_account(acc_id: str, data: dict) -> Optional[dict]:
    with _lock:
        accounts = load_accounts()
        for i, a in enumerate(accounts):
            if a["id"] == acc_id:
                accounts[i] = {**a, **{k: v for k, v in data.items() if k != "id"}}
                _save(accounts)
                logger.info(f"[accounts] Updated '{accounts[i]['name']}' (id={acc_id})")
                return accounts[i]
    logger.warning(f"[accounts] update: id={acc_id} not found")
    return None


def remove_account(acc_id: str) -> bool:
    with _lock:
        accounts = load_accounts()
        new = [a for a in accounts if a["id"] != acc_id]
        if len(new) < len(accounts):
            _save(new)
            logger.info(f"[accounts] Removed id={acc_id}")
            return True
    return False


def toggle_account(acc_id: str) -> Optional[dict]:
    with _lock:
        accounts = load_accounts()
        for i, a in enumerate(accounts):
            if a["id"] == acc_id:
                accounts[i]["enabled"] = not a.get("enabled", True)
                _save(accounts)
                return accounts[i]
    return None


def get_enabled_accounts() -> list[dict]:
    """Return only accounts that are enabled and have API credentials set."""
    return [
        a for a in load_accounts()
        if a.get("enabled", True) and a.get("api_key") and a.get("api_secret")
    ]


# ── Per-account executor ──────────────────────────────────────────────────────

def get_executor(account: dict):
    """
    Return a TradeExecutor configured for the given account dict.
    The executor is NOT cached — callers can cache if needed.
    """
    from trade_executor import TradeExecutor
    return TradeExecutor(
        api_key    = account["api_key"],
        api_secret = account["api_secret"],
        testnet    = account.get("testnet", False),
        demo       = account.get("demo", False),
    )


def get_account_balance(account: dict) -> dict:
    """Fetch full balance for one account. Returns safe fallback on error."""
    try:
        ex = get_executor(account)
        bal = ex.get_full_balance()
        bal["account_id"]   = account["id"]
        bal["account_name"] = account["name"]
        return bal
    except Exception as e:
        return {
            "account_id": account["id"], "account_name": account["name"],
            "equity": 0, "available": 0, "used_margin": 0,
            "unrealised_pnl": 0, "total_equity_usd": 0,
            "positions": [], "total_pnl": 0, "error": str(e),
        }
