"""
Centralised data-file paths for the Prolific signals bot.

Priority order for the data directory:
  1. DATA_DIR env var  — set this in Railway to point at your mounted volume
  2. /data             — Railway's default volume mount point (writable check)
  3. Project directory — local development fallback

All persistent files (signals, accounts, stats, logs) live in DATA_DIR so they
survive Railway redeploys when a volume is mounted.
"""
from __future__ import annotations
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Directory where this file lives (project root)
BASE = Path(__file__).parent


def _resolve_data_dir() -> Path:
    # 1. Explicit override
    env = os.environ.get("DATA_DIR", "").strip()
    if env:
        p = Path(env)
        p.mkdir(parents=True, exist_ok=True)
        logger.info(f"[paths] DATA_DIR from env: {p}")
        return p

    # 2. Railway volume default mount point
    p = Path("/data")
    if p.exists() and os.access(p, os.W_OK):
        logger.info(f"[paths] DATA_DIR: /data (Railway volume)")
        return p

    # 3. Local fallback — project directory
    logger.info(f"[paths] DATA_DIR: {BASE} (local fallback)")
    return BASE


DATA_DIR = _resolve_data_dir()

# ── Persistent data files ────────────────────────────────────────────────────
SIGNALS_FILE    = DATA_DIR / "signals.json"
PROCESSED_FILE  = DATA_DIR / "processed_signals.json"
STATS_FILE      = DATA_DIR / "trade_stats.json"
HISTORY_FILE    = DATA_DIR / "trade_history.json"
ACCOUNTS_FILE   = DATA_DIR / "accounts.json"

# ── Log files ────────────────────────────────────────────────────────────────
LOG_FILE        = DATA_DIR / "bot.log"
DISCORD_LOG     = DATA_DIR / "bot_discord.log"
