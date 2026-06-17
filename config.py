"""
Configuration for the Bybit Signal Trading Bot.
Listens to CopyBot#8959 signals in #daily-signals and executes trades.
Edit .env or set environment variables before running.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# --- Bybit API credentials (YOUR account) ---
API_KEY = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")

# --- Discord Signal Source ---
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
SIGNAL_CHANNEL = os.getenv("SIGNAL_CHANNEL", "daily-signals")
SIGNAL_BOT_NAME = os.getenv("SIGNAL_BOT_NAME", "CopyBot#8959")
SIGNAL_BOT_ID = os.getenv("SIGNAL_BOT_ID", "")  # Discord user ID of CopyBot#8959
OWNER_DISCORD_ID = os.getenv("OWNER_DISCORD_ID", "754978386669207593")  # Your Discord ID for DM alerts
DM_ALERTS = os.getenv("DM_ALERTS", "true").lower() in ("true", "1", "yes")  # Set false to mute DMs

# --- Risk / Sizing ---
EQUITY_FRACTION = float(os.getenv("EQUITY_FRACTION", "1.0"))
DEFAULT_LEVERAGE = float(os.getenv("DEFAULT_LEVERAGE", "5"))

# --- Demo / Testnet mode ---
USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() in ("true", "1", "yes")

# --- Bybit endpoints ---
BYBIT_MAINNET = "https://api.bybit.com"

# --- Logging ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
# LOG_FILE resolved from paths.py (volume-aware); fallback kept for manual CLI use
try:
    from paths import LOG_FILE as _pf
    LOG_FILE = str(_pf)
except Exception:
    LOG_FILE = os.getenv("LOG_FILE", "bot.log")

# --- Position mode ---
POSITION_MODE = int(os.getenv("POSITION_MODE", "0"))  # 0 = one-way

# --- Auto-execute signals ---
AUTO_EXECUTE = os.getenv("AUTO_EXECUTE", "true").lower() in ("true", "1", "yes")

# --- Bybit proxy (optional) --------------------------------------------------
# Set BYBIT_PROXY_URL if your server's IP is blocked by Bybit (e.g. Railway US West).
# Format: http://user:pass@host:port  OR  socks5://user:pass@host:port
# Leave blank to connect directly (recommended after moving to EU/AP Railway region).
BYBIT_PROXY_URL = os.getenv("BYBIT_PROXY_URL", "").strip()

# --- Anthropic AI (signal analysis) -----------------------------------------
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# --- Risk management strategy ------------------------------------------------
# MIN_AI_SCORE: signals scoring below this are logged but NOT executed.
# Set to 0 to disable the filter entirely.
MIN_AI_SCORE     = int(os.getenv("MIN_AI_SCORE", "60"))

# RISK_PCT: fraction of account to risk on a single trade (e.g. 0.02 = 2%).
# Position size is back-calculated from this + SL distance so risk is fixed.
RISK_PCT         = float(os.getenv("RISK_PCT", "0.02"))

# AUTO_SL_PCT: if a signal has no stop loss, set one this far from entry.
# e.g. 0.03 = 3% below entry for longs, 3% above for shorts.
AUTO_SL_PCT      = float(os.getenv("AUTO_SL_PCT", "0.03"))

# PHASE_THRESHOLDS: equity levels (USDT) that trigger risk tier upgrades.
# Phase 1 (0→750): RISK_PCT used as-is.
# Phase 2 (750→1500): risk multiplied by 1.5×.
# Phase 3 (1500+): risk multiplied by 2.5×.
PHASE_2_EQUITY   = float(os.getenv("PHASE_2_EQUITY", "750"))
PHASE_3_EQUITY   = float(os.getenv("PHASE_3_EQUITY", "1500"))

# DAILY_DD_LIMIT: fraction of day-start equity — if daily loss exceeds this,
# the circuit breaker trips and all new signal execution is blocked until
# manually reset from the dashboard or the next UTC day.
# e.g. 0.05 = stop trading after losing 5% in one day.
DAILY_DD_LIMIT     = float(os.getenv("DAILY_DD_LIMIT", "0.05"))

# MAX_OPEN_POSITIONS: hard cap on concurrent open positions across the primary
# account. Signals are skipped (not queued) when this limit is reached.
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "3"))
