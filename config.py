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
