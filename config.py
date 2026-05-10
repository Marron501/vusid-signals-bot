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

# --- Risk / Sizing ---
EQUITY_FRACTION = float(os.getenv("EQUITY_FRACTION", "1.0"))
DEFAULT_LEVERAGE = float(os.getenv("DEFAULT_LEVERAGE", "5"))

# --- Demo / Testnet mode ---
USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() in ("true", "1", "yes")

# --- Bybit endpoints ---
BYBIT_MAINNET = "https://api.bybit.com"

# --- Logging ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "bot.log")

# --- Position mode ---
POSITION_MODE = int(os.getenv("POSITION_MODE", "0"))  # 0 = one-way

# --- Auto-execute signals ---
AUTO_EXECUTE = os.getenv("AUTO_EXECUTE", "true").lower() in ("true", "1", "yes")
