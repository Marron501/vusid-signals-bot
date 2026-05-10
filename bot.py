#!/usr/bin/env python3
"""
Bybit Signal Trading Bot
Listens to CopyBot#8959 in #daily-signals on Discord and auto-executes trades.
Also supports manual trades from the command line.

Usage:
    python3 bot.py                           # Start listening for Discord signals
    python3 bot.py --status                  # Show current positions & account
    python3 bot.py open BTC Buy 500 10       # Manual: open Buy BTCUSDT, $500, 10x
    python3 bot.py close BTC Buy             # Manual: close Buy BTCUSDT
    python3 bot.py close-all                 # Close all open positions
    python3 bot.py positions                 # List all open positions
    python3 bot.py stats                     # Show trading performance stats
    python3 bot.py parse "BUY BTC 65000 10x" # Test signal parsing (no execution)
"""
import argparse
import logging
import signal
import sys

import config

# --- Logging setup ---
def setup_logging():
    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(config.LOG_FILE),
    ]
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
        format=fmt,
        handlers=handlers,
    )
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("pybit").setLevel(logging.WARNING)
    logging.getLogger("discord").setLevel(logging.WARNING)

logger = logging.getLogger("bot")

# --- Graceful shutdown ---
shutdown = False

def signal_handler(sig, frame):
    global shutdown
    logger.info("Shutdown signal received. Stopping...")
    shutdown = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def run_bot():
    """Start the Discord signal listener."""
    setup_logging()

    logger.info("=" * 60)
    logger.info("  BYBIT SIGNAL TRADING BOT")
    logger.info(f"  Signal source: {config.SIGNAL_BOT_NAME} in #{config.SIGNAL_CHANNEL}")
    logger.info(f"  Equity fraction: {config.EQUITY_FRACTION}")
    logger.info(f"  Default leverage: {config.DEFAULT_LEVERAGE}x")
    logger.info(f"  Auto-execute: {config.AUTO_EXECUTE}")
    logger.info(f"  Demo mode: {config.USE_TESTNET}")
    logger.info("=" * 60)

    from signal_listener import start_signal_listener
    start_signal_listener()


def show_status():
    """Show current account status and positions."""
    setup_logging()
    from manual_trade import ManualTrader
    trader = ManualTrader()
    trader.list_positions()


def handle_manual_trade(args):
    """Handle manual trade commands."""
    setup_logging()
    from manual_trade import ManualTrader
    trader = ManualTrader()

    command = args.command

    if command == "open":
        if not args.symbol or not args.side:
            print("  Usage: python3 bot.py open <SYMBOL> <Buy|Sell> <USDT_AMOUNT> <LEVERAGE>")
            print("  Example: python3 bot.py open BTC Buy 500 10")
            return
        amount = float(args.amount) if args.amount else 100.0
        leverage = float(args.leverage) if args.leverage else config.DEFAULT_LEVERAGE
        no_optimize = args.no_optimize if hasattr(args, "no_optimize") else False
        trader.open_trade(args.symbol, args.side, amount, leverage, optimize=not no_optimize)

    elif command == "close":
        if not args.symbol or not args.side:
            print("  Usage: python3 bot.py close <SYMBOL> <Buy|Sell>")
            print("  Example: python3 bot.py close BTC Buy")
            return
        trader.close_trade(args.symbol, args.side)

    elif command == "close-all":
        from signal_listener import SignalExecutor
        executor = SignalExecutor()
        executor._close_all()

    elif command == "positions":
        trader.list_positions()

    elif command == "stats":
        trader.show_stats()

    elif command == "parse":
        from signal_listener import SignalParser
        text = args.text
        result = SignalParser.parse(text)
        if result:
            print(f"\n  Parsed signal:")
            for k, v in result.items():
                print(f"    {k}: {v}")
        else:
            print(f"\n  Could not parse as a trade signal: '{text}'")
        print()


def main():
    parser = argparse.ArgumentParser(description="Bybit Signal Trading Bot — CopyBot#8959")
    subparsers = parser.add_subparsers(dest="command")

    # Default mode flags
    parser.add_argument("--status", action="store_true", help="Show current positions & account")

    # Manual open trade
    open_parser = subparsers.add_parser("open", help="Open a manual trade")
    open_parser.add_argument("symbol", help="Trading pair (e.g. BTC, ETHUSDT)")
    open_parser.add_argument("side", help="Buy or Sell")
    open_parser.add_argument("amount", nargs="?", default="100", help="USDT amount (default: 100)")
    open_parser.add_argument("leverage", nargs="?", default=None, help=f"Leverage (default: {config.DEFAULT_LEVERAGE})")
    open_parser.add_argument("--no-optimize", action="store_true", help="Skip optimizer suggestions")

    # Manual close trade
    close_parser = subparsers.add_parser("close", help="Close a position")
    close_parser.add_argument("symbol", help="Trading pair (e.g. BTC, ETHUSDT)")
    close_parser.add_argument("side", help="Buy or Sell")

    # Close all
    subparsers.add_parser("close-all", help="Close ALL open positions")

    # List positions
    subparsers.add_parser("positions", help="List all open positions with PnL")

    # Show stats
    subparsers.add_parser("stats", help="Show trading performance statistics")

    # Test signal parsing
    parse_parser = subparsers.add_parser("parse", help="Test signal parsing without executing")
    parse_parser.add_argument("text", help="Signal text to parse")

    args = parser.parse_args()

    if args.command in ("open", "close", "close-all", "positions", "stats", "parse"):
        handle_manual_trade(args)
    elif args.status:
        show_status()
    else:
        run_bot()


if __name__ == "__main__":
    main()
