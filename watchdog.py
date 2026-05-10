#!/usr/bin/env python3
"""
VusiD Signal Bot — 24/7 Watchdog
Keeps bot.py alive forever. Restarts it automatically on any crash,
network drop, or unexpected exit.
"""
import subprocess
import sys
import time
import logging
from pathlib import Path
from datetime import datetime

BOT_DIR = Path(__file__).parent
LOG_FILE = BOT_DIR / "watchdog.log"
BOT_SCRIPT = BOT_DIR / "bot.py"
PYTHON = sys.executable

# Setup watchdog logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | WATCHDOG | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger("watchdog")

RESTART_DELAY = 10      # seconds between restarts
MAX_FAST_RESTARTS = 5   # if bot crashes this many times quickly, wait longer
FAST_RESTART_WINDOW = 60  # seconds — "fast" means crashed within this window


def run():
    log.info("=" * 50)
    log.info("  VusiD Signal Bot — Watchdog Started")
    log.info(f"  Bot: {BOT_SCRIPT}")
    log.info(f"  Python: {PYTHON}")
    log.info("=" * 50)

    restart_times = []

    while True:
        now = time.time()

        # Track recent crashes
        restart_times = [t for t in restart_times if now - t < FAST_RESTART_WINDOW]

        if len(restart_times) >= MAX_FAST_RESTARTS:
            wait = 60
            log.warning(f"Bot crashed {MAX_FAST_RESTARTS}x in {FAST_RESTART_WINDOW}s — waiting {wait}s before retry...")
            time.sleep(wait)
            restart_times = []

        log.info("Starting bot.py...")
        try:
            proc = subprocess.Popen(
                [PYTHON, str(BOT_SCRIPT)],
                cwd=str(BOT_DIR),
            )
            log.info(f"Bot running with PID {proc.pid}")
            proc.wait()
            exit_code = proc.returncode
        except Exception as e:
            log.error(f"Failed to start bot: {e}")
            exit_code = -1

        restart_times.append(time.time())
        log.warning(f"Bot exited (code={exit_code}). Restarting in {RESTART_DELAY}s...")
        time.sleep(RESTART_DELAY)


if __name__ == "__main__":
    run()
