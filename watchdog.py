#!/usr/bin/env python3
"""
VusiD Signal Bot — 24/7 Watchdog
Runs bot.py AND dashboard.py together.
Restarts each automatically if they crash.
"""
import subprocess
import sys
import time
import logging
import threading
from pathlib import Path

BOT_DIR = Path(__file__).parent
LOG_FILE = BOT_DIR / "watchdog.log"
PYTHON = sys.executable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | WATCHDOG | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger("watchdog")

RESTART_DELAY = 10
MAX_FAST_RESTARTS = 5
FAST_RESTART_WINDOW = 60


def run_process(name: str, script: str):
    """Keep a script running forever with auto-restart."""
    restart_times = []
    while True:
        now = time.time()
        restart_times = [t for t in restart_times if now - t < FAST_RESTART_WINDOW]

        if len(restart_times) >= MAX_FAST_RESTARTS:
            log.warning(f"[{name}] Crashed {MAX_FAST_RESTARTS}x fast — waiting 60s...")
            time.sleep(60)
            restart_times = []

        log.info(f"[{name}] Starting {script}...")
        try:
            proc = subprocess.Popen(
                [PYTHON, str(BOT_DIR / script)],
                cwd=str(BOT_DIR),
            )
            log.info(f"[{name}] Running (PID {proc.pid})")
            proc.wait()
            exit_code = proc.returncode
        except Exception as e:
            log.error(f"[{name}] Failed to start: {e}")
            exit_code = -1

        restart_times.append(time.time())
        log.warning(f"[{name}] Exited (code={exit_code}). Restarting in {RESTART_DELAY}s...")
        time.sleep(RESTART_DELAY)


def run():
    log.info("=" * 50)
    log.info("  VusiD Watchdog — Bot + Dashboard")
    log.info("=" * 50)

    # Run bot.py and dashboard.py in parallel threads
    threads = [
        threading.Thread(target=run_process, args=("BOT", "bot.py"), daemon=True),
        threading.Thread(target=run_process, args=("DASHBOARD", "dashboard.py"), daemon=True),
    ]

    for t in threads:
        t.start()

    # Keep main thread alive
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Watchdog stopped.")


if __name__ == "__main__":
    run()
