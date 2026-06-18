"""Gunicorn config — reads PORT from environment so Railway works correctly."""
import os

bind = f"0.0.0.0:{os.environ.get('PORT', '8080')}"
workers = 1
threads = 4
timeout = 120
worker_class = "gthread"
accesslog = "-"
errorlog = "-"
loglevel = "info"


def on_starting(server):
    """Run one-time migrations before workers start."""
    try:
        from accounts_manager import migrate_testnet_to_demo
        migrate_testnet_to_demo()
    except Exception as e:
        print(f"[startup] migration error: {e}")
