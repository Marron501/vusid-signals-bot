"""Gunicorn config — reads PORT from environment so Railway works correctly."""
import os

bind = f"0.0.0.0:{os.environ.get('PORT', '5000')}"
workers = 1
threads = 4
timeout = 120
worker_class = "gthread"
accesslog = "-"
errorlog = "-"
loglevel = "info"
