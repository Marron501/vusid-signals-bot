"""Minimal Railway health check."""
import os
from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/")
def index():
    return "<h1>VusiD Bot is LIVE on Railway!</h1>"

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

# Hardcoded 5000 to match Railway networking setting
PORT = int(os.environ.get("PORT", 5000))
print(f"=== STARTING ON PORT {PORT} ===", flush=True)
app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
