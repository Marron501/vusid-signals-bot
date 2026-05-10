"""Minimal Railway health check — confirms Flask + PORT binding works."""
import os
from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/")
def index():
    return "<h1>VusiD Bot is running on Railway</h1><p>PORT: " + str(os.environ.get("PORT", "not set")) + "</p>"

@app.route("/health")
def health():
    return jsonify({"status": "ok", "port": os.environ.get("PORT", "not set")})

port = int(os.environ.get("PORT", 5000))
print(f"Starting on port {port}")
app.run(host="0.0.0.0", port=port, debug=False)
