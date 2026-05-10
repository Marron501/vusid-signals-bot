"""
VusiD Signals Bot — Web Dashboard
Mobile-first dashboard showing bot status, positions, trades, signals and PnL.
On Railway: this IS the main process. It binds to PORT immediately (satisfies
Railway health check) and spawns bot.py as a background subprocess.
"""
import json
import os
import subprocess
import sys
import threading
import time
import logging
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from flask import Flask, jsonify, render_template_string
from flask_cors import CORS

import config
from trade_executor import TradeExecutor
from signal_listener import get_win_rate

app = Flask(__name__)
CORS(app)

BASE = Path(__file__).parent
log = logging.getLogger("dashboard")

# ─────────────────────────────────────────────
# Background: spawn & watchdog bot.py
# ─────────────────────────────────────────────

def _run_bot_forever():
    """Keep bot.py running in the background with auto-restart."""
    restart_times = []
    FAST_WINDOW = 60
    MAX_FAST = 5
    DELAY = 10

    while True:
        now = time.time()
        restart_times = [t for t in restart_times if now - t < FAST_WINDOW]
        if len(restart_times) >= MAX_FAST:
            log.warning("[BOT] Crashed too fast — waiting 60s...")
            time.sleep(60)
            restart_times = []

        log.info("[BOT] Starting bot.py...")
        try:
            env = os.environ.copy()
            proc = subprocess.Popen(
                [sys.executable, str(BASE / "bot.py")],
                cwd=str(BASE),
                env=env,
            )
            log.info(f"[BOT] Running (PID {proc.pid})")
            proc.wait()
            code = proc.returncode
        except Exception as e:
            log.error(f"[BOT] Failed to start: {e}")
            code = -1

        restart_times.append(time.time())
        log.warning(f"[BOT] Exited (code={code}). Restarting in {DELAY}s...")
        time.sleep(DELAY)


def start_bot_thread():
    t = threading.Thread(target=_run_bot_forever, daemon=True, name="bot-watchdog")
    t.start()
    log.info("[DASHBOARD] Bot watchdog thread started.")


HISTORY_FILE = BASE / "trade_history.json"
STATS_FILE   = BASE / "trade_stats.json"
LOG_FILE     = BASE / "bot.log"
DISCORD_LOG  = BASE / "bot_discord.log"

# ─────────────────────────────────────────────
# Data helpers
# ─────────────────────────────────────────────

def get_account():
    try:
        ex = TradeExecutor()
        equity   = float(ex.get_equity())
        positions = ex.get_my_positions()
        pos_list  = []
        total_pnl = 0.0
        for key, p in positions.items():
            pnl = float(p["unrealisedPnl"])
            total_pnl += pnl
            pos_list.append({
                "symbol":   p["symbol"],
                "side":     p["side"],
                "size":     str(p["size"]),
                "entry":    str(p["avgPrice"]),
                "leverage": str(p["leverage"]),
                "pnl":      round(pnl, 4),
            })
        return {"equity": round(equity, 2), "positions": pos_list, "total_pnl": round(total_pnl, 4), "error": None}
    except Exception as e:
        return {"equity": 0, "positions": [], "total_pnl": 0, "error": str(e)}


def get_stats():
    win_rate, wins, total = get_win_rate()
    losses = total - wins
    stats = {"win_rate": round(win_rate * 100, 1), "wins": wins, "losses": losses, "total": total}
    if STATS_FILE.exists():
        try:
            with open(STATS_FILE) as f:
                s = json.load(f)
            stats["total_pnl"]  = round(float(s.get("total_pnl", 0)), 2)
            stats["avg_win"]    = round(float(s.get("avg_win", 0)), 2)
            stats["avg_loss"]   = round(float(s.get("avg_loss", 0)), 2)
            stats["best_trade"] = s.get("best_trade")
            stats["worst_trade"]= s.get("worst_trade")
        except Exception:
            pass
    return stats


def get_history(limit=20):
    if not HISTORY_FILE.exists():
        return []
    try:
        with open(HISTORY_FILE) as f:
            h = json.load(f)
        return list(reversed(h))[:limit]
    except Exception:
        return []


def get_recent_logs(limit=30):
    """Return last N lines from bot_discord.log that are meaningful."""
    lines = []
    for log in [DISCORD_LOG, LOG_FILE]:
        if log.exists():
            try:
                with open(log) as f:
                    all_lines = f.readlines()
                for line in reversed(all_lines):
                    line = line.strip()
                    if line and any(k in line for k in [
                        "SIGNAL", "OPENED", "CLOSED", "FAILED", "connected",
                        "Listening", "Win rate", "Daily", "ONLINE", "execution",
                        "Reconnecting", "started", "INFO", "WARNING", "ERROR"
                    ]):
                        lines.append(line)
                    if len(lines) >= limit:
                        break
                break
            except Exception:
                pass
    return lines


def bot_is_online():
    """Check if bot process is running."""
    import subprocess
    try:
        out = subprocess.check_output(["pgrep", "-f", "bot.py"], text=True)
        return bool(out.strip())
    except Exception:
        return False

# ─────────────────────────────────────────────
# HTML Template
# ─────────────────────────────────────────────

TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">
<meta name="apple-mobile-web-app-capable" content="yes">
<title>VusiD Signals Bot</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --bg: #0d0d1a;
    --card: #141428;
    --border: #1e1e3a;
    --accent: #6c5ce7;
    --green: #00b894;
    --red: #d63031;
    --yellow: #fdcb6e;
    --text: #e0e0f0;
    --muted: #666688;
    --font: 'Segoe UI', system-ui, -apple-system, sans-serif;
  }
  body { background: var(--bg); color: var(--text); font-family: var(--font); min-height: 100vh; }
  .topbar { background: var(--card); border-bottom: 1px solid var(--border); padding: 14px 16px; display: flex; align-items: center; justify-content: space-between; position: sticky; top: 0; z-index: 100; }
  .topbar h1 { font-size: 17px; font-weight: 700; letter-spacing: 0.3px; }
  .topbar h1 span { color: var(--accent); }
  .status-dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; margin-right: 6px; }
  .dot-green { background: var(--green); box-shadow: 0 0 6px var(--green); animation: pulse 2s infinite; }
  .dot-red   { background: var(--red); }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
  .container { padding: 12px; max-width: 480px; margin: 0 auto; }
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 16px; margin-bottom: 12px; }
  .card-title { font-size: 11px; text-transform: uppercase; letter-spacing: 1.2px; color: var(--muted); margin-bottom: 12px; font-weight: 600; }
  .big-num { font-size: 32px; font-weight: 800; letter-spacing: -1px; }
  .big-num.green { color: var(--green); }
  .big-num.red   { color: var(--red); }
  .sub { font-size: 12px; color: var(--muted); margin-top: 2px; }
  .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 12px; }
  .grid3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 10px; margin-bottom: 12px; }
  .stat-card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 14px 12px; text-align: center; }
  .stat-val  { font-size: 22px; font-weight: 800; }
  .stat-lbl  { font-size: 10px; color: var(--muted); margin-top: 3px; text-transform: uppercase; letter-spacing: 0.8px; }
  .pos-row { background: #1a1a30; border-radius: 10px; padding: 12px; margin-bottom: 8px; }
  .pos-top { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
  .pos-symbol { font-weight: 700; font-size: 15px; }
  .pos-pnl { font-weight: 700; font-size: 15px; }
  .pos-bot { display: flex; gap: 8px; flex-wrap: wrap; }
  .tag { background: #23234a; border-radius: 6px; padding: 3px 8px; font-size: 11px; color: var(--muted); }
  .tag.sell { color: var(--red); }
  .tag.buy  { color: var(--green); }
  .history-row { display: flex; justify-content: space-between; align-items: center; padding: 9px 0; border-bottom: 1px solid var(--border); font-size: 13px; }
  .history-row:last-child { border-bottom: none; }
  .badge { padding: 3px 9px; border-radius: 20px; font-size: 11px; font-weight: 600; }
  .badge-open  { background: #1a3a2a; color: var(--green); }
  .badge-close { background: #3a1a1a; color: var(--red); }
  .log-box { background: #0a0a18; border-radius: 10px; padding: 10px; max-height: 220px; overflow-y: auto; }
  .log-line { font-size: 10px; font-family: monospace; color: #9090b0; padding: 2px 0; line-height: 1.5; }
  .log-line.warn  { color: var(--yellow); }
  .log-line.error { color: var(--red); }
  .log-line.good  { color: var(--green); }
  .winrate-bar { background: #1a1a30; border-radius: 8px; height: 10px; margin: 8px 0; overflow: hidden; }
  .winrate-fill { height: 100%; border-radius: 8px; background: linear-gradient(90deg, var(--accent), var(--green)); transition: width 0.5s; }
  .filter-badge { display: inline-block; padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: 700; }
  .filter-pass { background: #1a3a2a; color: var(--green); }
  .filter-fail { background: #3a1a1a; color: var(--red); }
  .refresh-bar { text-align: center; font-size: 11px; color: var(--muted); padding: 8px; }
  .divider { height: 1px; background: var(--border); margin: 10px 0; }
  .empty { text-align: center; color: var(--muted); font-size: 13px; padding: 16px 0; }
  .info-row { display: flex; justify-content: space-between; font-size: 13px; padding: 5px 0; }
  .info-row span:last-child { color: var(--text); font-weight: 600; }
</style>
</head>
<body>

<div class="topbar">
  <h1>VusiD <span>Signals Bot</span></h1>
  <div id="status-label" style="font-size:13px; display:flex; align-items:center;">
    <span class="status-dot dot-green" id="dot"></span>
    <span id="status-text">Loading...</span>
  </div>
</div>

<div class="container">

  <!-- Equity Card -->
  <div class="card">
    <div class="card-title">Account Equity</div>
    <div class="big-num" id="equity">—</div>
    <div class="sub" id="equity-sub">LIVE Account · Bybit</div>
    <div class="divider"></div>
    <div class="info-row"><span>Per Trade</span><span id="per-trade">—</span></div>
    <div class="info-row"><span>Leverage</span><span id="leverage">—</span></div>
    <div class="info-row"><span>Auto Execute</span><span style="color:var(--green)">ON</span></div>
  </div>

  <!-- Stats Grid -->
  <div class="grid3">
    <div class="stat-card">
      <div class="stat-val green" id="stat-wins">—</div>
      <div class="stat-lbl">Wins</div>
    </div>
    <div class="stat-card">
      <div class="stat-val red" id="stat-losses">—</div>
      <div class="stat-lbl">Losses</div>
    </div>
    <div class="stat-card">
      <div class="stat-val" id="stat-total">—</div>
      <div class="stat-lbl">Total</div>
    </div>
  </div>

  <!-- Win Rate Card -->
  <div class="card">
    <div class="card-title">Win Rate · 70% Filter</div>
    <div style="display:flex; justify-content:space-between; align-items:center;">
      <div class="big-num" id="win-rate">—</div>
      <span class="filter-badge" id="filter-badge">—</span>
    </div>
    <div class="winrate-bar"><div class="winrate-fill" id="winrate-fill" style="width:0%"></div></div>
    <div class="sub" id="win-sub">—</div>
  </div>

  <!-- Open Positions -->
  <div class="card">
    <div class="card-title">Open Positions · <span id="pos-count">0</span></div>
    <div id="positions-container"><div class="empty">No open positions</div></div>
    <div class="divider"></div>
    <div style="display:flex; justify-content:space-between; font-size:13px; font-weight:700;">
      <span style="color:var(--muted)">Total PnL</span>
      <span id="total-pnl">—</span>
    </div>
  </div>

  <!-- Trade History -->
  <div class="card">
    <div class="card-title">Trade History</div>
    <div id="history-container"><div class="empty">No trades yet</div></div>
  </div>

  <!-- Live Logs -->
  <div class="card">
    <div class="card-title">Live Logs</div>
    <div class="log-box" id="log-box"><div class="log-line">Loading logs...</div></div>
  </div>

  <div class="refresh-bar" id="refresh-bar">Auto-refreshes every 15s</div>
  <div style="height:20px"></div>
</div>

<script>
let countdown = 15;

async function loadData() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();

    // Status
    const online = d.bot_online;
    document.getElementById('dot').className = 'status-dot ' + (online ? 'dot-green' : 'dot-red');
    document.getElementById('status-text').textContent = online ? 'ONLINE' : 'OFFLINE';

    // Equity
    document.getElementById('equity').textContent = d.account.equity.toFixed(2) + ' USDT';
    document.getElementById('per-trade').textContent = (d.equity_fraction * 100).toFixed(0) + '% = ' + (d.account.equity * d.equity_fraction).toFixed(2) + ' USDT';
    document.getElementById('leverage').textContent = d.default_leverage + 'x Cross';

    // Stats
    document.getElementById('stat-wins').textContent = d.stats.wins;
    document.getElementById('stat-losses').textContent = d.stats.losses;
    document.getElementById('stat-total').textContent = d.stats.total;
    const wr = d.stats.win_rate;
    document.getElementById('win-rate').textContent = wr.toFixed(1) + '%';
    document.getElementById('winrate-fill').style.width = Math.min(wr, 100) + '%';
    document.getElementById('win-sub').textContent = d.stats.wins + ' wins / ' + d.stats.total + ' total trades';
    const passing = wr >= 70;
    const fb = document.getElementById('filter-badge');
    fb.textContent = passing ? '✅ PASSING' : '❌ FAILING';
    fb.className = 'filter-badge ' + (passing ? 'filter-pass' : 'filter-fail');

    // Positions
    const positions = d.account.positions;
    document.getElementById('pos-count').textContent = positions.length;
    const pc = document.getElementById('positions-container');
    if (positions.length === 0) {
      pc.innerHTML = '<div class="empty">No open positions</div>';
    } else {
      pc.innerHTML = positions.map(p => {
        const pnl = p.pnl;
        const pnlStr = (pnl >= 0 ? '+' : '') + pnl.toFixed(2) + ' USDT';
        const pnlColor = pnl >= 0 ? 'var(--green)' : 'var(--red)';
        const sideClass = p.side === 'Buy' ? 'buy' : 'sell';
        return \`<div class="pos-row">
          <div class="pos-top">
            <span class="pos-symbol">\${p.symbol}</span>
            <span class="pos-pnl" style="color:\${pnlColor}">\${pnlStr}</span>
          </div>
          <div class="pos-bot">
            <span class="tag \${sideClass}">\${p.side}</span>
            <span class="tag">Size: \${p.size}</span>
            <span class="tag">Entry: \${p.entry}</span>
            <span class="tag">\${p.leverage}x</span>
          </div>
        </div>\`;
      }).join('');
    }

    // Total PnL
    const tpnl = d.account.total_pnl;
    const tpnlEl = document.getElementById('total-pnl');
    tpnlEl.textContent = (tpnl >= 0 ? '+' : '') + tpnl.toFixed(2) + ' USDT';
    tpnlEl.style.color = tpnl >= 0 ? 'var(--green)' : 'var(--red)';

    // History
    const hc = document.getElementById('history-container');
    if (!d.history || d.history.length === 0) {
      hc.innerHTML = '<div class="empty">No trade history yet</div>';
    } else {
      hc.innerHTML = d.history.map(t => {
        const ts = new Date(t.timestamp).toLocaleString();
        const badgeClass = t.action === 'open' ? 'badge-open' : 'badge-close';
        const pnl = t.pnl ? ((parseFloat(t.pnl) >= 0 ? '+' : '') + parseFloat(t.pnl).toFixed(2) + ' USDT') : '';
        return \`<div class="history-row">
          <div>
            <div style="font-weight:600">\${t.symbol} <span style="color:var(--muted);font-size:11px">\${t.side}</span></div>
            <div style="font-size:10px;color:var(--muted)">\${ts}</div>
          </div>
          <div style="text-align:right">
            <span class="badge \${badgeClass}">\${t.action.toUpperCase()}</span>
            \${pnl ? \`<div style="font-size:12px;margin-top:2px;color:\${parseFloat(t.pnl)>=0?'var(--green)':'var(--red)'}">\${pnl}</div>\` : ''}
          </div>
        </div>\`;
      }).join('');
    }

    // Logs
    const lb = document.getElementById('log-box');
    if (d.logs && d.logs.length > 0) {
      lb.innerHTML = d.logs.map(line => {
        let cls = 'log-line';
        if (line.includes('ERROR') || line.includes('FAIL')) cls += ' error';
        else if (line.includes('WARNING') || line.includes('SKIP')) cls += ' warn';
        else if (line.includes('OPENED') || line.includes('connected') || line.includes('SUCCESS') || line.includes('ONLINE')) cls += ' good';
        return \`<div class="\${cls}">\${line.replace(/</g,'&lt;')}</div>\`;
      }).join('');
      lb.scrollTop = lb.scrollHeight;
    }

  } catch(e) {
    console.error(e);
  }
}

function tick() {
  countdown--;
  document.getElementById('refresh-bar').textContent = \`Last updated: \${new Date().toLocaleTimeString()} · Refreshing in \${countdown}s\`;
  if (countdown <= 0) {
    countdown = 15;
    loadData();
  }
}

loadData();
setInterval(tick, 1000);
</script>
</body>
</html>"""

# ─────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(TEMPLATE)


@app.route("/api/status")
def api_status():
    account = get_account()
    stats   = get_stats()
    history = get_history()
    logs    = get_recent_logs()
    online  = bot_is_online()

    return jsonify({
        "bot_online":       online,
        "timestamp":        datetime.now().isoformat(),
        "account":          account,
        "stats":            stats,
        "history":          history,
        "logs":             logs,
        "equity_fraction":  config.EQUITY_FRACTION,
        "default_leverage": config.DEFAULT_LEVERAGE,
        "signal_channel":   config.SIGNAL_CHANNEL,
    })


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    # Start bot.py watchdog in background BEFORE Flask binds
    start_bot_thread()

    # Railway injects PORT; fall back to DASHBOARD_PORT, then 5000
    port = int(os.environ.get("PORT", os.environ.get("DASHBOARD_PORT", 5000)))
    print(f"\n  VusiD Dashboard running at http://0.0.0.0:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=False)
