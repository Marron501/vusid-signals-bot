"""
VusiD Signals Bot — Production Dashboard
- Manual trade controls (open / close / close-all)
- Toggle AUTO_EXECUTE, adjust equity % and leverage live
- Signal history feed (every received signal with status)
- Self-ping keepalive every 4 min (prevents Railway sleep)
- Live account equity, positions, PnL, win rate, logs
"""
from __future__ import annotations
import json
import logging
import os
import subprocess
import sys
import threading
import time
import requests
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, render_template_string, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

BASE           = Path(__file__).parent
HISTORY_FILE   = BASE / "trade_history.json"
STATS_FILE     = BASE / "trade_stats.json"
SIGNALS_FILE   = BASE / "signals.json"
LOG_FILE       = BASE / "bot.log"
DISCORD_LOG    = BASE / "bot_discord.log"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(name)s | %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger("dashboard")


# ─────────────────────────────────────────────
# Bot watchdog thread
# ─────────────────────────────────────────────

def _run_bot_forever():
    while True:
        restart_times = []
        FAST_WINDOW, MAX_FAST, DELAY = 60, 5, 10
        now = time.time()
        restart_times = [t for t in restart_times if now - t < FAST_WINDOW]
        if len(restart_times) >= MAX_FAST:
            log.warning("[BOT] Crashed too fast — waiting 60s...")
            time.sleep(60)
            restart_times = []
        log.info("[BOT] Starting bot.py...")
        try:
            env  = os.environ.copy()
            proc = subprocess.Popen([sys.executable, str(BASE / "bot.py")],
                                    cwd=str(BASE), env=env)
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


# ─────────────────────────────────────────────
# Self-ping keepalive (prevents Railway sleep)
# ─────────────────────────────────────────────

def _keepalive_loop():
    time.sleep(60)  # wait for Flask to start
    port = int(os.environ.get("PORT", os.environ.get("DASHBOARD_PORT", 8080)))
    url  = f"http://localhost:{port}/health"
    while True:
        try:
            requests.get(url, timeout=5)
            log.info(f"[KEEPALIVE] ping ✅")
        except Exception as e:
            log.warning(f"[KEEPALIVE] ping failed: {e}")
        time.sleep(240)  # every 4 minutes


def start_keepalive_thread():
    t = threading.Thread(target=_keepalive_loop, daemon=True, name="keepalive")
    t.start()
    log.info("[DASHBOARD] Keepalive thread started.")


# Start both at module level (gunicorn compatibility)
start_bot_thread()
start_keepalive_thread()


# ─────────────────────────────────────────────
# Lazy imports
# ─────────────────────────────────────────────

def _cfg():
    import config
    return config

def _executor():
    from trade_executor import TradeExecutor
    return TradeExecutor()

def _win_rate():
    from signal_listener import get_win_rate
    return get_win_rate()


# ─────────────────────────────────────────────
# Data helpers
# ─────────────────────────────────────────────

def get_account():
    try:
        ex        = _executor()
        equity    = float(ex.get_equity())
        positions = ex.get_my_positions()
        pos_list  = []
        total_pnl = 0.0
        for key, p in positions.items():
            pnl       = float(p["unrealisedPnl"])
            total_pnl += pnl
            pos_list.append({
                "symbol":   p["symbol"],
                "side":     p["side"],
                "size":     str(p["size"]),
                "entry":    str(p["avgPrice"]),
                "leverage": str(p["leverage"]),
                "pnl":      round(pnl, 4),
            })
        return {"equity": round(equity, 2), "positions": pos_list,
                "total_pnl": round(total_pnl, 4), "error": None}
    except Exception as e:
        return {"equity": 0, "positions": [], "total_pnl": 0, "error": str(e)}


def get_stats():
    win_rate, wins, total = _win_rate()
    losses = total - wins
    stats  = {"win_rate": round(win_rate * 100, 1), "wins": wins,
               "losses": losses, "total": total}
    if STATS_FILE.exists():
        try:
            with open(STATS_FILE) as f:
                s = json.load(f)
            stats["total_pnl"]   = round(float(s.get("total_pnl", 0)), 2)
            stats["avg_win"]     = round(float(s.get("avg_win", 0)), 2)
            stats["avg_loss"]    = round(float(s.get("avg_loss", 0)), 2)
            stats["best_trade"]  = s.get("best_trade")
            stats["worst_trade"] = s.get("worst_trade")
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


def get_signals(limit=30):
    if not SIGNALS_FILE.exists():
        return []
    try:
        with open(SIGNALS_FILE) as f:
            s = json.load(f)
        return list(reversed(s))[:limit]
    except Exception:
        return []


def get_recent_logs(limit=40):
    lines = []
    for log_file in [DISCORD_LOG, LOG_FILE]:
        if log_file.exists():
            try:
                with open(log_file) as f:
                    all_lines = f.readlines()
                keywords = ["SIGNAL", "OPENED", "CLOSED", "FAILED", "connected",
                            "Listening", "Win rate", "HEARTBEAT", "ALIVE",
                            "ONLINE", "execution", "Reconnect", "INFO", "WARNING",
                            "ERROR", "RECOVERED", "SKIP", "started", "KEEPALIVE"]
                for line in reversed(all_lines):
                    line = line.strip()
                    if line and any(k in line for k in keywords):
                        lines.append(line)
                    if len(lines) >= limit:
                        break
                break
            except Exception:
                pass
    return lines


def bot_is_online():
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
  *{box-sizing:border-box;margin:0;padding:0}
  :root{
    --bg:#0d0d1a;--card:#141428;--border:#1e1e3a;--accent:#6c5ce7;
    --green:#00b894;--red:#d63031;--yellow:#fdcb6e;--text:#e0e0f0;
    --muted:#666688;--orange:#e17055;
  }
  body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;min-height:100vh}
  .topbar{background:var(--card);border-bottom:1px solid var(--border);padding:14px 16px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}
  .topbar h1{font-size:17px;font-weight:700}.topbar h1 span{color:var(--accent)}
  .status-dot{width:10px;height:10px;border-radius:50%;display:inline-block;margin-right:6px}
  .dot-green{background:var(--green);box-shadow:0 0 6px var(--green);animation:pulse 2s infinite}
  .dot-red{background:var(--red)}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
  .container{padding:12px;max-width:500px;margin:0 auto}
  .card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:16px;margin-bottom:12px}
  .card-title{font-size:11px;text-transform:uppercase;letter-spacing:1.2px;color:var(--muted);margin-bottom:12px;font-weight:600}
  .big-num{font-size:30px;font-weight:800;letter-spacing:-1px}
  .big-num.green{color:var(--green)}.big-num.red{color:var(--red)}
  .sub{font-size:12px;color:var(--muted);margin-top:2px}
  .grid3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:12px}
  .stat-card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:14px 12px;text-align:center}
  .stat-val{font-size:22px;font-weight:800}.stat-lbl{font-size:10px;color:var(--muted);margin-top:3px;text-transform:uppercase;letter-spacing:.8px}
  .pos-row{background:#1a1a30;border-radius:10px;padding:12px;margin-bottom:8px}
  .pos-top{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
  .pos-symbol{font-weight:700;font-size:15px}.pos-pnl{font-weight:700;font-size:15px}
  .pos-bot{display:flex;gap:8px;flex-wrap:wrap}
  .tag{background:#23234a;border-radius:6px;padding:3px 8px;font-size:11px;color:var(--muted)}
  .tag.sell{color:var(--red)}.tag.buy{color:var(--green)}
  .log-box{background:#0a0a18;border-radius:10px;padding:10px;max-height:200px;overflow-y:auto}
  .log-line{font-size:10px;font-family:monospace;color:#9090b0;padding:2px 0;line-height:1.5}
  .log-line.warn{color:var(--yellow)}.log-line.error{color:var(--red)}.log-line.good{color:var(--green)}
  .winrate-bar{background:#1a1a30;border-radius:8px;height:10px;margin:8px 0;overflow:hidden}
  .winrate-fill{height:100%;border-radius:8px;background:linear-gradient(90deg,var(--accent),var(--green));transition:width .5s}
  .filter-badge{display:inline-block;padding:4px 12px;border-radius:20px;font-size:12px;font-weight:700}
  .filter-pass{background:#1a3a2a;color:var(--green)}.filter-fail{background:#3a1a1a;color:var(--red)}
  .divider{height:1px;background:var(--border);margin:10px 0}
  .empty{text-align:center;color:var(--muted);font-size:13px;padding:16px 0}
  .info-row{display:flex;justify-content:space-between;font-size:13px;padding:5px 0}
  .info-row span:last-child{color:var(--text);font-weight:600}
  .refresh-bar{text-align:center;font-size:11px;color:var(--muted);padding:8px}
  /* Controls */
  .ctrl-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px}
  .btn{border:none;border-radius:10px;padding:12px;font-size:13px;font-weight:700;cursor:pointer;width:100%;transition:opacity .2s}
  .btn:active{opacity:.7}
  .btn-green{background:#1a3a2a;color:var(--green);border:1px solid var(--green)}
  .btn-red{background:#3a1a1a;color:var(--red);border:1px solid var(--red)}
  .btn-orange{background:#3a2a1a;color:var(--orange);border:1px solid var(--orange)}
  .btn-accent{background:#1e1a3a;color:var(--accent);border:1px solid var(--accent)}
  .btn-yellow{background:#2a2a1a;color:var(--yellow);border:1px solid var(--yellow)}
  .input-row{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px}
  .inp{background:#0a0a18;border:1px solid var(--border);border-radius:8px;padding:10px;color:var(--text);font-size:13px;width:100%}
  .inp:focus{outline:none;border-color:var(--accent)}
  .toggle-row{display:flex;justify-content:space-between;align-items:center;padding:8px 0}
  .toggle{position:relative;display:inline-block;width:48px;height:26px}
  .toggle input{opacity:0;width:0;height:0}
  .slider{position:absolute;cursor:pointer;top:0;left:0;right:0;bottom:0;background:#23234a;border-radius:26px;transition:.3s}
  .slider:before{position:absolute;content:"";height:20px;width:20px;left:3px;bottom:3px;background:#fff;border-radius:50%;transition:.3s}
  input:checked+.slider{background:var(--green)}
  input:checked+.slider:before{transform:translateX(22px)}
  .signal-row{padding:9px 0;border-bottom:1px solid var(--border);font-size:12px}
  .signal-row:last-child{border-bottom:none}
  .sig-top{display:flex;justify-content:space-between;align-items:center;margin-bottom:3px}
  .sig-symbol{font-weight:700;font-size:13px}
  .sig-badge{padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700}
  .sig-exec{background:#1a3a2a;color:var(--green)}
  .sig-skip{background:#3a1a1a;color:var(--red)}
  .sig-pending{background:#2a2a1a;color:var(--yellow)}
  .toast{position:fixed;bottom:20px;left:50%;transform:translateX(-50%);background:#23234a;color:var(--text);padding:12px 24px;border-radius:12px;font-size:13px;z-index:999;display:none;border:1px solid var(--border)}
</style>
</head>
<body>
<div class="topbar">
  <h1>VusiD <span>Signals Bot</span></h1>
  <div style="font-size:13px;display:flex;align-items:center">
    <span class="status-dot dot-green" id="dot"></span>
    <span id="status-text">Loading...</span>
  </div>
</div>

<div class="container">

  <!-- Equity -->
  <div class="card">
    <div class="card-title">Account Equity</div>
    <div class="big-num" id="equity">—</div>
    <div class="sub">LIVE Account · Bybit</div>
    <div class="divider"></div>
    <div class="info-row"><span>Per Trade</span><span id="per-trade">—</span></div>
    <div class="info-row"><span>Leverage</span><span id="leverage">—</span></div>
    <div class="info-row"><span>Total Unrealized PnL</span><span id="total-pnl">—</span></div>
  </div>

  <!-- Stats -->
  <div class="grid3">
    <div class="stat-card"><div class="stat-val green" id="s-wins">—</div><div class="stat-lbl">Wins</div></div>
    <div class="stat-card"><div class="stat-val red"   id="s-losses">—</div><div class="stat-lbl">Losses</div></div>
    <div class="stat-card"><div class="stat-val"       id="s-total">—</div><div class="stat-lbl">Total</div></div>
  </div>

  <!-- Win Rate -->
  <div class="card">
    <div class="card-title">Win Rate · 70% Filter</div>
    <div style="display:flex;justify-content:space-between;align-items:center">
      <div class="big-num" id="win-rate">—</div>
      <span class="filter-badge" id="filter-badge">—</span>
    </div>
    <div class="winrate-bar"><div class="winrate-fill" id="wr-fill" style="width:0%"></div></div>
    <div class="sub" id="wr-sub">—</div>
  </div>

  <!-- Positions -->
  <div class="card">
    <div class="card-title">Open Positions · <span id="pos-count">0</span></div>
    <div id="positions-container"><div class="empty">No open positions</div></div>
  </div>

  <!-- Manual Controls -->
  <div class="card">
    <div class="card-title">Manual Controls</div>

    <!-- Auto Execute Toggle -->
    <div class="toggle-row">
      <span style="font-size:13px;font-weight:600">Auto Execute Signals</span>
      <label class="toggle">
        <input type="checkbox" id="auto-toggle" onchange="toggleAutoExecute(this)">
        <span class="slider"></span>
      </label>
    </div>

    <div class="divider"></div>

    <!-- Equity % and Leverage -->
    <div class="input-row">
      <div>
        <div class="sub" style="margin-bottom:4px">Equity % per trade</div>
        <input class="inp" type="number" id="inp-equity" min="1" max="100" step="1" placeholder="10">
      </div>
      <div>
        <div class="sub" style="margin-bottom:4px">Leverage (x)</div>
        <input class="inp" type="number" id="inp-leverage" min="1" max="100" step="1" placeholder="5">
      </div>
    </div>
    <button class="btn btn-accent" onclick="applySettings()" style="margin-bottom:12px">Apply Settings</button>

    <div class="divider"></div>

    <!-- Open Trade -->
    <div class="sub" style="margin-bottom:6px;font-weight:700;color:var(--text)">Open Manual Trade</div>
    <div class="input-row">
      <input class="inp" type="text" id="inp-symbol" placeholder="e.g. BTC or BTCUSDT">
      <select class="inp" id="inp-side">
        <option value="Buy">Buy (Long)</option>
        <option value="Sell">Sell (Short)</option>
      </select>
    </div>
    <div class="ctrl-grid">
      <button class="btn btn-green" onclick="openTrade()">▲ Open Trade</button>
      <button class="btn btn-orange" onclick="promptClose()">▼ Close Position</button>
    </div>

    <div class="divider"></div>

    <!-- Emergency -->
    <button class="btn btn-red" onclick="closeAll()" style="margin-top:4px">🚨 CLOSE ALL POSITIONS</button>
  </div>

  <!-- Signal Feed -->
  <div class="card">
    <div class="card-title">Signal Feed</div>
    <div id="signals-container"><div class="empty">No signals yet</div></div>
  </div>

  <!-- Trade History -->
  <div class="card">
    <div class="card-title">Trade History</div>
    <div id="history-container"><div class="empty">No trades yet</div></div>
  </div>

  <!-- Live Logs -->
  <div class="card">
    <div class="card-title">Live Logs</div>
    <div class="log-box" id="log-box"><div class="log-line">Loading...</div></div>
  </div>

  <div class="refresh-bar" id="refresh-bar">Auto-refreshes every 15s</div>
  <div style="height:20px"></div>
</div>

<div class="toast" id="toast"></div>

<script>
let countdown = 15;

function showToast(msg, color='var(--green)') {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.borderColor = color;
  t.style.display = 'block';
  setTimeout(() => t.style.display = 'none', 3000);
}

async function loadData() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();

    // Status
    const online = d.bot_online;
    document.getElementById('dot').className = 'status-dot ' + (online ? 'dot-green' : 'dot-red');
    document.getElementById('status-text').textContent = online ? 'ONLINE' : 'OFFLINE';

    // Equity
    const eq = d.account.equity;
    document.getElementById('equity').textContent = eq.toFixed(2) + ' USDT';
    document.getElementById('per-trade').textContent =
      (d.equity_fraction * 100).toFixed(0) + '% = ' + (eq * d.equity_fraction).toFixed(2) + ' USDT';
    document.getElementById('leverage').textContent = d.default_leverage + 'x Cross';
    const tpnl = d.account.total_pnl;
    const tEl = document.getElementById('total-pnl');
    tEl.textContent = (tpnl >= 0 ? '+' : '') + tpnl.toFixed(2) + ' USDT';
    tEl.style.color = tpnl >= 0 ? 'var(--green)' : 'var(--red)';

    // Stats
    document.getElementById('s-wins').textContent   = d.stats.wins;
    document.getElementById('s-losses').textContent = d.stats.losses;
    document.getElementById('s-total').textContent  = d.stats.total;
    const wr = d.stats.win_rate;
    document.getElementById('win-rate').textContent = wr.toFixed(1) + '%';
    document.getElementById('wr-fill').style.width  = Math.min(wr, 100) + '%';
    document.getElementById('wr-sub').textContent   = d.stats.wins + ' wins / ' + d.stats.total + ' total';
    const passing = wr >= 70;
    const fb = document.getElementById('filter-badge');
    fb.textContent  = passing ? '✅ PASSING' : '❌ FAILING';
    fb.className    = 'filter-badge ' + (passing ? 'filter-pass' : 'filter-fail');

    // Positions
    const positions = d.account.positions;
    document.getElementById('pos-count').textContent = positions.length;
    const pc = document.getElementById('positions-container');
    if (!positions.length) {
      pc.innerHTML = '<div class="empty">No open positions</div>';
    } else {
      pc.innerHTML = positions.map(p => {
        const pnl = p.pnl;
        const pnlStr = (pnl >= 0 ? '+' : '') + pnl.toFixed(2) + ' USDT';
        const pnlColor = pnl >= 0 ? 'var(--green)' : 'var(--red)';
        return \`<div class="pos-row">
          <div class="pos-top">
            <span class="pos-symbol">\${p.symbol}</span>
            <span class="pos-pnl" style="color:\${pnlColor}">\${pnlStr}</span>
          </div>
          <div class="pos-bot">
            <span class="tag \${p.side.toLowerCase()}">\${p.side}</span>
            <span class="tag">Size: \${p.size}</span>
            <span class="tag">Entry: \${p.entry}</span>
            <span class="tag">\${p.leverage}x</span>
          </div>
        </div>\`;
      }).join('');
    }

    // Settings state
    document.getElementById('auto-toggle').checked  = d.auto_execute;
    if (!document.getElementById('inp-equity').value)
      document.getElementById('inp-equity').value   = (d.equity_fraction * 100).toFixed(0);
    if (!document.getElementById('inp-leverage').value)
      document.getElementById('inp-leverage').value = d.default_leverage;

    // Signals
    const sc = document.getElementById('signals-container');
    if (!d.signals || !d.signals.length) {
      sc.innerHTML = '<div class="empty">No signals yet</div>';
    } else {
      sc.innerHTML = d.signals.map(s => {
        const sig    = s.signal || {};
        const symbol = sig.symbol || '?';
        const side   = sig.side   || sig.action || '?';
        const ts     = new Date(s.timestamp).toLocaleString();
        const exec   = s.executed;
        const src    = s.source === 'recovery' ? ' ⏪' : '';
        const cls    = exec ? 'sig-exec' : (s.reason === 'AUTO_EXECUTE=off' ? 'sig-pending' : 'sig-skip');
        const lbl    = exec ? 'EXECUTED' : (s.reason === 'AUTO_EXECUTE=off' ? 'PAUSED' : 'SKIPPED');
        return \`<div class="signal-row">
          <div class="sig-top">
            <span class="sig-symbol">\${symbol} <span style="color:var(--muted);font-size:11px">\${side}\${src}</span></span>
            <span class="sig-badge \${cls}">\${lbl}</span>
          </div>
          <div style="font-size:10px;color:var(--muted)">\${ts} · \${s.reason || ''}</div>
        </div>\`;
      }).join('');
    }

    // History
    const hc = document.getElementById('history-container');
    if (!d.history || !d.history.length) {
      hc.innerHTML = '<div class="empty">No trade history yet</div>';
    } else {
      hc.innerHTML = d.history.map(t => {
        const ts  = new Date(t.timestamp).toLocaleString();
        const cls = t.action === 'open' ? 'sig-exec' : 'sig-skip';
        const pnl = t.pnl ? ((parseFloat(t.pnl) >= 0 ? '+' : '') + parseFloat(t.pnl).toFixed(2) + ' USDT') : '';
        return \`<div class="signal-row">
          <div class="sig-top">
            <span class="sig-symbol">\${t.symbol} <span style="color:var(--muted);font-size:11px">\${t.side}</span></span>
            <span class="sig-badge \${cls}">\${t.action.toUpperCase()}</span>
          </div>
          <div style="font-size:10px;color:var(--muted)">\${ts} \${pnl}</div>
        </div>\`;
      }).join('');
    }

    // Logs
    const lb = document.getElementById('log-box');
    if (d.logs && d.logs.length) {
      lb.innerHTML = d.logs.map(line => {
        let cls = 'log-line';
        if (line.includes('ERROR') || line.includes('FAIL') || line.includes('❌')) cls += ' error';
        else if (line.includes('WARNING') || line.includes('SKIP') || line.includes('⚠')) cls += ' warn';
        else if (line.includes('OPENED') || line.includes('connected') || line.includes('✅') || line.includes('HEARTBEAT')) cls += ' good';
        return \`<div class="\${cls}">\${line.replace(/</g,'&lt;')}</div>\`;
      }).join('');
      lb.scrollTop = lb.scrollHeight;
    }

  } catch(e) { console.error(e); }
}

function tick() {
  countdown--;
  document.getElementById('refresh-bar').textContent =
    \`Last updated: \${new Date().toLocaleTimeString()} · Refreshing in \${countdown}s\`;
  if (countdown <= 0) { countdown = 15; loadData(); }
}

// ── Manual controls ──────────────────────────

async function toggleAutoExecute(el) {
  const r = await fetch('/api/settings', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({auto_execute: el.checked})
  });
  const d = await r.json();
  showToast('Auto Execute: ' + (el.checked ? 'ON ✅' : 'OFF 🔕'),
            el.checked ? 'var(--green)' : 'var(--red)');
}

async function applySettings() {
  const eq  = parseFloat(document.getElementById('inp-equity').value) / 100;
  const lev = parseFloat(document.getElementById('inp-leverage').value);
  if (!eq || !lev) { showToast('Enter valid values', 'var(--red)'); return; }
  await fetch('/api/settings', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({equity_fraction: eq, default_leverage: lev})
  });
  showToast(\`✅ Set \${(eq*100).toFixed(0)}% equity · \${lev}x leverage\`);
  countdown = 1;
}

async function openTrade() {
  const symbol = document.getElementById('inp-symbol').value.trim().toUpperCase();
  const side   = document.getElementById('inp-side').value;
  if (!symbol) { showToast('Enter a symbol', 'var(--red)'); return; }
  showToast('Opening trade...');
  const r = await fetch('/api/trade', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({symbol, side})
  });
  const d = await r.json();
  showToast(d.success ? \`✅ Opened \${side} \${symbol}\` : \`❌ Failed: \${d.error}\`,
            d.success ? 'var(--green)' : 'var(--red)');
  if (d.success) countdown = 1;
}

function promptClose() {
  const symbol = document.getElementById('inp-symbol').value.trim().toUpperCase();
  if (!symbol) { showToast('Enter a symbol to close', 'var(--red)'); return; }
  closeSymbol(symbol);
}

async function closeSymbol(symbol) {
  showToast('Closing ' + symbol + '...');
  const r = await fetch('/api/close', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({symbol})
  });
  const d = await r.json();
  showToast(d.success ? \`✅ Closed \${symbol}\` : \`❌ \${d.error}\`,
            d.success ? 'var(--green)' : 'var(--red)');
  if (d.success) countdown = 1;
}

async function closeAll() {
  if (!confirm('Close ALL open positions?')) return;
  showToast('Closing all positions...');
  const r = await fetch('/api/close-all', {method: 'POST'});
  const d = await r.json();
  showToast(d.success ? '✅ All positions closed' : '❌ Some failed',
            d.success ? 'var(--green)' : 'var(--red)');
  if (d.success) countdown = 1;
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


@app.route("/health")
def health():
    return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()})


@app.route("/api/status")
def api_status():
    cfg     = _cfg()
    account = get_account()
    stats   = get_stats()
    history = get_history()
    signals = get_signals()
    logs    = get_recent_logs()
    online  = bot_is_online()
    return jsonify({
        "bot_online":       online,
        "timestamp":        datetime.now().isoformat(),
        "account":          account,
        "stats":            stats,
        "history":          history,
        "signals":          signals,
        "logs":             logs,
        "equity_fraction":  cfg.EQUITY_FRACTION,
        "default_leverage": cfg.DEFAULT_LEVERAGE,
        "signal_channel":   cfg.SIGNAL_CHANNEL,
        "auto_execute":     cfg.AUTO_EXECUTE,
    })


@app.route("/api/settings", methods=["POST"])
def api_settings():
    """Live-adjust AUTO_EXECUTE, EQUITY_FRACTION, DEFAULT_LEVERAGE."""
    import config as cfg_module
    data = request.get_json() or {}
    if "auto_execute" in data:
        cfg_module.AUTO_EXECUTE = bool(data["auto_execute"])
    if "equity_fraction" in data:
        val = float(data["equity_fraction"])
        if 0 < val <= 1:
            cfg_module.EQUITY_FRACTION = val
    if "default_leverage" in data:
        val = float(data["default_leverage"])
        if 1 <= val <= 100:
            cfg_module.DEFAULT_LEVERAGE = val
    return jsonify({
        "success":          True,
        "auto_execute":     cfg_module.AUTO_EXECUTE,
        "equity_fraction":  cfg_module.EQUITY_FRACTION,
        "default_leverage": cfg_module.DEFAULT_LEVERAGE,
    })


@app.route("/api/trade", methods=["POST"])
def api_trade():
    """Manual open trade."""
    from decimal import Decimal
    data   = request.get_json() or {}
    symbol = data.get("symbol", "").upper().strip()
    side   = data.get("side", "Buy")
    if not symbol:
        return jsonify({"success": False, "error": "Symbol required"})
    if not symbol.endswith("USDT"):
        symbol += "USDT"
    try:
        cfg = _cfg()
        ex  = _executor()
        equity   = ex.get_equity()
        cost     = equity * Decimal(str(cfg.EQUITY_FRACTION))
        leverage = Decimal(str(cfg.DEFAULT_LEVERAGE))
        success  = ex.open_position(symbol, side, cost, leverage)
        if success:
            mark = ex.get_mark_price(symbol)
            return jsonify({"success": True, "symbol": symbol, "side": side,
                            "entry": str(mark), "cost": str(round(float(cost), 2))})
        return jsonify({"success": False, "error": "Order failed — check logs"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/close", methods=["POST"])
def api_close():
    """Manual close a position by symbol."""
    data   = request.get_json() or {}
    symbol = data.get("symbol", "").upper().strip()
    if not symbol:
        return jsonify({"success": False, "error": "Symbol required"})
    if not symbol.endswith("USDT"):
        symbol += "USDT"
    try:
        from signal_listener import SignalExecutor
        ex      = SignalExecutor()
        success = ex._close(symbol)
        return jsonify({"success": success, "symbol": symbol})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/close-all", methods=["POST"])
def api_close_all():
    """Close all open positions."""
    try:
        from signal_listener import SignalExecutor
        ex      = SignalExecutor()
        success = ex._close_all()
        return jsonify({"success": success})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", os.environ.get("DASHBOARD_PORT", 8080)))
    print(f"\n  VusiD Dashboard → http://0.0.0.0:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
