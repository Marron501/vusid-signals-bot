"""
Prolific — VusiD Signals Bot Dashboard
Premium mobile-first trading dashboard.
5 tabs: Home · Positions · Signals · Controls · Logs
"""
from __future__ import annotations
import json, logging, os, subprocess, sys, threading, time
from datetime import datetime
from pathlib import Path

import requests
from flask import Flask, jsonify, render_template_string, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

BASE          = Path(__file__).parent
HISTORY_FILE  = BASE / "trade_history.json"
STATS_FILE    = BASE / "trade_stats.json"
SIGNALS_FILE  = BASE / "signals.json"
LOG_FILE      = BASE / "bot.log"
DISCORD_LOG   = BASE / "bot_discord.log"

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger("prolific")


# ── Bot watchdog ─────────────────────────────

def _run_bot_forever():
    restart_times = []
    while True:
        now = time.time()
        restart_times = [t for t in restart_times if now - t < 60]
        if len(restart_times) >= 5:
            log.warning("[BOT] Fast-crash loop — waiting 60s")
            time.sleep(60); restart_times = []
        log.info("[BOT] Starting bot.py...")
        try:
            proc = subprocess.Popen([sys.executable, str(BASE / "bot.py")],
                                    cwd=str(BASE), env=os.environ.copy())
            log.info(f"[BOT] PID {proc.pid}")
            proc.wait(); code = proc.returncode
        except Exception as e:
            log.error(f"[BOT] Start failed: {e}"); code = -1
        restart_times.append(time.time())
        log.warning(f"[BOT] Exited ({code}). Restarting in 10s...")
        time.sleep(10)

def _keepalive_loop():
    time.sleep(60)
    port = int(os.environ.get("PORT", os.environ.get("DASHBOARD_PORT", 8080)))
    while True:
        try: requests.get(f"http://localhost:{port}/health", timeout=5)
        except: pass
        time.sleep(240)

threading.Thread(target=_run_bot_forever, daemon=True, name="bot").start()
threading.Thread(target=_keepalive_loop,  daemon=True, name="keepalive").start()
log.info("[PROLIFIC] Bot + keepalive threads started")


# ── Lazy helpers ─────────────────────────────

def _cfg():
    import config; return config

def _executor():
    from trade_executor import TradeExecutor; return TradeExecutor()

def _win_rate():
    from signal_listener import get_win_rate; return get_win_rate()


# ── Data helpers ─────────────────────────────

def get_account():
    try:
        ex = _executor()
        equity = float(ex.get_equity())
        positions = ex.get_my_positions()
        pos_list, total_pnl = [], 0.0
        for _, p in positions.items():
            pnl = float(p["unrealisedPnl"]); total_pnl += pnl
            entry = float(p["avgPrice"]); mark = float(ex.get_mark_price(p["symbol"]))
            pct = ((mark - entry) / entry * 100) if p["side"] == "Buy" else ((entry - mark) / entry * 100)
            pos_list.append({"symbol": p["symbol"], "side": p["side"],
                "size": str(p["size"]), "entry": str(p["avgPrice"]),
                "mark": str(round(mark, 6)), "leverage": str(p["leverage"]),
                "pnl": round(pnl, 4), "pct": round(pct, 2)})
        return {"equity": round(equity, 2), "positions": pos_list,
                "total_pnl": round(total_pnl, 4), "error": None}
    except Exception as e:
        return {"equity": 0, "positions": [], "total_pnl": 0, "error": str(e)}

def get_stats():
    win_rate, wins, total = _win_rate()
    s = {"win_rate": round(win_rate * 100, 1), "wins": wins,
         "losses": total - wins, "total": total,
         "total_pnl": 0, "best_trade": None, "worst_trade": None}
    if STATS_FILE.exists():
        try:
            d = json.loads(STATS_FILE.read_text())
            s.update({"total_pnl": round(float(d.get("total_pnl", 0)), 2),
                       "best_trade": d.get("best_trade"), "worst_trade": d.get("worst_trade")})
        except: pass
    return s

def get_history(limit=30):
    if not HISTORY_FILE.exists(): return []
    try: return list(reversed(json.loads(HISTORY_FILE.read_text())))[:limit]
    except: return []

def get_signals(limit=50):
    if not SIGNALS_FILE.exists(): return []
    try: return list(reversed(json.loads(SIGNALS_FILE.read_text())))[:limit]
    except: return []

def get_logs(limit=60):
    for f in [DISCORD_LOG, LOG_FILE]:
        if f.exists():
            try:
                lines = f.read_text().splitlines()
                kw = ["SIGNAL","OPENED","CLOSED","FAILED","connected","Listening",
                      "HEARTBEAT","ONLINE","SKIP","RECOVERED","ERROR","WARNING",
                      "INFO","started","Reconnect","KEEPALIVE"]
                return [l for l in reversed(lines) if any(k in l for k in kw)][:limit]
            except: pass
    return []

def bot_is_online():
    try: return bool(subprocess.check_output(["pgrep","-f","bot.py"],text=True).strip())
    except: return False


# ── HTML ─────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="theme-color" content="#07071a">
<title>Prolific</title>
<style>
:root{
  --bg:#07071a;--surface:#0f0f2a;--card:#13132e;--border:#1d1d45;
  --accent:#7c6af7;--accent2:#a78bfa;--green:#10b981;--red:#ef4444;
  --yellow:#f59e0b;--blue:#3b82f6;--text:#e2e8f0;--muted:#64748b;
  --muted2:#94a3b8;--font:'Inter',system-ui,sans-serif;
  --r:16px;--safe-bottom:env(safe-area-inset-bottom,0px);
}
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
html,body{height:100%;overflow:hidden;background:var(--bg);color:var(--text);font-family:var(--font)}

/* ── SCROLLABLE CONTENT ── */
.app{display:flex;flex-direction:column;height:100%}
.pages{flex:1;overflow:hidden;position:relative}
.page{position:absolute;inset:0;overflow-y:auto;overflow-x:hidden;
  padding:0 0 calc(80px + var(--safe-bottom)) 0;
  display:none;-webkit-overflow-scrolling:touch}
.page.active{display:block}

/* ── TOP BAR ── */
.topbar{background:rgba(7,7,26,.92);backdrop-filter:blur(20px);
  -webkit-backdrop-filter:blur(20px);border-bottom:1px solid var(--border);
  padding:16px 20px 14px;display:flex;align-items:center;
  justify-content:space-between;position:sticky;top:0;z-index:50;
  padding-top:calc(16px + env(safe-area-inset-top,0px))}
.brand{font-size:22px;font-weight:800;letter-spacing:-0.5px;
  background:linear-gradient(135deg,var(--accent2),var(--accent));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent}
.brand-sub{font-size:11px;color:var(--muted);letter-spacing:1px;text-transform:uppercase;margin-top:1px}
.status-pill{display:flex;align-items:center;gap:6px;background:var(--card);
  border:1px solid var(--border);border-radius:20px;padding:6px 12px}
.dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.dot-on{background:var(--green);box-shadow:0 0 8px var(--green);animation:blink 2s infinite}
.dot-off{background:var(--red)}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}
.status-txt{font-size:12px;font-weight:600}

/* ── BOTTOM NAV ── */
.nav{position:fixed;bottom:0;left:0;right:0;z-index:100;
  background:rgba(7,7,26,.95);backdrop-filter:blur(20px);
  -webkit-backdrop-filter:blur(20px);
  border-top:1px solid var(--border);
  padding-bottom:var(--safe-bottom);
  display:grid;grid-template-columns:repeat(5,1fr)}
.nav-btn{display:flex;flex-direction:column;align-items:center;justify-content:center;
  gap:4px;padding:10px 4px;border:none;background:none;color:var(--muted);
  cursor:pointer;font-size:10px;font-weight:600;letter-spacing:.5px;
  text-transform:uppercase;transition:color .2s}
.nav-btn.active{color:var(--accent2)}
.nav-btn svg{width:20px;height:20px;stroke-width:2}

/* ── CARDS ── */
.pad{padding:16px}
.card{background:var(--card);border:1px solid var(--border);border-radius:var(--r);
  padding:16px;margin-bottom:12px;position:relative;overflow:hidden}
.card::before{content:'';position:absolute;top:0;left:0;right:0;height:1px;
  background:linear-gradient(90deg,transparent,rgba(124,106,247,.3),transparent)}
.card-label{font-size:10px;font-weight:700;text-transform:uppercase;
  letter-spacing:1.5px;color:var(--muted);margin-bottom:10px;display:flex;
  align-items:center;gap:6px}
.card-label .dot{width:6px;height:6px}

/* ── HERO EQUITY ── */
.hero{background:linear-gradient(135deg,#0f0f2a,#1a1040);
  border:1px solid rgba(124,106,247,.25);border-radius:var(--r);
  padding:20px;margin-bottom:12px;position:relative;overflow:hidden}
.hero::after{content:'';position:absolute;top:-40px;right:-40px;
  width:120px;height:120px;border-radius:50%;
  background:radial-gradient(circle,rgba(124,106,247,.15),transparent 70%)}
.hero-label{font-size:11px;font-weight:700;text-transform:uppercase;
  letter-spacing:1.5px;color:var(--muted2);margin-bottom:4px}
.hero-val{font-size:42px;font-weight:900;letter-spacing:-2px;line-height:1}
.hero-sub{font-size:13px;color:var(--muted2);margin-top:6px}
.hero-meta{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:14px}
.meta-box{background:rgba(255,255,255,.04);border-radius:10px;padding:10px 12px}
.meta-label{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px}
.meta-val{font-size:16px;font-weight:800;margin-top:2px}

/* ── STAT GRID ── */
.stat-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px}
.stat-box{background:var(--card);border:1px solid var(--border);border-radius:12px;
  padding:14px 10px;text-align:center}
.stat-num{font-size:24px;font-weight:900;line-height:1}
.stat-lbl{font-size:9px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-top:4px}

/* ── WIN RATE BAR ── */
.wr-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.wr-big{font-size:36px;font-weight:900;letter-spacing:-1px}
.wr-badge{padding:5px 14px;border-radius:20px;font-size:12px;font-weight:800}
.wr-pass{background:rgba(16,185,129,.15);color:var(--green);border:1px solid rgba(16,185,129,.3)}
.wr-fail{background:rgba(239,68,68,.15);color:var(--red);border:1px solid rgba(239,68,68,.3)}
.bar-track{background:#1a1a3a;border-radius:8px;height:8px;overflow:hidden;margin-bottom:6px}
.bar-fill{height:100%;border-radius:8px;background:linear-gradient(90deg,var(--accent),var(--green));transition:width .8s cubic-bezier(.4,0,.2,1)}

/* ── POSITION CARD ── */
.pos-card{background:#0f0f28;border:1px solid var(--border);border-radius:12px;
  padding:14px;margin-bottom:10px;position:relative}
.pos-head{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px}
.pos-sym{font-size:17px;font-weight:800;letter-spacing:-.3px}
.pos-pnl{font-size:17px;font-weight:800;text-align:right}
.pos-pct{font-size:11px;font-weight:600;text-align:right;margin-top:1px}
.pos-tags{display:flex;gap:6px;flex-wrap:wrap}
.tag{background:rgba(255,255,255,.06);border-radius:6px;padding:4px 9px;
  font-size:11px;font-weight:600;color:var(--muted2)}
.tag.long{color:var(--green);background:rgba(16,185,129,.1)}
.tag.short{color:var(--red);background:rgba(239,68,68,.1)}
.pos-progress{background:#1a1a3a;border-radius:4px;height:3px;margin-top:10px;overflow:hidden}
.pos-progress-fill{height:100%;border-radius:4px;transition:width .5s}

/* ── SIGNAL ROW ── */
.sig-row{padding:12px 0;border-bottom:1px solid var(--border);display:flex;
  justify-content:space-between;align-items:flex-start;gap:10px}
.sig-row:last-child{border:none}
.sig-left{flex:1;min-width:0}
.sig-sym{font-size:15px;font-weight:800}
.sig-side{font-size:11px;font-weight:700;margin-left:6px}
.sig-time{font-size:10px;color:var(--muted);margin-top:3px}
.sig-content{font-size:11px;color:var(--muted2);margin-top:2px;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:220px}
.badge{padding:4px 10px;border-radius:8px;font-size:10px;font-weight:800;
  text-transform:uppercase;letter-spacing:.5px;white-space:nowrap;flex-shrink:0}
.badge-exec{background:rgba(16,185,129,.15);color:var(--green);border:1px solid rgba(16,185,129,.2)}
.badge-skip{background:rgba(239,68,68,.15);color:var(--red);border:1px solid rgba(239,68,68,.2)}
.badge-rec{background:rgba(124,106,247,.15);color:var(--accent2);border:1px solid rgba(124,106,247,.2)}
.badge-pause{background:rgba(245,158,11,.15);color:var(--yellow);border:1px solid rgba(245,158,11,.2)}

/* ── HISTORY ROW ── */
.hist-row{padding:12px 0;border-bottom:1px solid var(--border);
  display:flex;justify-content:space-between;align-items:center}
.hist-row:last-child{border:none}
.hist-sym{font-size:14px;font-weight:700}
.hist-side{font-size:11px;color:var(--muted);margin-top:2px}
.hist-time{font-size:10px;color:var(--muted);margin-top:2px}

/* ── LOG ── */
.log-container{background:#050510;border-radius:10px;padding:12px;
  max-height:calc(100vh - 300px);overflow-y:auto}
.log-line{font-size:10px;font-family:'SF Mono',monospace;padding:2px 0;
  line-height:1.6;color:#6b7280;word-break:break-all}
.log-line.err{color:#f87171}.log-line.warn{color:#fbbf24}
.log-line.good{color:#34d399}.log-line.info{color:#818cf8}

/* ── CONTROLS ── */
.ctrl-section{margin-bottom:16px}
.ctrl-title{font-size:12px;font-weight:700;text-transform:uppercase;
  letter-spacing:1px;color:var(--muted2);margin-bottom:10px}
.toggle-row{display:flex;justify-content:space-between;align-items:center;
  padding:14px 16px;background:var(--card);border:1px solid var(--border);
  border-radius:12px;margin-bottom:8px}
.toggle-info{flex:1}
.toggle-info strong{font-size:14px;font-weight:700;display:block}
.toggle-info span{font-size:11px;color:var(--muted);margin-top:2px;display:block}
.switch{position:relative;width:50px;height:28px;flex-shrink:0}
.switch input{opacity:0;width:0;height:0}
.switch-slider{position:absolute;inset:0;background:#1e1e45;border-radius:28px;
  cursor:pointer;transition:.3s;border:1px solid var(--border)}
.switch-slider:before{content:'';position:absolute;width:22px;height:22px;
  left:2px;top:2px;background:#fff;border-radius:50%;transition:.3s;box-shadow:0 2px 4px rgba(0,0,0,.3)}
input:checked+.switch-slider{background:rgba(16,185,129,.3);border-color:var(--green)}
input:checked+.switch-slider:before{transform:translateX(22px);background:var(--green)}

.inp-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px}
.inp-wrap .inp-label{font-size:11px;color:var(--muted);margin-bottom:6px;display:block;font-weight:600}
.inp{width:100%;background:#0a0a20;border:1px solid var(--border);border-radius:10px;
  padding:12px 14px;color:var(--text);font-size:15px;font-weight:700;
  -webkit-appearance:none;appearance:none}
.inp:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 3px rgba(124,106,247,.15)}
.inp-full{grid-column:1/-1}
select.inp{background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' fill='%2364748b' viewBox='0 0 16 16'%3E%3Cpath d='M7.247 11.14 2.451 5.658C1.885 5.013 2.345 4 3.204 4h9.592a1 1 0 0 1 .753 1.659l-4.796 5.48a1 1 0 0 1-1.506 0z'/%3E%3C/svg%3E");
  background-repeat:no-repeat;background-position:right 12px center;padding-right:36px}

.btn{width:100%;padding:16px;border:none;border-radius:12px;font-size:15px;
  font-weight:800;cursor:pointer;letter-spacing:.3px;transition:all .15s;
  display:flex;align-items:center;justify-content:center;gap:8px}
.btn:active{transform:scale(.97);opacity:.85}
.btn-primary{background:linear-gradient(135deg,#6d55f5,#8b5cf6);color:#fff;
  box-shadow:0 4px 20px rgba(109,85,245,.3)}
.btn-green{background:rgba(16,185,129,.15);color:var(--green);border:1px solid rgba(16,185,129,.25)}
.btn-red{background:rgba(239,68,68,.12);color:var(--red);border:1px solid rgba(239,68,68,.2)}
.btn-yellow{background:rgba(245,158,11,.12);color:var(--yellow);border:1px solid rgba(245,158,11,.2)}
.btn-ghost{background:rgba(255,255,255,.05);color:var(--muted2);border:1px solid var(--border)}
.btn-sm{padding:11px;font-size:13px;border-radius:10px}
.btn-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px}

/* ── EMPTY STATE ── */
.empty{text-align:center;padding:40px 20px;color:var(--muted)}
.empty-icon{font-size:40px;margin-bottom:10px;display:block}
.empty-text{font-size:14px}

/* ── DIVIDER ── */
.div{height:1px;background:var(--border);margin:14px 0}

/* ── REFRESH ── */
.refresh{text-align:center;font-size:11px;color:var(--muted);
  padding:8px;position:sticky;bottom:90px;pointer-events:none}

/* ── TOAST ── */
#toast{position:fixed;bottom:calc(80px + 16px + var(--safe-bottom));left:16px;right:16px;
  background:#1e1e45;color:var(--text);padding:14px 18px;border-radius:14px;
  font-size:14px;font-weight:600;text-align:center;z-index:999;
  display:none;border:1px solid var(--border);
  box-shadow:0 8px 32px rgba(0,0,0,.5);
  animation:slideup .25s ease}
@keyframes slideup{from{transform:translateY(20px);opacity:0}to{transform:translateY(0);opacity:1}}

/* ── LOADING ── */
.spinner{width:20px;height:20px;border:2px solid var(--border);
  border-top-color:var(--accent);border-radius:50%;animation:spin .7s linear infinite;
  display:inline-block}
@keyframes spin{to{transform:rotate(360deg)}}

/* ── PNL CHART ── */
.chart-wrap{height:80px;margin:12px 0 4px;position:relative}
canvas#pnlChart{width:100%;height:80px}

/* ── SECTION DIVIDER ── */
.section-title{font-size:11px;font-weight:800;text-transform:uppercase;
  letter-spacing:1.5px;color:var(--muted);padding:16px 16px 8px;
  position:sticky;top:0;background:var(--bg);z-index:10}

/* ── PULL INDICATOR ── */
.pull-indicator{text-align:center;padding:8px;font-size:11px;color:var(--muted);
  letter-spacing:.5px}

/* ── OVERVIEW QUICK ACTIONS ── */
.quick-actions{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px}
.qa-btn{background:var(--card);border:1px solid var(--border);border-radius:12px;
  padding:14px 8px;text-align:center;cursor:pointer;transition:all .15s;border:none}
.qa-btn:active{transform:scale(.95);background:#1a1a35}
.qa-icon{font-size:22px;display:block;margin-bottom:4px}
.qa-label{font-size:10px;font-weight:700;text-transform:uppercase;
  letter-spacing:.8px;color:var(--muted2)}

/* ── ACCOUNT INFO ROWS ── */
.info-row{display:flex;justify-content:space-between;align-items:center;
  padding:11px 0;border-bottom:1px solid rgba(29,29,69,.7)}
.info-row:last-child{border:none;padding-bottom:0}
.info-key{font-size:13px;color:var(--muted2)}
.info-val{font-size:13px;font-weight:700;text-align:right}

/* ── SCROLLBAR ── */
::-webkit-scrollbar{width:3px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
</style>
</head>
<body>
<div class="app">

<!-- TOP BAR -->
<div class="topbar">
  <div>
    <div class="brand">Prolific</div>
    <div class="brand-sub">Signals Bot</div>
  </div>
  <div class="status-pill">
    <span class="dot dot-off" id="dot"></span>
    <span class="status-txt" id="status-txt">—</span>
  </div>
</div>

<!-- PAGES -->
<div class="pages">

  <!-- ① HOME -->
  <div class="page active" id="page-home">
    <div class="pull-indicator" id="pull-home">↓ Pull to refresh</div>
    <div class="pad">

      <!-- Hero Equity -->
      <div class="hero">
        <div class="hero-label">Account Equity</div>
        <div class="hero-val" id="h-equity">—</div>
        <div class="hero-sub" id="h-equity-sub">LIVE · Bybit Unified</div>
        <div class="hero-meta">
          <div class="meta-box">
            <div class="meta-label">Per Trade</div>
            <div class="meta-val" id="h-per-trade">—</div>
          </div>
          <div class="meta-box">
            <div class="meta-label">Leverage</div>
            <div class="meta-val" id="h-leverage">—</div>
          </div>
          <div class="meta-box">
            <div class="meta-label">Total PnL</div>
            <div class="meta-val" id="h-tpnl">—</div>
          </div>
          <div class="meta-box">
            <div class="meta-label">Positions</div>
            <div class="meta-val" id="h-pos-count">—</div>
          </div>
        </div>
      </div>

      <!-- Stat Grid -->
      <div class="stat-grid">
        <div class="stat-box">
          <div class="stat-num" style="color:var(--green)" id="s-wins">—</div>
          <div class="stat-lbl">Wins</div>
        </div>
        <div class="stat-box">
          <div class="stat-num" style="color:var(--red)" id="s-losses">—</div>
          <div class="stat-lbl">Losses</div>
        </div>
        <div class="stat-box">
          <div class="stat-num" id="s-total">—</div>
          <div class="stat-lbl">Signals</div>
        </div>
      </div>

      <!-- Win Rate -->
      <div class="card">
        <div class="card-label"><span class="dot dot-on"></span>Win Rate · 70% Filter</div>
        <div class="wr-row">
          <div class="wr-big" id="h-wr">—</div>
          <span class="wr-badge" id="h-wr-badge">—</span>
        </div>
        <div class="bar-track"><div class="bar-fill" id="h-wr-bar" style="width:0"></div></div>
        <div style="font-size:11px;color:var(--muted)" id="h-wr-sub">—</div>
      </div>

      <!-- Quick Actions -->
      <div class="quick-actions">
        <div class="qa-btn" onclick="goTab('positions')">
          <span class="qa-icon">📊</span>
          <div class="qa-label">Positions</div>
        </div>
        <div class="qa-btn" onclick="goTab('signals')">
          <span class="qa-icon">📡</span>
          <div class="qa-label">Signals</div>
        </div>
        <div class="qa-btn" onclick="goTab('controls')">
          <span class="qa-icon">🎮</span>
          <div class="qa-label">Controls</div>
        </div>
      </div>

      <!-- Account Info -->
      <div class="card">
        <div class="card-label">Bot Configuration</div>
        <div class="info-row">
          <span class="info-key">Signal Channel</span>
          <span class="info-val" id="cfg-channel">—</span>
        </div>
        <div class="info-row">
          <span class="info-key">Auto Execute</span>
          <span class="info-val" id="cfg-auto">—</span>
        </div>
        <div class="info-row">
          <span class="info-key">Equity per Trade</span>
          <span class="info-val" id="cfg-equity">—</span>
        </div>
        <div class="info-row">
          <span class="info-key">Leverage</span>
          <span class="info-val" id="cfg-lev">—</span>
        </div>
        <div class="info-row">
          <span class="info-key">Mode</span>
          <span class="info-val" style="color:var(--red)">LIVE</span>
        </div>
        <div class="info-row">
          <span class="info-key">Last Refresh</span>
          <span class="info-val" id="cfg-refresh">—</span>
        </div>
      </div>

      <div style="text-align:center;font-size:11px;color:var(--muted);padding:4px 0 8px" id="countdown-bar">—</div>
    </div>
  </div>

  <!-- ② POSITIONS -->
  <div class="page" id="page-positions">
    <div class="pad">
      <div class="card" style="margin-bottom:12px">
        <div class="card-label">Open Positions</div>
        <div class="wr-row" style="margin-bottom:0">
          <div>
            <div style="font-size:28px;font-weight:900" id="p-count">0</div>
            <div style="font-size:11px;color:var(--muted)">positions open</div>
          </div>
          <div style="text-align:right">
            <div style="font-size:20px;font-weight:900" id="p-tpnl">—</div>
            <div style="font-size:11px;color:var(--muted)">unrealized PnL</div>
          </div>
        </div>
      </div>
      <div id="pos-list"><div class="empty"><span class="empty-icon">🏖️</span><div class="empty-text">No open positions</div></div></div>

      <div class="div"></div>
      <div class="card-label" style="padding:0 0 10px">Trade History</div>
      <div id="hist-list"><div class="empty"><span class="empty-icon">📭</span><div class="empty-text">No history yet</div></div></div>
    </div>
  </div>

  <!-- ③ SIGNALS -->
  <div class="page" id="page-signals">
    <div class="pad">
      <div class="card" style="margin-bottom:12px">
        <div class="card-label">Signal Feed · <span id="sig-count">0</span> total</div>
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-top:4px">
          <div style="text-align:center">
            <div style="font-size:22px;font-weight:900;color:var(--green)" id="sig-exec-count">0</div>
            <div style="font-size:10px;color:var(--muted);margin-top:2px">Executed</div>
          </div>
          <div style="text-align:center">
            <div style="font-size:22px;font-weight:900;color:var(--red)" id="sig-skip-count">0</div>
            <div style="font-size:10px;color:var(--muted);margin-top:2px">Skipped</div>
          </div>
          <div style="text-align:center">
            <div style="font-size:22px;font-weight:900;color:var(--accent2)" id="sig-rec-count">0</div>
            <div style="font-size:10px;color:var(--muted);margin-top:2px">Recovered</div>
          </div>
        </div>
      </div>
      <div id="sig-list"><div class="empty"><span class="empty-icon">📡</span><div class="empty-text">Waiting for signals...</div></div></div>
    </div>
  </div>

  <!-- ④ CONTROLS -->
  <div class="page" id="page-controls">
    <div class="pad">

      <!-- Auto Execute -->
      <div class="ctrl-section">
        <div class="ctrl-title">Bot Settings</div>
        <div class="toggle-row">
          <div class="toggle-info">
            <strong>Auto Execute</strong>
            <span>Automatically trade Discord signals</span>
          </div>
          <label class="switch">
            <input type="checkbox" id="tog-auto" onchange="setAutoExecute(this)">
            <span class="switch-slider"></span>
          </label>
        </div>

        <div class="inp-grid" style="margin-top:10px">
          <div class="inp-wrap">
            <label class="inp-label">Equity % per trade</label>
            <input class="inp" type="number" id="inp-eq" min="1" max="100" placeholder="10">
          </div>
          <div class="inp-wrap">
            <label class="inp-label">Leverage (x)</label>
            <input class="inp" type="number" id="inp-lev" min="1" max="100" placeholder="5">
          </div>
        </div>
        <button class="btn btn-primary" onclick="applySettings()">Apply Settings</button>
      </div>

      <div class="div"></div>

      <!-- Open Trade -->
      <div class="ctrl-section">
        <div class="ctrl-title">Open Manual Trade</div>
        <div class="inp-grid">
          <div class="inp-wrap">
            <label class="inp-label">Symbol</label>
            <input class="inp" type="text" id="inp-sym" placeholder="BTC" autocomplete="off" autocapitalize="characters">
          </div>
          <div class="inp-wrap">
            <label class="inp-label">Direction</label>
            <select class="inp" id="inp-side">
              <option value="Buy">Long ↑</option>
              <option value="Sell">Short ↓</option>
            </select>
          </div>
        </div>
        <div class="btn-grid">
          <button class="btn btn-green btn-sm" onclick="openTrade()">▲ Open Trade</button>
          <button class="btn btn-yellow btn-sm" onclick="closeTrade()">▼ Close Position</button>
        </div>
      </div>

      <div class="div"></div>

      <!-- Emergency -->
      <div class="ctrl-section">
        <div class="ctrl-title">Emergency</div>
        <button class="btn btn-red" onclick="closeAll()" style="margin-bottom:8px">
          🚨 CLOSE ALL POSITIONS
        </button>
        <button class="btn btn-ghost btn-sm" onclick="refreshNow()">🔄 Force Refresh</button>
      </div>

    </div>
  </div>

  <!-- ⑤ LOGS -->
  <div class="page" id="page-logs">
    <div class="pad">
      <div class="card" style="margin-bottom:10px">
        <div class="card-label">Live Activity · <span id="log-count">0</span> lines</div>
        <div style="display:flex;gap:8px;margin-top:6px">
          <button class="btn btn-ghost btn-sm" style="flex:1" onclick="filterLogs('all')">All</button>
          <button class="btn btn-ghost btn-sm" style="flex:1" onclick="filterLogs('error')">Errors</button>
          <button class="btn btn-ghost btn-sm" style="flex:1" onclick="filterLogs('trade')">Trades</button>
        </div>
      </div>
      <div class="log-container" id="log-box">
        <div class="log-line">Loading logs...</div>
      </div>
    </div>
  </div>

</div><!-- /pages -->

<!-- BOTTOM NAV -->
<nav class="nav">
  <button class="nav-btn active" id="nav-home" onclick="goTab('home')">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="m3 9 9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline stroke-linecap="round" stroke-linejoin="round" points="9 22 9 12 15 12 15 22"/></svg>
    Home
  </button>
  <button class="nav-btn" id="nav-positions" onclick="goTab('positions')">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><rect x="3" y="3" width="7" height="7" stroke-linecap="round" stroke-linejoin="round"/><rect x="14" y="3" width="7" height="7" stroke-linecap="round" stroke-linejoin="round"/><rect x="14" y="14" width="7" height="7" stroke-linecap="round" stroke-linejoin="round"/><rect x="3" y="14" width="7" height="7" stroke-linecap="round" stroke-linejoin="round"/></svg>
    Positions
  </button>
  <button class="nav-btn" id="nav-signals" onclick="goTab('signals')">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M8.111 16.404a5.5 5.5 0 0 1 7.778 0M12 20h.01m-7.08-7.071c3.904-3.905 10.236-3.905 14.141 0M1.394 9.393c5.857-5.857 15.355-5.857 21.213 0"/></svg>
    Signals
  </button>
  <button class="nav-btn" id="nav-controls" onclick="goTab('controls')">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M12 6V4m0 2a2 2 0 1 0 0 4m0-4a2 2 0 1 1 0 4m-6 8a2 2 0 1 0 0-4m0 4a2 2 0 1 1 0-4m0 4v2m0-6V4m6 6v10m6-2a2 2 0 1 0 0-4m0 4a2 2 0 1 1 0-4m0 4v2m0-6V4"/></svg>
    Controls
  </button>
  <button class="nav-btn" id="nav-logs" onclick="goTab('logs')">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M9 12h6m-6 4h6m2 5H7a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5.586a1 1 0 0 1 .707.293l5.414 5.414a1 1 0 0 1 .293.707V19a2 2 0 0 1-2 2z"/></svg>
    Logs
  </button>
</nav>

</div><!-- /app -->
<div id="toast"></div>

<script>
// ── State ─────────────────────────────────
let data = null;
let countdown = 15;
let activeTab = 'home';
let allLogs = [];

// ── Tab nav ──────────────────────────────
function goTab(tab) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('page-' + tab).classList.add('active');
  document.getElementById('nav-' + tab).classList.add('active');
  activeTab = tab;
  if (tab === 'logs') renderLogs('all');
}

// ── Toast ────────────────────────────────
function toast(msg, ok=true) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.borderColor = ok ? 'rgba(16,185,129,.4)' : 'rgba(239,68,68,.4)';
  t.style.display = 'block';
  setTimeout(() => t.style.display = 'none', 3000);
}

// ── Fetch & render ───────────────────────
async function fetchData() {
  try {
    const r = await fetch('/api/status');
    data = await r.json();
    render();
  } catch(e) { console.error('Fetch failed', e); }
}

function render() {
  if (!data) return;
  const d = data;
  const eq = d.account.equity;

  // Status dot
  const online = d.bot_online;
  document.getElementById('dot').className = 'dot ' + (online ? 'dot-on' : 'dot-off');
  document.getElementById('status-txt').textContent = online ? 'Online' : 'Offline';

  // ── Home ──
  document.getElementById('h-equity').textContent = eq.toFixed(2) + ' USDT';
  document.getElementById('h-equity-sub').innerHTML =
    '<span style="color:var(--green)">●</span> LIVE · Bybit Unified';
  const perTrade = (eq * d.equity_fraction);
  document.getElementById('h-per-trade').textContent = perTrade.toFixed(2) + ' USDT';
  document.getElementById('h-leverage').textContent = d.default_leverage + 'x';
  const tpnl = d.account.total_pnl;
  const tEl = document.getElementById('h-tpnl');
  tEl.textContent = (tpnl >= 0 ? '+' : '') + tpnl.toFixed(2);
  tEl.style.color = tpnl >= 0 ? 'var(--green)' : 'var(--red)';
  document.getElementById('h-pos-count').textContent = d.account.positions.length;

  document.getElementById('s-wins').textContent   = d.stats.wins;
  document.getElementById('s-losses').textContent = d.stats.losses;
  document.getElementById('s-total').textContent  = d.stats.total;

  const wr = d.stats.win_rate;
  document.getElementById('h-wr').textContent     = wr.toFixed(1) + '%';
  document.getElementById('h-wr-bar').style.width = Math.min(wr, 100) + '%';
  document.getElementById('h-wr-sub').textContent =
    d.stats.wins + 'W / ' + d.stats.losses + 'L / ' + d.stats.total + ' signals';
  const pass = wr >= 70;
  const wb = document.getElementById('h-wr-badge');
  wb.textContent  = pass ? '✅ PASSING' : '❌ FAILING';
  wb.className    = 'wr-badge ' + (pass ? 'wr-pass' : 'wr-fail');

  document.getElementById('cfg-channel').textContent  = '#daily-signals';
  document.getElementById('cfg-auto').innerHTML =
    d.auto_execute
      ? '<span style="color:var(--green)">ON ✅</span>'
      : '<span style="color:var(--red)">OFF 🔕</span>';
  document.getElementById('cfg-equity').textContent  = (d.equity_fraction * 100).toFixed(0) + '%';
  document.getElementById('cfg-lev').textContent     = d.default_leverage + 'x Cross';
  document.getElementById('cfg-refresh').textContent = new Date(d.timestamp).toLocaleTimeString();

  // Controls sync
  document.getElementById('tog-auto').checked = d.auto_execute;
  if (!document.getElementById('inp-eq').value)
    document.getElementById('inp-eq').value   = (d.equity_fraction * 100).toFixed(0);
  if (!document.getElementById('inp-lev').value)
    document.getElementById('inp-lev').value  = d.default_leverage;

  // ── Positions ──
  const positions = d.account.positions;
  document.getElementById('p-count').textContent = positions.length;
  const tpnlEl = document.getElementById('p-tpnl');
  tpnlEl.textContent = (tpnl >= 0 ? '+' : '') + tpnl.toFixed(2) + ' USDT';
  tpnlEl.style.color = tpnl >= 0 ? 'var(--green)' : 'var(--red)';

  const pl = document.getElementById('pos-list');
  if (!positions.length) {
    pl.innerHTML = '<div class="empty"><span class="empty-icon">🏖️</span><div class="empty-text">No open positions</div></div>';
  } else {
    pl.innerHTML = positions.map(p => {
      const pnl    = p.pnl;
      const pnlStr = (pnl >= 0 ? '+' : '') + pnl.toFixed(2) + ' USDT';
      const pct    = p.pct || 0;
      const pctStr = (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%';
      const c      = pnl >= 0 ? 'var(--green)' : 'var(--red)';
      const side   = p.side === 'Buy' ? 'long' : 'short';
      const barW   = Math.min(Math.abs(pct) * 5, 100);
      return `<div class="pos-card">
        <div class="pos-head">
          <div>
            <div class="pos-sym">${p.symbol}</div>
            <div class="pos-tags" style="margin-top:6px">
              <span class="tag ${side}">${p.side}</span>
              <span class="tag">× ${p.leverage}</span>
              <span class="tag">Sz ${p.size}</span>
              <span class="tag">@ ${parseFloat(p.entry).toFixed(4)}</span>
            </div>
          </div>
          <div>
            <div class="pos-pnl" style="color:${c}">${pnlStr}</div>
            <div class="pos-pct" style="color:${c}">${pctStr}</div>
          </div>
        </div>
        <div class="pos-progress">
          <div class="pos-progress-fill" style="width:${barW}%;background:${c}"></div>
        </div>
      </div>`;
    }).join('');
  }

  // History
  const hl = document.getElementById('hist-list');
  if (!d.history || !d.history.length) {
    hl.innerHTML = '<div class="empty"><span class="empty-icon">📭</span><div class="empty-text">No history yet</div></div>';
  } else {
    hl.innerHTML = d.history.map(t => {
      const ts  = new Date(t.timestamp).toLocaleString();
      const isOpen = t.action === 'open';
      const badge = isOpen
        ? '<span class="badge badge-exec">OPEN</span>'
        : '<span class="badge badge-skip">CLOSE</span>';
      return `<div class="hist-row">
        <div>
          <div class="hist-sym">${t.symbol}</div>
          <div class="hist-side">${t.side}</div>
          <div class="hist-time">${ts}</div>
        </div>
        ${badge}
      </div>`;
    }).join('');
  }

  // ── Signals ──
  const sigs = d.signals || [];
  document.getElementById('sig-count').textContent      = sigs.length;
  document.getElementById('sig-exec-count').textContent = sigs.filter(s => s.executed).length;
  document.getElementById('sig-skip-count').textContent = sigs.filter(s => !s.executed && s.reason !== 'AUTO_EXECUTE=off').length;
  document.getElementById('sig-rec-count').textContent  = sigs.filter(s => s.source === 'recovery').length;

  const sl = document.getElementById('sig-list');
  if (!sigs.length) {
    sl.innerHTML = '<div class="empty"><span class="empty-icon">📡</span><div class="empty-text">Waiting for signals...</div></div>';
  } else {
    sl.innerHTML = sigs.map(s => {
      const sig    = s.signal || {};
      const symbol = sig.symbol || '?';
      const side   = sig.side   || sig.action || '?';
      const ts     = new Date(s.timestamp).toLocaleString();
      const isRec  = s.source === 'recovery';
      const exec   = s.executed;
      const pause  = s.reason === 'AUTO_EXECUTE=off';
      let badge, sideColor;
      if (isRec && exec)        badge = '<span class="badge badge-rec">⏪ Recovered</span>';
      else if (exec)            badge = '<span class="badge badge-exec">✅ Executed</span>';
      else if (pause)           badge = '<span class="badge badge-pause">⏸ Paused</span>';
      else                      badge = '<span class="badge badge-skip">⛔ Skipped</span>';
      sideColor = side === 'Buy' ? 'var(--green)' : (side === 'Sell' ? 'var(--red)' : 'var(--muted2)');
      return `<div class="sig-row">
        <div class="sig-left">
          <div><span class="sig-sym">${symbol}</span>
            <span class="sig-side" style="color:${sideColor}">${side}</span></div>
          <div class="sig-time">${ts}${isRec ? ' · recovered' : ''}</div>
          <div class="sig-content">${(s.content || '').slice(0, 80)}</div>
        </div>
        ${badge}
      </div>`;
    }).join('');
  }

  // Logs
  allLogs = d.logs || [];
  document.getElementById('log-count').textContent = allLogs.length;
  if (activeTab === 'logs') renderLogs('all');
}

function renderLogs(filter) {
  const lb  = document.getElementById('log-box');
  let lines = allLogs;
  if (filter === 'error') lines = allLogs.filter(l => l.includes('ERROR') || l.includes('FAIL'));
  if (filter === 'trade') lines = allLogs.filter(l =>
    l.includes('OPENED') || l.includes('CLOSED') || l.includes('SIGNAL') || l.includes('EXECUTED'));
  if (!lines.length) {
    lb.innerHTML = '<div class="log-line" style="color:var(--muted)">No matching logs</div>';
    return;
  }
  lb.innerHTML = lines.map(line => {
    let cls = 'log-line';
    if (line.includes('ERROR') || line.includes('FAIL') || line.includes('❌')) cls += ' err';
    else if (line.includes('WARNING') || line.includes('SKIP') || line.includes('⚠')) cls += ' warn';
    else if (line.includes('OPENED') || line.includes('connected') || line.includes('✅') || line.includes('HEARTBEAT')) cls += ' good';
    else if (line.includes('INFO') || line.includes('Discord')) cls += ' info';
    return `<div class="${cls}">${line.replace(/</g,'&lt;')}</div>`;
  }).join('');
  lb.scrollTop = lb.scrollHeight;
}

function filterLogs(f) {
  document.querySelectorAll('#page-logs .btn-ghost').forEach(b => b.style.opacity='.5');
  event.target.style.opacity='1';
  renderLogs(f);
}

// ── Auto refresh ─────────────────────────
function tick() {
  countdown--;
  const bar = document.getElementById('countdown-bar');
  if (bar) bar.textContent = `Refreshes in ${countdown}s · ${new Date().toLocaleTimeString()}`;
  if (countdown <= 0) { countdown = 15; fetchData(); }
}

function refreshNow() { countdown = 15; fetchData(); toast('Refreshed ✅'); }

// ── Controls ─────────────────────────────
async function setAutoExecute(el) {
  const r = await fetch('/api/settings', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({auto_execute: el.checked})
  });
  toast(el.checked ? '✅ Auto Execute ON' : '🔕 Auto Execute OFF', el.checked);
  countdown = 2;
}

async function applySettings() {
  const eq  = parseFloat(document.getElementById('inp-eq').value) / 100;
  const lev = parseFloat(document.getElementById('inp-lev').value);
  if (!eq || !lev || eq <= 0 || lev < 1) { toast('Enter valid values', false); return; }
  await fetch('/api/settings', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({equity_fraction: eq, default_leverage: lev})
  });
  toast(`✅ ${(eq*100).toFixed(0)}% equity · ${lev}x leverage applied`);
  countdown = 2;
}

async function openTrade() {
  let sym  = document.getElementById('inp-sym').value.trim().toUpperCase();
  const side = document.getElementById('inp-side').value;
  if (!sym) { toast('Enter a symbol', false); return; }
  toast('Opening ' + side + ' ' + sym + '...');
  const r = await fetch('/api/trade', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({symbol: sym, side})
  });
  const d = await r.json();
  if (d.success) { toast(`✅ ${side} ${d.symbol} @ ${parseFloat(d.entry).toFixed(4)}`); countdown=2; }
  else toast('❌ ' + (d.error || 'Failed'), false);
}

async function closeTrade() {
  let sym = document.getElementById('inp-sym').value.trim().toUpperCase();
  if (!sym) { toast('Enter a symbol to close', false); return; }
  if (!sym.endsWith('USDT')) sym += 'USDT';
  toast('Closing ' + sym + '...');
  const r = await fetch('/api/close', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({symbol: sym})
  });
  const d = await r.json();
  if (d.success) { toast('✅ Closed ' + sym); countdown=2; }
  else toast('❌ ' + (d.error || 'Failed'), false);
}

async function closeAll() {
  if (!confirm('Close ALL open positions now?')) return;
  toast('Closing all positions...');
  const r = await fetch('/api/close-all', {method:'POST'});
  const d = await r.json();
  toast(d.success ? '✅ All closed' : '❌ Some failed', d.success);
  countdown = 2;
}

// ── Init ──────────────────────────────────
fetchData();
setInterval(tick, 1000);
</script>
</body>
</html>"""


# ── Routes ───────────────────────────────────

@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/health")
def health():
    return jsonify({"status": "ok", "ts": datetime.now().isoformat()})

@app.route("/api/status")
def api_status():
    cfg = _cfg()
    return jsonify({
        "bot_online":       bot_is_online(),
        "timestamp":        datetime.now().isoformat(),
        "account":          get_account(),
        "stats":            get_stats(),
        "history":          get_history(),
        "signals":          get_signals(),
        "logs":             get_logs(),
        "equity_fraction":  cfg.EQUITY_FRACTION,
        "default_leverage": cfg.DEFAULT_LEVERAGE,
        "signal_channel":   cfg.SIGNAL_CHANNEL,
        "auto_execute":     cfg.AUTO_EXECUTE,
    })

@app.route("/api/settings", methods=["POST"])
def api_settings():
    import config as c
    d = request.get_json() or {}
    if "auto_execute"     in d: c.AUTO_EXECUTE    = bool(d["auto_execute"])
    if "equity_fraction"  in d:
        v = float(d["equity_fraction"])
        if 0 < v <= 1: c.EQUITY_FRACTION = v
    if "default_leverage" in d:
        v = float(d["default_leverage"])
        if 1 <= v <= 100: c.DEFAULT_LEVERAGE = v
    return jsonify({"success": True, "auto_execute": c.AUTO_EXECUTE,
                    "equity_fraction": c.EQUITY_FRACTION, "default_leverage": c.DEFAULT_LEVERAGE})

@app.route("/api/trade", methods=["POST"])
def api_trade():
    from decimal import Decimal
    d = request.get_json() or {}
    sym  = d.get("symbol","").upper().strip()
    side = d.get("side","Buy")
    if not sym: return jsonify({"success": False, "error": "Symbol required"})
    if not sym.endswith("USDT"): sym += "USDT"
    try:
        cfg = _cfg(); ex = _executor()
        cost = ex.get_equity() * Decimal(str(cfg.EQUITY_FRACTION))
        lev  = Decimal(str(cfg.DEFAULT_LEVERAGE))
        ok   = ex.open_position(sym, side, cost, lev)
        if ok:
            return jsonify({"success": True, "symbol": sym, "side": side,
                            "entry": str(ex.get_mark_price(sym)), "cost": str(round(float(cost),2))})
        return jsonify({"success": False, "error": "Order failed — check logs"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/api/close", methods=["POST"])
def api_close():
    d = request.get_json() or {}
    sym = d.get("symbol","").upper().strip()
    if not sym: return jsonify({"success": False, "error": "Symbol required"})
    if not sym.endswith("USDT"): sym += "USDT"
    try:
        from signal_listener import SignalExecutor
        return jsonify({"success": SignalExecutor()._close(sym), "symbol": sym})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/api/close-all", methods=["POST"])
def api_close_all():
    try:
        from signal_listener import SignalExecutor
        return jsonify({"success": SignalExecutor()._close_all()})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", os.environ.get("DASHBOARD_PORT", 8080)))
    print(f"\n  🚀 Prolific Dashboard → http://0.0.0.0:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
