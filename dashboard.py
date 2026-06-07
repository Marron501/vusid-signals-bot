"""
Prolific — VusiD Signals Bot Dashboard
Red & black theme · Dark / Light mode · Full balance display
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

BASE         = Path(__file__).parent
HISTORY_FILE = BASE / "trade_history.json"
STATS_FILE   = BASE / "trade_stats.json"
SIGNALS_FILE = BASE / "signals.json"
LOG_FILE     = BASE / "bot.log"
DISCORD_LOG  = BASE / "bot_discord.log"

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger("prolific")


# ── Bot watchdog ──────────────────────────────────────────────────────────────

def _run_bot_forever():
    restart_times = []
    while True:
        now = time.time()
        restart_times = [t for t in restart_times if now - t < 60]
        if len(restart_times) >= 5:
            log.warning("[BOT] Fast-crash loop — waiting 60s"); time.sleep(60); restart_times = []
        log.info("[BOT] Starting bot.py...")
        try:
            proc = subprocess.Popen([sys.executable, str(BASE / "bot.py")],
                                    cwd=str(BASE), env=os.environ.copy())
            log.info(f"[BOT] PID {proc.pid}"); proc.wait(); code = proc.returncode
        except Exception as e:
            log.error(f"[BOT] Start failed: {e}"); code = -1
        restart_times.append(time.time())
        log.warning(f"[BOT] Exited ({code}). Restarting in 10s..."); time.sleep(10)

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


# ── Helpers ───────────────────────────────────────────────────────────────────

def _cfg():
    import config; return config

def _executor():
    from trade_executor import TradeExecutor; return TradeExecutor()

def _win_rate():
    from signal_listener import get_win_rate; return get_win_rate()

def get_account():
    try:
        ex      = _executor()
        bal     = ex.get_full_balance()
        pos_map = ex.get_my_positions()
        pos_list, total_pnl = [], 0.0
        for _, p in pos_map.items():
            pnl = float(p["unrealisedPnl"]); total_pnl += pnl
            entry = float(p["avgPrice"])
            mark  = float(ex.get_mark_price(p["symbol"]))
            pct   = ((mark-entry)/entry*100) if p["side"]=="Buy" else ((entry-mark)/entry*100)
            pos_list.append({"symbol": p["symbol"], "side": p["side"],
                "size": str(p["size"]), "entry": str(p["avgPrice"]),
                "mark": str(round(mark,6)), "leverage": str(p["leverage"]),
                "pnl": round(pnl,4), "pct": round(pct,2)})
        bal["positions"]  = pos_list
        bal["total_pnl"]  = round(total_pnl, 4)
        return bal
    except Exception as e:
        return {"equity":0,"available":0,"used_margin":0,"unrealised_pnl":0,
                "total_equity_usd":0,"positions":[],"total_pnl":0,"error":str(e)}

def get_stats():
    wr, wins, total = _win_rate()
    s = {"win_rate": round(wr*100,1), "wins": wins, "losses": total-wins, "total": total,
         "total_pnl": 0, "best_trade": None, "worst_trade": None}
    if STATS_FILE.exists():
        try:
            d = json.loads(STATS_FILE.read_text())
            s.update({"total_pnl": round(float(d.get("total_pnl",0)),2),
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
                kw = ["SIGNAL","OPENED","CLOSED","FAILED","connected","Listening",
                      "HEARTBEAT","ONLINE","SKIP","RECOVERED","ERROR","WARNING",
                      "INFO","started","Reconnect","KEEPALIVE"]
                lines = [l for l in f.read_text().splitlines() if any(k in l for k in kw)]
                return list(reversed(lines))[:limit]
            except: pass
    return []

def bot_is_online():
    try: return bool(subprocess.check_output(["pgrep","-f","bot.py"],text=True).strip())
    except: return False


# ── HTML ──────────────────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="theme-color" id="theme-meta" content="#0a0a0a">
<title>Prolific</title>
<style>
/* ── TOKENS ── */
:root[data-theme="dark"] {
  --bg:       #0a0a0a;
  --surface:  #111111;
  --card:     #161616;
  --card2:    #1c1c1c;
  --border:   #2a2a2a;
  --border2:  #333333;
  --accent:   #e53e3e;
  --accent2:  #fc5555;
  --accentbg: rgba(229,62,62,.12);
  --accentbrd:rgba(229,62,62,.3);
  --green:    #22c55e;
  --greenbg:  rgba(34,197,94,.12);
  --red:      #ef4444;
  --redbg:    rgba(239,68,68,.12);
  --yellow:   #f59e0b;
  --text:     #f5f5f5;
  --text2:    #a3a3a3;
  --text3:    #525252;
  --shadow:   rgba(0,0,0,.6);
  --nav-bg:   rgba(10,10,10,.95);
  --top-bg:   rgba(10,10,10,.92);
  --input-bg: #0d0d0d;
}
:root[data-theme="light"] {
  --bg:       #f5f5f5;
  --surface:  #efefef;
  --card:     #ffffff;
  --card2:    #f9f9f9;
  --border:   #e0e0e0;
  --border2:  #d0d0d0;
  --accent:   #dc2626;
  --accent2:  #ef4444;
  --accentbg: rgba(220,38,38,.08);
  --accentbrd:rgba(220,38,38,.25);
  --green:    #16a34a;
  --greenbg:  rgba(22,163,74,.1);
  --red:      #dc2626;
  --redbg:    rgba(220,38,38,.1);
  --yellow:   #d97706;
  --text:     #111111;
  --text2:    #555555;
  --text3:    #aaaaaa;
  --shadow:   rgba(0,0,0,.12);
  --nav-bg:   rgba(255,255,255,.97);
  --top-bg:   rgba(255,255,255,.95);
  --input-bg: #f0f0f0;
}

/* ── RESET ── */
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
html,body{height:100%;overflow:hidden;background:var(--bg);color:var(--text);
  font-family:-apple-system,'Segoe UI',system-ui,sans-serif;transition:background .25s,color .25s}

/* ── LAYOUT ── */
.app{display:flex;flex-direction:column;height:100%}
.pages{flex:1;overflow:hidden;position:relative}
.page{position:absolute;inset:0;overflow-y:auto;overflow-x:hidden;
  padding-bottom:calc(76px + env(safe-area-inset-bottom,0px));
  display:none;-webkit-overflow-scrolling:touch}
.page.active{display:block}
.pad{padding:14px}

/* ── TOPBAR ── */
.topbar{background:var(--top-bg);backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);
  border-bottom:1px solid var(--border);padding:14px 16px;
  padding-top:calc(14px + env(safe-area-inset-top,0px));
  display:flex;align-items:center;justify-content:space-between;
  position:sticky;top:0;z-index:50;transition:background .25s,border-color .25s}
.brand{display:flex;flex-direction:column}
.brand-name{font-size:22px;font-weight:900;letter-spacing:-0.5px;color:var(--accent)}
.brand-sub{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;
  color:var(--text3);margin-top:1px}
.topbar-right{display:flex;align-items:center;gap:10px}
.status-pill{display:flex;align-items:center;gap:6px;background:var(--card);
  border:1px solid var(--border);border-radius:20px;padding:6px 12px;transition:all .25s}
.dot{width:8px;height:8px;border-radius:50%;flex-shrink:0;transition:background .3s}
.dot-on{background:var(--green);box-shadow:0 0 6px var(--green);animation:blink 2s infinite}
.dot-off{background:var(--red)}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.25}}
.status-txt{font-size:12px;font-weight:700;color:var(--text2)}
.theme-btn{width:34px;height:34px;border-radius:50%;border:1px solid var(--border);
  background:var(--card);cursor:pointer;font-size:16px;display:flex;align-items:center;
  justify-content:center;transition:all .2s}
.theme-btn:active{transform:scale(.9)}

/* ── BOTTOM NAV ── */
.nav{position:fixed;bottom:0;left:0;right:0;z-index:100;
  background:var(--nav-bg);backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);
  border-top:1px solid var(--border);
  padding-bottom:env(safe-area-inset-bottom,0px);
  display:grid;grid-template-columns:repeat(5,1fr);transition:background .25s,border-color .25s}
.nav-btn{display:flex;flex-direction:column;align-items:center;justify-content:center;
  gap:3px;padding:9px 4px;border:none;background:none;color:var(--text3);
  cursor:pointer;font-size:9px;font-weight:700;letter-spacing:.5px;text-transform:uppercase;transition:color .2s}
.nav-btn.active{color:var(--accent)}
.nav-btn svg{width:20px;height:20px;stroke-width:2}
.nav-indicator{position:absolute;bottom:0;left:50%;transform:translateX(-50%);
  width:20px;height:2px;background:var(--accent);border-radius:2px 2px 0 0;opacity:0;transition:opacity .2s}
.nav-btn.active .nav-indicator{opacity:1}

/* ── CARDS ── */
.card{background:var(--card);border:1px solid var(--border);border-radius:16px;
  padding:16px;margin-bottom:12px;transition:background .25s,border-color .25s;position:relative}
.card-label{font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:1.5px;
  color:var(--text3);margin-bottom:12px;display:flex;align-items:center;gap:6px}

/* ── BALANCE HERO ── */
.balance-hero{background:var(--card);border:1px solid var(--border);
  border-radius:16px;padding:20px;margin-bottom:12px;
  position:relative;overflow:hidden;transition:all .25s}
.balance-hero::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,var(--accent),#ff8080,var(--accent))}
.balance-hero::after{content:'';position:absolute;top:-60px;right:-30px;
  width:140px;height:140px;border-radius:50%;
  background:radial-gradient(circle,var(--accentbg),transparent 70%);pointer-events:none}
.bal-label{font-size:11px;font-weight:700;text-transform:uppercase;
  letter-spacing:1.5px;color:var(--text3);margin-bottom:6px}
.bal-amount{font-size:46px;font-weight:900;letter-spacing:-2px;line-height:1;color:var(--text)}
.bal-currency{font-size:18px;font-weight:700;color:var(--text3);margin-left:4px}
.bal-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:16px}
.bal-box{background:var(--card2);border:1px solid var(--border);border-radius:12px;padding:12px}
.bal-box-label{font-size:9px;font-weight:800;text-transform:uppercase;
  letter-spacing:1px;color:var(--text3);margin-bottom:4px}
.bal-box-val{font-size:18px;font-weight:800;color:var(--text)}
.bal-box-val.green{color:var(--green)}
.bal-box-val.red{color:var(--red)}
.bal-box-val.accent{color:var(--accent)}

/* ── STAT GRID ── */
.stat-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px}
.stat-box{background:var(--card);border:1px solid var(--border);border-radius:12px;
  padding:14px 10px;text-align:center;transition:all .25s}
.stat-num{font-size:26px;font-weight:900;line-height:1}
.stat-lbl{font-size:9px;text-transform:uppercase;letter-spacing:1px;color:var(--text3);margin-top:4px}

/* ── WIN RATE ── */
.wr-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.wr-big{font-size:38px;font-weight:900;letter-spacing:-1px}
.wr-badge{padding:5px 14px;border-radius:20px;font-size:11px;font-weight:800}
.wr-pass{background:var(--greenbg);color:var(--green);border:1px solid rgba(34,197,94,.25)}
.wr-fail{background:var(--redbg);color:var(--red);border:1px solid rgba(239,68,68,.25)}
.bar-track{background:var(--card2);border-radius:8px;height:8px;overflow:hidden;
  margin-bottom:6px;border:1px solid var(--border)}
.bar-fill{height:100%;border-radius:8px;
  background:linear-gradient(90deg,var(--accent),#ff6b6b);transition:width .8s cubic-bezier(.4,0,.2,1)}

/* ── POSITION CARD ── */
.pos-card{background:var(--card2);border:1px solid var(--border);border-radius:14px;
  padding:14px;margin-bottom:10px;transition:all .25s}
.pos-head{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px}
.pos-sym{font-size:17px;font-weight:800}
.pos-pnl{font-size:17px;font-weight:800;text-align:right}
.pos-pct{font-size:11px;font-weight:700;text-align:right;margin-top:1px}
.pos-tags{display:flex;gap:6px;flex-wrap:wrap;margin-top:6px}
.tag{background:var(--card);border:1px solid var(--border);border-radius:6px;
  padding:3px 9px;font-size:11px;font-weight:600;color:var(--text2)}
.tag.long{color:var(--green);background:var(--greenbg);border-color:rgba(34,197,94,.2)}
.tag.short{color:var(--red);background:var(--redbg);border-color:rgba(239,68,68,.2)}
.tag.accent{color:var(--accent);background:var(--accentbg);border-color:var(--accentbrd)}
.pos-bar{background:var(--card);border-radius:4px;height:3px;margin-top:12px;overflow:hidden;
  border:1px solid var(--border)}
.pos-bar-fill{height:100%;border-radius:4px;transition:width .5s}

/* ── SIGNAL / HISTORY ROW ── */
.row{padding:12px 0;border-bottom:1px solid var(--border);
  display:flex;justify-content:space-between;align-items:flex-start;gap:10px}
.row:last-child{border:none;padding-bottom:0}
.row-left{flex:1;min-width:0}
.row-sym{font-size:15px;font-weight:800}
.row-meta{font-size:11px;color:var(--text3);margin-top:3px}
.row-content{font-size:11px;color:var(--text2);margin-top:2px;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:220px}
.badge{padding:4px 10px;border-radius:8px;font-size:10px;font-weight:800;
  text-transform:uppercase;letter-spacing:.3px;white-space:nowrap;flex-shrink:0}
.b-exec{background:var(--greenbg);color:var(--green);border:1px solid rgba(34,197,94,.2)}
.b-skip{background:var(--redbg);color:var(--red);border:1px solid rgba(239,68,68,.2)}
.b-rec{background:var(--accentbg);color:var(--accent2);border:1px solid var(--accentbrd)}
.b-pause{background:rgba(245,158,11,.12);color:var(--yellow);border:1px solid rgba(245,158,11,.2)}
.b-open{background:var(--accentbg);color:var(--accent);border:1px solid var(--accentbrd)}
.b-close{background:var(--card2);color:var(--text3);border:1px solid var(--border)}

/* ── LOG ── */
.log-box{background:var(--input-bg);border:1px solid var(--border);border-radius:12px;
  padding:12px;max-height:calc(100vh - 280px);overflow-y:auto;transition:all .25s}
.log-line{font-size:10px;font-family:'SF Mono','Fira Code',monospace;
  padding:2px 0;line-height:1.6;color:var(--text3);word-break:break-all}
.log-line.err{color:var(--red)}.log-line.warn{color:var(--yellow)}
.log-line.good{color:var(--green)}.log-line.info{color:#818cf8}

/* ── CONTROLS ── */
.section-lbl{font-size:10px;font-weight:800;text-transform:uppercase;
  letter-spacing:1.5px;color:var(--text3);margin-bottom:10px}
.toggle-row{display:flex;justify-content:space-between;align-items:center;
  padding:14px 16px;background:var(--card);border:1px solid var(--border);
  border-radius:14px;margin-bottom:10px;transition:all .25s}
.toggle-info strong{font-size:14px;font-weight:700;display:block;color:var(--text)}
.toggle-info span{font-size:11px;color:var(--text3);margin-top:2px;display:block}
.switch{position:relative;width:50px;height:28px;flex-shrink:0}
.switch input{opacity:0;width:0;height:0}
.sw-track{position:absolute;inset:0;background:var(--card2);border-radius:28px;
  cursor:pointer;transition:.3s;border:1px solid var(--border)}
.sw-track:before{content:'';position:absolute;width:22px;height:22px;
  left:2px;top:2px;background:var(--text3);border-radius:50%;transition:.3s}
input:checked+.sw-track{background:var(--accentbg);border-color:var(--accent)}
input:checked+.sw-track:before{transform:translateX(22px);background:var(--accent)}

.inp-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px}
.inp-wrap .inp-lbl{font-size:10px;font-weight:700;color:var(--text3);
  text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px;display:block}
.inp{width:100%;background:var(--input-bg);border:1px solid var(--border);border-radius:12px;
  padding:13px 14px;color:var(--text);font-size:15px;font-weight:700;
  -webkit-appearance:none;transition:all .2s}
.inp:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 3px var(--accentbg)}
select.inp option{background:var(--card);color:var(--text)}

.btn{width:100%;padding:16px;border:none;border-radius:14px;font-size:15px;
  font-weight:800;cursor:pointer;letter-spacing:.3px;transition:all .15s;
  display:flex;align-items:center;justify-content:center;gap:8px;color:var(--text)}
.btn:active{transform:scale(.97);opacity:.85}
.btn-accent{background:var(--accent);color:#fff;box-shadow:0 4px 20px rgba(229,62,62,.35)}
.btn-green{background:var(--greenbg);color:var(--green);border:1px solid rgba(34,197,94,.25)}
.btn-red{background:var(--redbg);color:var(--red);border:1px solid rgba(239,68,68,.2)}
.btn-ghost{background:var(--card);color:var(--text2);border:1px solid var(--border)}
.btn-sm{padding:12px;font-size:13px;border-radius:12px}
.btn-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.mb{margin-bottom:10px}

/* ── INFO ROWS ── */
.info-row{display:flex;justify-content:space-between;align-items:center;
  padding:11px 0;border-bottom:1px solid var(--border)}
.info-row:last-child{border:none;padding-bottom:0}
.info-key{font-size:13px;color:var(--text2)}
.info-val{font-size:13px;font-weight:700}

/* ── EMPTY ── */
.empty{text-align:center;padding:36px 20px;color:var(--text3)}
.empty-icon{font-size:38px;display:block;margin-bottom:8px}

/* ── DIVIDER ── */
.div{height:1px;background:var(--border);margin:14px 0}

/* ── TOAST ── */
#toast{position:fixed;bottom:calc(76px + 14px + env(safe-area-inset-bottom,0px));
  left:14px;right:14px;background:var(--card);color:var(--text);
  padding:14px 18px;border-radius:14px;font-size:14px;font-weight:600;
  text-align:center;z-index:999;display:none;border:1px solid var(--border);
  box-shadow:0 8px 32px var(--shadow);animation:slideup .2s ease}
@keyframes slideup{from{transform:translateY(16px);opacity:0}to{transform:translateY(0);opacity:1}}

/* ── COUNTDOWN ── */
.countdown{text-align:center;font-size:11px;color:var(--text3);padding:6px 0 2px}

/* ── QUICK ACTIONS ── */
.qa-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px}
.qa{background:var(--card);border:1px solid var(--border);border-radius:14px;
  padding:14px 8px;text-align:center;cursor:pointer;transition:all .15s;
  display:flex;flex-direction:column;align-items:center;gap:4px}
.qa:active{transform:scale(.95);background:var(--card2)}
.qa-icon{font-size:22px}
.qa-lbl{font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:var(--text3)}

/* ── FILTER PILLS ── */
.pills{display:flex;gap:6px;margin-bottom:12px;overflow-x:auto;padding-bottom:2px}
.pill{padding:7px 14px;border-radius:20px;font-size:11px;font-weight:700;
  border:1px solid var(--border);background:var(--card);color:var(--text3);
  cursor:pointer;white-space:nowrap;transition:all .2s}
.pill.active{background:var(--accentbg);color:var(--accent);border-color:var(--accentbrd)}

::-webkit-scrollbar{width:3px;height:3px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
</style>
</head>
<body>
<div class="app">

<!-- TOPBAR -->
<div class="topbar">
  <div class="brand">
    <div class="brand-name">Prolific</div>
    <div class="brand-sub">Signals Bot</div>
  </div>
  <div class="topbar-right">
    <button class="theme-btn" onclick="toggleTheme()" id="theme-btn" title="Toggle theme">🌙</button>
    <div class="status-pill">
      <span class="dot dot-off" id="dot"></span>
      <span class="status-txt" id="status-txt">—</span>
    </div>
  </div>
</div>

<!-- PAGES -->
<div class="pages">

  <!-- ① HOME -->
  <div class="page active" id="page-home">
    <div class="pad">

      <!-- Balance Hero -->
      <div class="balance-hero">
        <div class="bal-label">Total Balance</div>
        <div style="display:flex;align-items:baseline;gap:4px">
          <div class="bal-amount" id="b-equity">—</div>
          <div class="bal-currency">USDT</div>
        </div>
        <div style="font-size:11px;color:var(--text3);margin-top:4px" id="b-sub">LIVE · Bybit Unified Account</div>
        <div class="bal-grid">
          <div class="bal-box">
            <div class="bal-box-label">Available</div>
            <div class="bal-box-val green" id="b-avail">—</div>
            <div style="font-size:9px;color:var(--text3);margin-top:2px">USDT</div>
          </div>
          <div class="bal-box">
            <div class="bal-box-label">Used Margin</div>
            <div class="bal-box-val accent" id="b-margin">—</div>
            <div style="font-size:9px;color:var(--text3);margin-top:2px">USDT</div>
          </div>
          <div class="bal-box">
            <div class="bal-box-label">Unrealised PnL</div>
            <div class="bal-box-val" id="b-upnl">—</div>
            <div style="font-size:9px;color:var(--text3);margin-top:2px">USDT</div>
          </div>
          <div class="bal-box">
            <div class="bal-box-label">Per Trade</div>
            <div class="bal-box-val accent" id="b-pertrade">—</div>
            <div style="font-size:9px;color:var(--text3);margin-top:2px">USDT @ <span id="b-lev">—</span>x</div>
          </div>
        </div>
      </div>

      <!-- Stats -->
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
        <div class="card-label">Win Rate · 70% Filter</div>
        <div class="wr-row">
          <div class="wr-big" id="h-wr">—</div>
          <span class="wr-badge" id="h-wr-badge">—</span>
        </div>
        <div class="bar-track"><div class="bar-fill" id="h-wr-bar" style="width:0"></div></div>
        <div style="font-size:11px;color:var(--text3)" id="h-wr-sub">—</div>
      </div>

      <!-- Quick actions -->
      <div class="qa-grid">
        <div class="qa" onclick="goTab('positions')">
          <span class="qa-icon">📊</span><span class="qa-lbl">Positions</span>
        </div>
        <div class="qa" onclick="goTab('signals')">
          <span class="qa-icon">📡</span><span class="qa-lbl">Signals</span>
        </div>
        <div class="qa" onclick="goTab('controls')">
          <span class="qa-icon">🎮</span><span class="qa-lbl">Controls</span>
        </div>
      </div>

      <!-- Config -->
      <div class="card">
        <div class="card-label">Configuration</div>
        <div class="info-row"><span class="info-key">Auto Execute</span><span class="info-val" id="cfg-auto">—</span></div>
        <div class="info-row"><span class="info-key">Equity per Trade</span><span class="info-val" id="cfg-eq">—</span></div>
        <div class="info-row"><span class="info-key">Leverage</span><span class="info-val" id="cfg-lev">—</span></div>
        <div class="info-row"><span class="info-key">Channel</span><span class="info-val" style="color:var(--accent)">#daily-signals</span></div>
        <div class="info-row"><span class="info-key">Mode</span><span class="info-val" style="color:var(--red)">LIVE 🔴</span></div>
        <div class="info-row"><span class="info-key">Last Updated</span><span class="info-val" id="cfg-ts">—</span></div>
      </div>

      <div class="countdown" id="cd">—</div>
    </div>
  </div>

  <!-- ② POSITIONS -->
  <div class="page" id="page-positions">
    <div class="pad">
      <div class="card mb">
        <div class="card-label">Portfolio Summary</div>
        <div style="display:flex;justify-content:space-between;align-items:center">
          <div>
            <div style="font-size:32px;font-weight:900" id="p-count">0</div>
            <div style="font-size:11px;color:var(--text3)">open positions</div>
          </div>
          <div style="text-align:right">
            <div style="font-size:24px;font-weight:900" id="p-tpnl">—</div>
            <div style="font-size:11px;color:var(--text3)">unrealized PnL</div>
          </div>
        </div>
      </div>
      <div id="pos-list"><div class="empty"><span class="empty-icon">🏖️</span>No open positions</div></div>
      <div class="div"></div>
      <div class="card-label" style="margin-bottom:10px">Trade History</div>
      <div id="hist-list"><div class="empty"><span class="empty-icon">📭</span>No history yet</div></div>
    </div>
  </div>

  <!-- ③ SIGNALS -->
  <div class="page" id="page-signals">
    <div class="pad">
      <div class="card mb">
        <div class="card-label">Signal Feed</div>
        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:4px">
          <div style="text-align:center">
            <div style="font-size:24px;font-weight:900;color:var(--green)" id="sg-exec">0</div>
            <div style="font-size:9px;text-transform:uppercase;letter-spacing:.8px;color:var(--text3);margin-top:3px">Executed</div>
          </div>
          <div style="text-align:center">
            <div style="font-size:24px;font-weight:900;color:var(--red)" id="sg-skip">0</div>
            <div style="font-size:9px;text-transform:uppercase;letter-spacing:.8px;color:var(--text3);margin-top:3px">Skipped</div>
          </div>
          <div style="text-align:center">
            <div style="font-size:24px;font-weight:900;color:var(--accent)" id="sg-rec">0</div>
            <div style="font-size:9px;text-transform:uppercase;letter-spacing:.8px;color:var(--text3);margin-top:3px">Recovered</div>
          </div>
        </div>
      </div>
      <div id="sig-list"><div class="empty"><span class="empty-icon">📡</span>Waiting for signals...</div></div>
    </div>
  </div>

  <!-- ④ CONTROLS -->
  <div class="page" id="page-controls">
    <div class="pad">

      <div class="section-lbl">Bot Settings</div>
      <div class="toggle-row">
        <div class="toggle-info">
          <strong>Auto Execute Signals</strong>
          <span>Automatically trade Discord signals</span>
        </div>
        <label class="switch">
          <input type="checkbox" id="tog-auto" onchange="setAutoExecute(this)">
          <span class="sw-track"></span>
        </label>
      </div>
      <div class="inp-grid mb">
        <div class="inp-wrap">
          <label class="inp-lbl">Equity % per trade</label>
          <input class="inp" type="number" id="inp-eq" min="1" max="100" placeholder="10">
        </div>
        <div class="inp-wrap">
          <label class="inp-lbl">Leverage (x)</label>
          <input class="inp" type="number" id="inp-lev" min="1" max="100" placeholder="5">
        </div>
      </div>
      <button class="btn btn-accent mb" onclick="applySettings()">Apply Settings</button>

      <div class="div"></div>
      <div class="section-lbl">Manual Trade</div>
      <div class="inp-grid mb">
        <div class="inp-wrap">
          <label class="inp-lbl">Symbol</label>
          <input class="inp" type="text" id="inp-sym" placeholder="BTC" autocomplete="off" autocapitalize="characters">
        </div>
        <div class="inp-wrap">
          <label class="inp-lbl">Direction</label>
          <select class="inp" id="inp-side">
            <option value="Buy">Long ↑</option>
            <option value="Sell">Short ↓</option>
          </select>
        </div>
      </div>
      <div class="btn-grid mb">
        <button class="btn btn-green btn-sm" onclick="openTrade()">▲ Open</button>
        <button class="btn btn-ghost btn-sm" onclick="closeTrade()">▼ Close</button>
      </div>

      <div class="div"></div>
      <div class="section-lbl">Emergency</div>
      <button class="btn btn-red mb" onclick="closeAll()">🚨 Close All Positions</button>
      <button class="btn btn-ghost btn-sm" onclick="refreshNow()">🔄 Force Refresh</button>
    </div>
  </div>

  <!-- ⑤ LOGS -->
  <div class="page" id="page-logs">
    <div class="pad">
      <div class="card mb">
        <div class="card-label">Activity Log · <span id="log-count">0</span> lines</div>
        <div class="pills" id="log-pills">
          <span class="pill active" onclick="filterLogs('all',this)">All</span>
          <span class="pill" onclick="filterLogs('trade',this)">Trades</span>
          <span class="pill" onclick="filterLogs('signal',this)">Signals</span>
          <span class="pill" onclick="filterLogs('error',this)">Errors</span>
        </div>
      </div>
      <div class="log-box" id="log-box"><div class="log-line">Loading...</div></div>
    </div>
  </div>

</div><!-- /pages -->

<!-- BOTTOM NAV -->
<nav class="nav">
  <button class="nav-btn active" id="nav-home" onclick="goTab('home')">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="m3 9 9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline stroke-linecap="round" stroke-linejoin="round" points="9 22 9 12 15 12 15 22"/></svg>
    <span>Home</span><span class="nav-indicator"></span>
  </button>
  <button class="nav-btn" id="nav-positions" onclick="goTab('positions')">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><polyline stroke-linecap="round" stroke-linejoin="round" points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
    <span>Positions</span><span class="nav-indicator"></span>
  </button>
  <button class="nav-btn" id="nav-signals" onclick="goTab('signals')">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M8.111 16.404a5.5 5.5 0 0 1 7.778 0M12 20h.01m-7.08-7.071c3.904-3.905 10.236-3.905 14.141 0M1.394 9.393c5.857-5.857 15.355-5.857 21.213 0"/></svg>
    <span>Signals</span><span class="nav-indicator"></span>
  </button>
  <button class="nav-btn" id="nav-controls" onclick="goTab('controls')">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M12 6V4m0 2a2 2 0 1 0 0 4m0-4a2 2 0 1 1 0 4m-6 8a2 2 0 1 0 0-4m0 4a2 2 0 1 1 0-4m0 4v2m0-6V4m6 6v10m6-2a2 2 0 1 0 0-4m0 4a2 2 0 1 1 0-4m0 4v2m0-6V4"/></svg>
    <span>Controls</span><span class="nav-indicator"></span>
  </button>
  <button class="nav-btn" id="nav-logs" onclick="goTab('logs')">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M9 12h6m-6 4h6m2 5H7a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5.586a1 1 0 0 1 .707.293l5.414 5.414a1 1 0 0 1 .293.707V19a2 2 0 0 1-2 2z"/></svg>
    <span>Logs</span><span class="nav-indicator"></span>
  </button>
</nav>
</div>
<div id="toast"></div>

<script>
// ── Theme ─────────────────────────────────────────────────────────────────────
const stored = localStorage.getItem('theme') || 'dark';
document.documentElement.setAttribute('data-theme', stored);
document.getElementById('theme-meta').content = stored === 'dark' ? '#0a0a0a' : '#f5f5f5';
document.getElementById('theme-btn').textContent = stored === 'dark' ? '☀️' : '🌙';

function toggleTheme() {
  const cur  = document.documentElement.getAttribute('data-theme');
  const next = cur === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('theme', next);
  document.getElementById('theme-btn').textContent = next === 'dark' ? '☀️' : '🌙';
  document.getElementById('theme-meta').content = next === 'dark' ? '#0a0a0a' : '#f5f5f5';
}

// ── State ─────────────────────────────────────────────────────────────────────
let data = null, countdown = 15, activeTab = 'home', allLogs = [];

// ── Tabs ──────────────────────────────────────────────────────────────────────
function goTab(tab) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('page-' + tab).classList.add('active');
  document.getElementById('nav-' + tab).classList.add('active');
  activeTab = tab;
  if (tab === 'logs') renderLogs('all');
}

// ── Toast ─────────────────────────────────────────────────────────────────────
function toast(msg, ok = true) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.borderColor = ok ? 'rgba(34,197,94,.4)' : 'rgba(239,68,68,.4)';
  t.style.display = 'block';
  setTimeout(() => t.style.display = 'none', 3000);
}

// ── Fetch ─────────────────────────────────────────────────────────────────────
async function fetchData() {
  try {
    const r = await fetch('/api/status');
    data = await r.json();
    render();
  } catch(e) { console.error(e); }
}

// ── Render ────────────────────────────────────────────────────────────────────
function render() {
  if (!data) return;
  const d = data;

  // Status
  document.getElementById('dot').className = 'dot ' + (d.bot_online ? 'dot-on' : 'dot-off');
  document.getElementById('status-txt').textContent = d.bot_online ? 'Online' : 'Offline';

  // ── Balance Hero ──
  const acc = d.account;
  document.getElementById('b-equity').textContent   = acc.equity.toFixed(2);
  document.getElementById('b-avail').textContent    = (acc.available || 0).toFixed(2);
  document.getElementById('b-margin').textContent   = (acc.used_margin || 0).toFixed(2);
  const upnl = acc.unrealised_pnl || acc.total_pnl || 0;
  const upEl = document.getElementById('b-upnl');
  upEl.textContent = (upnl >= 0 ? '+' : '') + upnl.toFixed(2);
  upEl.className   = 'bal-box-val ' + (upnl >= 0 ? 'green' : 'red');
  const pertrade = acc.equity * d.equity_fraction;
  document.getElementById('b-pertrade').textContent = pertrade.toFixed(2);
  document.getElementById('b-lev').textContent      = d.default_leverage;

  // Stats
  document.getElementById('s-wins').textContent   = d.stats.wins;
  document.getElementById('s-losses').textContent = d.stats.losses;
  document.getElementById('s-total').textContent  = d.stats.total;

  // Win Rate
  const wr = d.stats.win_rate;
  document.getElementById('h-wr').textContent     = wr.toFixed(1) + '%';
  document.getElementById('h-wr-bar').style.width = Math.min(wr,100) + '%';
  document.getElementById('h-wr-sub').textContent = d.stats.wins + 'W / ' + d.stats.losses + 'L of ' + d.stats.total;
  const pass = wr >= 70;
  const wb = document.getElementById('h-wr-badge');
  wb.textContent = pass ? '✅ PASSING' : '❌ FAILING';
  wb.className   = 'wr-badge ' + (pass ? 'wr-pass' : 'wr-fail');

  // Config
  document.getElementById('cfg-auto').innerHTML =
    d.auto_execute ? '<span style="color:var(--green)">ON ✅</span>' : '<span style="color:var(--red)">OFF 🔕</span>';
  document.getElementById('cfg-eq').textContent  = (d.equity_fraction * 100).toFixed(0) + '%';
  document.getElementById('cfg-lev').textContent = d.default_leverage + 'x Cross';
  document.getElementById('cfg-ts').textContent  = new Date(d.timestamp).toLocaleTimeString();
  document.getElementById('tog-auto').checked    = d.auto_execute;
  if (!document.getElementById('inp-eq').value)  document.getElementById('inp-eq').value  = (d.equity_fraction*100).toFixed(0);
  if (!document.getElementById('inp-lev').value) document.getElementById('inp-lev').value = d.default_leverage;

  // ── Positions ──
  const pos = acc.positions || [];
  document.getElementById('p-count').textContent = pos.length;
  const tpnl   = acc.total_pnl;
  const tpEl   = document.getElementById('p-tpnl');
  tpEl.textContent = (tpnl >= 0 ? '+' : '') + tpnl.toFixed(2) + ' USDT';
  tpEl.style.color = tpnl >= 0 ? 'var(--green)' : 'var(--red)';
  const pl = document.getElementById('pos-list');
  if (!pos.length) {
    pl.innerHTML = '<div class="empty"><span class="empty-icon">🏖️</span>No open positions</div>';
  } else {
    pl.innerHTML = pos.map(p => {
      const pnl = p.pnl, pct = p.pct || 0;
      const c   = pnl >= 0 ? 'var(--green)' : 'var(--red)';
      const side = p.side === 'Buy' ? 'long' : 'short';
      return `<div class="pos-card">
        <div class="pos-head">
          <div>
            <div class="pos-sym">${p.symbol}</div>
            <div class="pos-tags">
              <span class="tag ${side}">${p.side}</span>
              <span class="tag accent">${p.leverage}x</span>
              <span class="tag">Size: ${p.size}</span>
              <span class="tag">@ ${parseFloat(p.entry).toFixed(4)}</span>
            </div>
          </div>
          <div>
            <div class="pos-pnl" style="color:${c}">${(pnl>=0?'+':'')+pnl.toFixed(2)} USDT</div>
            <div class="pos-pct" style="color:${c}">${(pct>=0?'+':'')+pct.toFixed(2)}%</div>
          </div>
        </div>
        <div class="pos-bar"><div class="pos-bar-fill" style="width:${Math.min(Math.abs(pct)*5,100)}%;background:${c}"></div></div>
      </div>`;
    }).join('');
  }

  // History
  const hl = document.getElementById('hist-list');
  if (!d.history?.length) {
    hl.innerHTML = '<div class="empty"><span class="empty-icon">📭</span>No history yet</div>';
  } else {
    hl.innerHTML = d.history.map(t => `<div class="row">
      <div class="row-left">
        <div class="row-sym">${t.symbol} <span style="font-size:11px;color:var(--text3);font-weight:600">${t.side}</span></div>
        <div class="row-meta">${new Date(t.timestamp).toLocaleString()}</div>
      </div>
      <span class="badge ${t.action==='open'?'b-open':'b-close'}">${t.action.toUpperCase()}</span>
    </div>`).join('');
  }

  // ── Signals ──
  const sigs = d.signals || [];
  document.getElementById('sg-exec').textContent = sigs.filter(s => s.executed).length;
  document.getElementById('sg-skip').textContent = sigs.filter(s => !s.executed && s.reason !== 'AUTO_EXECUTE=off').length;
  document.getElementById('sg-rec').textContent  = sigs.filter(s => s.source === 'recovery').length;
  const sl = document.getElementById('sig-list');
  if (!sigs.length) {
    sl.innerHTML = '<div class="empty"><span class="empty-icon">📡</span>Waiting for signals...</div>';
  } else {
    sl.innerHTML = sigs.map(s => {
      const sig = s.signal || {};
      const sym = sig.symbol || '?', side = sig.side || sig.action || '?';
      const isRec = s.source === 'recovery', exec = s.executed;
      const pause = s.reason === 'AUTO_EXECUTE=off';
      const badge = isRec && exec ? '<span class="badge b-rec">⏪ Recovered</span>'
                  : exec          ? '<span class="badge b-exec">✅ Executed</span>'
                  : pause         ? '<span class="badge b-pause">⏸ Paused</span>'
                  :                 '<span class="badge b-skip">⛔ Skipped</span>';
      const sc = side === 'Buy' ? 'var(--green)' : side === 'Sell' ? 'var(--red)' : 'var(--text3)';
      return `<div class="row">
        <div class="row-left">
          <div><span class="row-sym">${sym}</span> <span style="font-size:11px;font-weight:700;color:${sc}">${side}</span></div>
          <div class="row-meta">${new Date(s.timestamp).toLocaleString()}${isRec?' · recovered':''}</div>
          <div class="row-content">${(s.content||'').slice(0,80)}</div>
        </div>${badge}
      </div>`;
    }).join('');
  }

  // Logs
  allLogs = d.logs || [];
  document.getElementById('log-count').textContent = allLogs.length;
  if (activeTab === 'logs') renderLogs(currentLogFilter);
}

// ── Logs ──────────────────────────────────────────────────────────────────────
let currentLogFilter = 'all';
function filterLogs(f, el) {
  currentLogFilter = f;
  document.querySelectorAll('.pill').forEach(p => p.classList.remove('active'));
  if (el) el.classList.add('active');
  renderLogs(f);
}
function renderLogs(f) {
  let lines = allLogs;
  if (f === 'error')  lines = allLogs.filter(l => l.includes('ERROR') || l.includes('FAIL'));
  if (f === 'trade')  lines = allLogs.filter(l => l.includes('OPENED') || l.includes('CLOSED'));
  if (f === 'signal') lines = allLogs.filter(l => l.includes('SIGNAL') || l.includes('SKIP') || l.includes('RECOVERED'));
  const lb = document.getElementById('log-box');
  if (!lines.length) { lb.innerHTML = '<div class="log-line" style="color:var(--text3)">No matching logs</div>'; return; }
  lb.innerHTML = lines.map(l => {
    let cls = 'log-line';
    if (l.includes('ERROR')||l.includes('FAIL')||l.includes('❌')) cls += ' err';
    else if (l.includes('WARNING')||l.includes('SKIP')) cls += ' warn';
    else if (l.includes('OPENED')||l.includes('connected')||l.includes('✅')||l.includes('HEARTBEAT')) cls += ' good';
    else if (l.includes('INFO')) cls += ' info';
    return `<div class="${cls}">${l.replace(/</g,'&lt;')}</div>`;
  }).join('');
  lb.scrollTop = lb.scrollHeight;
}

// ── Refresh ───────────────────────────────────────────────────────────────────
function refreshNow() { countdown = 15; fetchData(); toast('Refreshed ✅'); }
function tick() {
  countdown--;
  const el = document.getElementById('cd');
  if (el) el.textContent = `Refreshes in ${countdown}s · ${new Date().toLocaleTimeString()}`;
  if (countdown <= 0) { countdown = 15; fetchData(); }
}

// ── Controls ──────────────────────────────────────────────────────────────────
async function setAutoExecute(el) {
  await fetch('/api/settings', {method:'POST',headers:{'Content-Type':'application/json'},
    body: JSON.stringify({auto_execute: el.checked})});
  toast(el.checked ? '✅ Auto Execute ON' : '🔕 Auto Execute OFF', el.checked);
  countdown = 2;
}

async function applySettings() {
  const eq = parseFloat(document.getElementById('inp-eq').value) / 100;
  const lv = parseFloat(document.getElementById('inp-lev').value);
  if (!eq || !lv || eq <= 0 || lv < 1) { toast('Enter valid values', false); return; }
  await fetch('/api/settings', {method:'POST',headers:{'Content-Type':'application/json'},
    body: JSON.stringify({equity_fraction: eq, default_leverage: lv})});
  toast(`✅ ${(eq*100).toFixed(0)}% equity · ${lv}x applied`);
  countdown = 2;
}

async function openTrade() {
  let sym = document.getElementById('inp-sym').value.trim().toUpperCase();
  const side = document.getElementById('inp-side').value;
  if (!sym) { toast('Enter a symbol', false); return; }
  toast('Opening ' + side + ' ' + sym + '...');
  const r = await fetch('/api/trade', {method:'POST',headers:{'Content-Type':'application/json'},
    body: JSON.stringify({symbol: sym, side})});
  const d = await r.json();
  if (d.success) { toast(`✅ ${side} ${d.symbol} @ ${parseFloat(d.entry).toFixed(4)}`); countdown=2; }
  else toast('❌ ' + (d.error||'Failed'), false);
}

async function closeTrade() {
  let sym = document.getElementById('inp-sym').value.trim().toUpperCase();
  if (!sym) { toast('Enter symbol to close', false); return; }
  if (!sym.endsWith('USDT')) sym += 'USDT';
  toast('Closing ' + sym + '...');
  const r = await fetch('/api/close', {method:'POST',headers:{'Content-Type':'application/json'},
    body: JSON.stringify({symbol: sym})});
  const d = await r.json();
  if (d.success) { toast('✅ Closed ' + sym); countdown=2; }
  else toast('❌ ' + (d.error||'Failed'), false);
}

async function closeAll() {
  if (!confirm('Close ALL open positions?')) return;
  toast('Closing all...');
  const r = await fetch('/api/close-all', {method:'POST'});
  const d = await r.json();
  toast(d.success ? '✅ All positions closed' : '❌ Some failed', d.success);
  countdown = 2;
}

// ── Boot ──────────────────────────────────────────────────────────────────────
fetchData();
setInterval(tick, 1000);
</script>
</body>
</html>"""


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/health")
def health():
    return jsonify({"status":"ok","ts":datetime.now().isoformat()})

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
    return jsonify({"success":True,"auto_execute":c.AUTO_EXECUTE,
                    "equity_fraction":c.EQUITY_FRACTION,"default_leverage":c.DEFAULT_LEVERAGE})

@app.route("/api/trade", methods=["POST"])
def api_trade():
    from decimal import Decimal
    d = request.get_json() or {}
    sym  = d.get("symbol","").upper().strip()
    side = d.get("side","Buy")
    if not sym: return jsonify({"success":False,"error":"Symbol required"})
    if not sym.endswith("USDT"): sym += "USDT"
    try:
        cfg  = _cfg(); ex = _executor()
        cost = ex.get_equity() * Decimal(str(cfg.EQUITY_FRACTION))
        lev  = Decimal(str(cfg.DEFAULT_LEVERAGE))
        ok   = ex.open_position(sym, side, cost, lev)
        if ok: return jsonify({"success":True,"symbol":sym,"side":side,
                               "entry":str(ex.get_mark_price(sym)),"cost":str(round(float(cost),2))})
        return jsonify({"success":False,"error":"Order failed"})
    except Exception as e:
        return jsonify({"success":False,"error":str(e)})

@app.route("/api/close", methods=["POST"])
def api_close():
    d = request.get_json() or {}
    sym = d.get("symbol","").upper().strip()
    if not sym: return jsonify({"success":False,"error":"Symbol required"})
    if not sym.endswith("USDT"): sym += "USDT"
    try:
        from signal_listener import SignalExecutor
        return jsonify({"success":SignalExecutor()._close(sym),"symbol":sym})
    except Exception as e:
        return jsonify({"success":False,"error":str(e)})

@app.route("/api/close-all", methods=["POST"])
def api_close_all():
    try:
        from signal_listener import SignalExecutor
        return jsonify({"success":SignalExecutor()._close_all()})
    except Exception as e:
        return jsonify({"success":False,"error":str(e)})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", os.environ.get("DASHBOARD_PORT", 8080)))
    print(f"\n  🚀 Prolific → http://0.0.0.0:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
