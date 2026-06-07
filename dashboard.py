"""
Prolific — VusiD Signals Bot Dashboard  v3
Blue / navy theme · Dark + Light mode
Real-time SSE signal feed · Multi-account management
"""
from __future__ import annotations
import json, logging, os, subprocess, sys, threading, time
from datetime import datetime
from pathlib import Path

import requests
from flask import Flask, Response, jsonify, render_template_string, request
from flask_cors import CORS

import event_bus

app = Flask(__name__)
CORS(app)

from paths import (SIGNALS_FILE, STATS_FILE, HISTORY_FILE,
                   LOG_FILE, DISCORD_LOG, DATA_DIR)
BASE = Path(__file__).parent

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
            log.warning("[BOT] Fast-crash loop — cooling down 60s")
            time.sleep(60); restart_times = []
        log.info("[BOT] Starting bot.py ...")
        try:
            proc = subprocess.Popen([sys.executable, str(BASE / "bot.py")],
                                    cwd=str(BASE), env=os.environ.copy())
            log.info(f"[BOT] PID {proc.pid}"); proc.wait(); code = proc.returncode
        except Exception as e:
            log.error(f"[BOT] Start failed: {e}"); code = -1
        restart_times.append(time.time())
        log.warning(f"[BOT] Exited ({code}). Restarting in 10s ..."); time.sleep(10)


def _keepalive_loop():
    time.sleep(60)
    port = int(os.environ.get("PORT", os.environ.get("DASHBOARD_PORT", 8080)))
    while True:
        try:
            requests.get(f"http://localhost:{port}/health", timeout=5)
        except Exception:
            pass
        time.sleep(240)


threading.Thread(target=_run_bot_forever, daemon=True, name="bot").start()
threading.Thread(target=_keepalive_loop,  daemon=True, name="keepalive").start()
log.info(f"[PROLIFIC] Bot + keepalive threads started")
log.info(f"[PROLIFIC] Data directory: {DATA_DIR}")
log.info(f"[PROLIFIC] Signals file  : {SIGNALS_FILE}")


# ── Data helpers ──────────────────────────────────────────────────────────────

def _cfg():
    import config; return config


def _executor():
    from trade_executor import TradeExecutor; return TradeExecutor()


def _win_rate():
    from signal_listener import get_win_rate; return get_win_rate()


def get_primary_account():
    try:
        ex      = _executor()
        bal     = ex.get_full_balance()
        pos_map = ex.get_my_positions()
        pos_list, total_pnl = [], 0.0
        for _, p in pos_map.items():
            pnl   = float(p["unrealisedPnl"]); total_pnl += pnl
            entry = float(p["avgPrice"])
            mark  = float(ex.get_mark_price(p["symbol"]))
            pct   = ((mark - entry) / entry * 100) if p["side"] == "Buy" \
                    else ((entry - mark) / entry * 100)
            pos_list.append({
                "symbol":   p["symbol"], "side":     p["side"],
                "size":     str(p["size"]), "entry":    str(p["avgPrice"]),
                "mark":     str(round(mark, 6)), "leverage": str(p["leverage"]),
                "pnl":      round(pnl, 4), "pct":      round(pct, 2),
            })
        bal["positions"] = pos_list
        bal["total_pnl"] = round(total_pnl, 4)
        bal["name"]      = "Primary"
        return bal
    except Exception as e:
        return {"equity": 0, "available": 0, "used_margin": 0,
                "unrealised_pnl": 0, "total_equity_usd": 0,
                "positions": [], "total_pnl": 0, "name": "Primary", "error": str(e)}


def get_stats():
    wr, wins, total = _win_rate()
    s = {"win_rate": round(wr * 100, 1), "wins": wins,
         "losses": total - wins, "total": total, "total_pnl": 0}
    if STATS_FILE.exists():
        try:
            d = json.loads(STATS_FILE.read_text())
            s["total_pnl"] = round(float(d.get("total_pnl", 0)), 2)
        except Exception:
            pass
    return s


def get_history(limit=30):
    if not HISTORY_FILE.exists(): return []
    try: return list(reversed(json.loads(HISTORY_FILE.read_text())))[:limit]
    except Exception: return []


def get_signals(limit=200, offset=0):
    if not SIGNALS_FILE.exists(): return []
    try:
        all_sigs = list(reversed(json.loads(SIGNALS_FILE.read_text())))
        return all_sigs[offset:offset + limit]
    except Exception: return []


def get_logs(limit=80):
    for f in [DISCORD_LOG, LOG_FILE]:
        if f.exists():
            try:
                kw = ["SIGNAL", "OPENED", "CLOSED", "FAILED", "connected",
                      "Listening", "HEARTBEAT", "ONLINE", "SKIP", "RECOVERED",
                      "ERROR", "WARNING", "INFO", "started", "Reconnect",
                      "KEEPALIVE", "multi-account", "broadcast", "MSG ["]
                lines = [l for l in f.read_text().splitlines()
                         if any(k in l for k in kw)]
                return list(reversed(lines))[:limit]
            except Exception:
                pass
    return []


def bot_is_online():
    try:
        return bool(subprocess.check_output(["pgrep", "-f", "bot.py"],
                                             text=True).strip())
    except Exception:
        return False


# ── Flask routes ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/health")
def health():
    return jsonify({"status": "ok", "ts": datetime.now().isoformat()})


@app.route("/api/events")
def api_events():
    """SSE endpoint — dashboard receives real-time signal pushes here."""
    q = event_bus.subscribe()
    def generate():
        try:
            while True:
                from queue import Empty
                try:
                    ev = q.get(timeout=25)
                    yield f"data: {json.dumps(ev)}\n\n"
                except Empty:
                    yield 'data: {"type":"ping"}\n\n'
        finally:
            event_bus.unsubscribe(q)
    return Response(generate(), content_type="text/event-stream",
                    headers={"Cache-Control": "no-cache",
                             "X-Accel-Buffering": "no",
                             "Connection": "keep-alive"})


@app.route("/api/status")
def api_status():
    cfg = _cfg()
    return jsonify({
        "bot_online":       bot_is_online(),
        "timestamp":        datetime.now().isoformat(),
        "account":          get_primary_account(),
        "stats":            get_stats(),
        "history":          get_history(),
        "signals":          get_signals(limit=200),
        "logs":             get_logs(),
        "equity_fraction":  cfg.EQUITY_FRACTION,
        "default_leverage": cfg.DEFAULT_LEVERAGE,
        "signal_channel":   cfg.SIGNAL_CHANNEL,
        "auto_execute":     cfg.AUTO_EXECUTE,
    })


@app.route("/api/signals")
def api_signals():
    limit  = int(request.args.get("limit",  200))
    offset = int(request.args.get("offset", 0))
    sigs   = get_signals(limit=limit, offset=offset)
    total  = 0
    if SIGNALS_FILE.exists():
        try: total = len(json.loads(SIGNALS_FILE.read_text()))
        except Exception: pass
    return jsonify({"signals": sigs, "total": total, "offset": offset, "limit": limit})


@app.route("/api/settings", methods=["POST"])
def api_settings():
    import config as c
    d = request.get_json() or {}
    if "auto_execute"     in d: c.AUTO_EXECUTE     = bool(d["auto_execute"])
    if "equity_fraction"  in d:
        v = float(d["equity_fraction"])
        if 0 < v <= 1: c.EQUITY_FRACTION = v
    if "default_leverage" in d:
        v = float(d["default_leverage"])
        if 1 <= v <= 100: c.DEFAULT_LEVERAGE = v
    return jsonify({"success": True, "auto_execute": c.AUTO_EXECUTE,
                    "equity_fraction": c.EQUITY_FRACTION,
                    "default_leverage": c.DEFAULT_LEVERAGE})


# ── Account management ────────────────────────────────────────────────────────

@app.route("/api/accounts", methods=["GET"])
def api_get_accounts():
    from accounts_manager import load_accounts
    accounts = load_accounts()
    safe = [{**a, "api_key": (a["api_key"][:6] + "..." if len(a.get("api_key","")) > 6 else a.get("api_key","")),
             "api_secret": "••••••"} for a in accounts]
    return jsonify(safe)


@app.route("/api/accounts", methods=["POST"])
def api_add_account():
    from accounts_manager import add_account
    d = request.get_json() or {}
    if not d.get("api_key") or not d.get("api_secret"):
        return jsonify({"success": False, "error": "api_key and api_secret required"})
    acc = add_account(d)
    return jsonify({"success": True, "account": {**acc,
        "api_key": acc["api_key"][:6] + "...", "api_secret": "••••••"}})


@app.route("/api/accounts/<acc_id>", methods=["PUT"])
def api_update_account(acc_id):
    from accounts_manager import update_account
    d = request.get_json() or {}
    acc = update_account(acc_id, d)
    if not acc:
        return jsonify({"success": False, "error": "Account not found"})
    return jsonify({"success": True, "account": {**acc,
        "api_key": acc["api_key"][:6] + "...", "api_secret": "••••••"}})


@app.route("/api/accounts/<acc_id>", methods=["DELETE"])
def api_delete_account(acc_id):
    from accounts_manager import remove_account
    return jsonify({"success": remove_account(acc_id)})


@app.route("/api/accounts/<acc_id>/toggle", methods=["POST"])
def api_toggle_account(acc_id):
    from accounts_manager import toggle_account
    acc = toggle_account(acc_id)
    if not acc:
        return jsonify({"success": False, "error": "Not found"})
    return jsonify({"success": True, "enabled": acc["enabled"]})


@app.route("/api/accounts/<acc_id>/balance", methods=["GET"])
def api_account_balance(acc_id):
    from accounts_manager import load_accounts, get_account_balance
    accounts = load_accounts()
    acc = next((a for a in accounts if a["id"] == acc_id), None)
    if not acc:
        return jsonify({"error": "Not found"})
    return jsonify(get_account_balance(acc))


# ── Trade routes ──────────────────────────────────────────────────────────────

@app.route("/api/connectivity")
def api_connectivity():
    """
    Checks Bybit API reachability from this server.
    Use to verify proxy / region config is working.
    """
    import time, requests as req
    results = {}

    # 1. Can we reach Bybit at all?
    try:
        t0 = time.time()
        r  = req.get("https://api.bybit.com/v5/market/time", timeout=8)
        results["bybit_reachable"] = r.status_code == 200
        results["bybit_latency_ms"] = round((time.time() - t0) * 1000)
        results["bybit_status_code"] = r.status_code
    except Exception as e:
        results["bybit_reachable"]    = False
        results["bybit_latency_ms"]   = -1
        results["bybit_status_code"]  = 0
        results["bybit_error"]        = str(e)[:120]

    # 2. Try authenticated balance call
    try:
        ex  = _executor()
        bal = ex.get_full_balance()
        results["auth_ok"]    = bal.get("error") is None
        results["equity"]     = bal.get("equity", 0)
        results["auth_error"] = bal.get("error", "")
    except Exception as e:
        results["auth_ok"]    = False
        results["equity"]     = 0
        results["auth_error"] = str(e)[:120]

    # 3. Proxy / region info
    import config as cfg
    results["proxy_configured"] = bool(cfg.BYBIT_PROXY_URL)
    results["proxy_url"]        = (cfg.BYBIT_PROXY_URL.split("@")[-1]
                                   if "@" in cfg.BYBIT_PROXY_URL
                                   else cfg.BYBIT_PROXY_URL[:40]) if cfg.BYBIT_PROXY_URL else ""

    # 4. Server's visible IP
    try:
        ip_r = req.get("https://api.ipify.org?format=json", timeout=5)
        results["server_ip"] = ip_r.json().get("ip", "?")
    except Exception:
        results["server_ip"] = "unknown"

    results["ok"] = results.get("bybit_reachable") and results.get("auth_ok")
    return jsonify(results)


@app.route("/api/trade", methods=["POST"])
def api_trade():
    from decimal import Decimal
    d    = request.get_json() or {}
    sym  = d.get("symbol", "").upper().strip()
    side = d.get("side", "Buy")
    acc_id = d.get("account_id")
    if not sym: return jsonify({"success": False, "error": "Symbol required"})
    if not sym.endswith("USDT"): sym += "USDT"
    try:
        if acc_id:
            from accounts_manager import load_accounts, get_executor
            accs = load_accounts()
            acc  = next((a for a in accs if a["id"] == acc_id), None)
            if not acc: return jsonify({"success": False, "error": "Account not found"})
            ex      = get_executor(acc)
            eq_frac = Decimal(str(acc.get("equity_fraction", 0.1)))
            lev     = Decimal(str(acc.get("leverage", 5)))
        else:
            cfg = _cfg(); ex = _executor()
            eq_frac = Decimal(str(cfg.EQUITY_FRACTION))
            lev     = Decimal(str(cfg.DEFAULT_LEVERAGE))
        cost = ex.get_equity() * eq_frac
        ok   = ex.open_position(sym, side, cost, lev)
        if ok:
            return jsonify({"success": True, "symbol": sym, "side": side,
                            "entry": str(ex.get_mark_price(sym)),
                            "cost": str(round(float(cost), 2))})
        return jsonify({"success": False, "error": "Order failed"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/close", methods=["POST"])
def api_close():
    d   = request.get_json() or {}
    sym = d.get("symbol", "").upper().strip()
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


# ── HTML ──────────────────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="theme-color" id="theme-meta" content="#060a14">
<title>Prolific</title>
<style>
/* ── TOKENS ──────────────────────────────────────────── */
:root[data-theme="dark"]{
  --bg:#060a14;--surface:#0b1120;--card:#0f172a;--card2:#131e33;--card3:#172039;
  --border:#1e3050;--border2:#253a60;
  --accent:#3b82f6;--accent2:#60a5fa;
  --accentbg:rgba(59,130,246,.1);--accentbrd:rgba(59,130,246,.3);
  --cyan:#06b6d4;--cyanbg:rgba(6,182,212,.1);
  --indigo:#818cf8;--indigobg:rgba(129,140,248,.1);
  --green:#22c55e;--greenbg:rgba(34,197,94,.1);--greenb:rgba(34,197,94,.3);
  --red:#ef4444;--redbg:rgba(239,68,68,.1);--redb:rgba(239,68,68,.3);
  --yellow:#f59e0b;--yellowbg:rgba(245,158,11,.1);
  --text:#e2e8f0;--text2:#8b9ab8;--text3:#3d5275;
  --shadow:rgba(0,5,20,.7);
  --nav-bg:rgba(6,10,20,.96);--top-bg:rgba(6,10,20,.94);
  --input-bg:#080e1c;--modal-bg:rgba(0,5,20,.85);
}
:root[data-theme="light"]{
  --bg:#eef2ff;--surface:#e8edf8;--card:#fff;--card2:#f4f7ff;--card3:#eef2ff;
  --border:#c7d4f0;--border2:#b8c8e8;
  --accent:#2563eb;--accent2:#3b82f6;
  --accentbg:rgba(37,99,235,.07);--accentbrd:rgba(37,99,235,.25);
  --cyan:#0891b2;--cyanbg:rgba(8,145,178,.08);
  --indigo:#6366f1;--indigobg:rgba(99,102,241,.08);
  --green:#16a34a;--greenbg:rgba(22,163,74,.08);--greenb:rgba(22,163,74,.25);
  --red:#dc2626;--redbg:rgba(220,38,38,.08);--redb:rgba(220,38,38,.25);
  --yellow:#d97706;--yellowbg:rgba(217,119,6,.08);
  --text:#0f172a;--text2:#475569;--text3:#94a3b8;
  --shadow:rgba(0,20,80,.1);
  --nav-bg:rgba(238,242,255,.97);--top-bg:rgba(238,242,255,.95);
  --input-bg:#e8edf8;--modal-bg:rgba(14,30,80,.6);
}

/* ── RESET ───────────────────────────────────────────── */
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
html,body{height:100%;overflow:hidden;background:var(--bg);color:var(--text);
  font-family:-apple-system,'Segoe UI',system-ui,sans-serif;
  -webkit-font-smoothing:antialiased;transition:background .3s,color .3s}
button,input,select{font-family:inherit}

/* ── SHELL ───────────────────────────────────────────── */
.app{display:flex;flex-direction:column;height:100%;height:100dvh}
.pages{flex:1;overflow:hidden;position:relative;min-height:0}
.page{position:absolute;inset:0;overflow-y:auto;overflow-x:hidden;
  padding-bottom:calc(64px + env(safe-area-inset-bottom,0px));
  display:none;-webkit-overflow-scrolling:touch}
.page.active{display:block}
.pad{padding:14px}

/* ── TOP BAR ─────────────────────────────────────────── */
.topbar{background:var(--top-bg);backdrop-filter:blur(24px);-webkit-backdrop-filter:blur(24px);
  border-bottom:1px solid var(--border);
  padding:11px 16px;padding-top:calc(11px + env(safe-area-inset-top,0px));
  display:flex;align-items:center;justify-content:space-between;
  position:sticky;top:0;z-index:60;transition:background .3s,border-color .3s}
.brand-name{font-size:22px;font-weight:900;letter-spacing:-.5px;
  background:linear-gradient(135deg,var(--accent),var(--cyan));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}
.brand-sub{font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:2px;color:var(--text3)}
.top-right{display:flex;align-items:center;gap:8px}
.icon-btn{width:34px;height:34px;border-radius:9px;border:1px solid var(--border);
  background:var(--card);cursor:pointer;display:flex;align-items:center;justify-content:center;
  color:var(--text2);transition:all .2s}
.icon-btn:active{transform:scale(.9)}
.icon-btn svg{width:17px;height:17px;stroke-width:1.8}
.status-pill{display:flex;align-items:center;gap:5px;background:var(--card);
  border:1px solid var(--border);border-radius:20px;padding:5px 11px}
.dot{width:7px;height:7px;border-radius:50%;flex-shrink:0;transition:background .3s}
.dot-on{background:var(--green);box-shadow:0 0 8px var(--green);animation:pulse 2s infinite}
.dot-off{background:var(--red)}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.4;transform:scale(.7)}}
.status-txt{font-size:11px;font-weight:700;color:var(--text2)}
.sig-flash{width:8px;height:8px;border-radius:50%;background:var(--cyan);
  display:none;box-shadow:0 0 8px var(--cyan)}
@keyframes sigblink{0%,100%{opacity:1}50%{opacity:0}}

/* ── NAV ─────────────────────────────────────────────── */
.nav{position:fixed;bottom:0;left:0;right:0;z-index:100;
  background:var(--nav-bg);backdrop-filter:blur(24px);-webkit-backdrop-filter:blur(24px);
  border-top:1px solid var(--border);
  padding-bottom:env(safe-area-inset-bottom,0px);
  display:grid;grid-template-columns:repeat(6,1fr)}
.nav-btn{display:flex;flex-direction:column;align-items:center;justify-content:center;
  gap:2px;padding:7px 2px;border:none;background:none;color:var(--text3);
  cursor:pointer;font-size:8px;font-weight:700;letter-spacing:.3px;
  text-transform:uppercase;transition:color .2s;position:relative;min-height:54px}
.nav-btn.active{color:var(--accent)}
.nav-btn svg{width:19px;height:19px;stroke-width:1.8;transition:transform .2s}
.nav-btn.active svg{transform:translateY(-1px)}
.nav-dot{position:absolute;top:5px;right:calc(50% - 16px);width:6px;height:6px;
  border-radius:50%;background:var(--cyan);display:none}
.nav-dot.show{display:block;animation:pulse 1.2s ease 4}
.tab-line{position:absolute;bottom:0;left:50%;transform:translateX(-50%);
  width:20px;height:2px;background:var(--accent);border-radius:3px 3px 0 0;
  opacity:0;transition:opacity .2s}
.nav-btn.active .tab-line{opacity:1}

/* ── CARDS ───────────────────────────────────────────── */
.card{background:var(--card);border:1px solid var(--border);border-radius:16px;
  padding:16px;margin-bottom:12px;position:relative;overflow:hidden;
  transition:background .3s,border-color .3s}
.card-label{font-size:9.5px;font-weight:800;text-transform:uppercase;letter-spacing:1.5px;
  color:var(--text3);margin-bottom:12px;display:flex;align-items:center;gap:6px}
.card-label svg{width:13px;height:13px;stroke-width:2}
.top-stripe{position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,var(--accent),var(--cyan))}

/* ── HERO BALANCE ────────────────────────────────────── */
.hero{background:var(--card);border:1px solid var(--border);border-radius:18px;
  padding:20px;margin-bottom:12px;position:relative;overflow:hidden}
.hero::before{content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,var(--accent),var(--cyan),var(--indigo))}
.hero::after{content:'';position:absolute;top:-50px;right:-50px;width:180px;height:180px;
  border-radius:50%;background:radial-gradient(circle,rgba(59,130,246,.06),transparent 70%);pointer-events:none}
.hero-lbl{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;color:var(--text3)}
.hero-amt{font-size:44px;font-weight:900;letter-spacing:-2px;line-height:1;margin:6px 0 2px;color:var(--text)}
.hero-sub{font-size:11px;color:var(--text3)}
.bal-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:16px}
.bal-box{background:var(--card2);border:1px solid var(--border);border-radius:12px;padding:12px}
.bal-lbl{font-size:9px;font-weight:800;text-transform:uppercase;letter-spacing:1px;color:var(--text3);margin-bottom:4px}
.bal-val{font-size:19px;font-weight:800}
.bal-val.cyan{color:var(--cyan)}.bal-val.blue{color:var(--accent2)}
.bal-val.pos{color:var(--green)}.bal-val.neg{color:var(--red)}

/* ── STATS ───────────────────────────────────────────── */
.stat-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px}
.stat-box{background:var(--card);border:1px solid var(--border);border-radius:12px;
  padding:14px 8px;text-align:center}
.stat-num{font-size:28px;font-weight:900;line-height:1}
.stat-lbl{font-size:8.5px;text-transform:uppercase;letter-spacing:.8px;color:var(--text3);margin-top:4px}

/* ── WIN RATE ────────────────────────────────────────── */
.wr-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}
.wr-big{font-size:40px;font-weight:900;letter-spacing:-1px}
.wr-badge{padding:5px 14px;border-radius:20px;font-size:10px;font-weight:800}
.wr-pass{background:var(--greenbg);color:var(--green);border:1px solid var(--greenb)}
.wr-fail{background:var(--redbg);color:var(--red);border:1px solid var(--redb)}
.bar-track{background:var(--card2);border-radius:8px;height:7px;overflow:hidden;border:1px solid var(--border)}
.bar-fill{height:100%;border-radius:8px;
  background:linear-gradient(90deg,var(--accent),var(--cyan));
  transition:width .8s cubic-bezier(.4,0,.2,1)}

/* ── POSITIONS ───────────────────────────────────────── */
.pos-card{background:var(--card2);border:1px solid var(--border);border-radius:14px;
  padding:14px;margin-bottom:10px;position:relative;overflow:hidden}
.pos-card .side-bar{position:absolute;left:0;top:0;bottom:0;width:3px}
.pos-card.long .side-bar{background:var(--green)}
.pos-card.short .side-bar{background:var(--red)}
.pos-head{display:flex;justify-content:space-between;align-items:flex-start}
.pos-sym{font-size:17px;font-weight:800}
.pos-pnl{font-size:17px;font-weight:800;text-align:right}
.pos-pct{font-size:11px;text-align:right;margin-top:2px;font-weight:700}
.tags{display:flex;gap:5px;flex-wrap:wrap;margin-top:8px}
.tag{background:var(--card);border:1px solid var(--border);border-radius:6px;
  padding:3px 9px;font-size:10.5px;font-weight:600;color:var(--text2)}
.tag.long{color:var(--green);background:var(--greenbg);border-color:var(--greenb)}
.tag.short{color:var(--red);background:var(--redbg);border-color:var(--redb)}
.tag.blue{color:var(--accent2);background:var(--accentbg);border-color:var(--accentbrd)}
.tag.cyan{color:var(--cyan);background:var(--cyanbg);border-color:rgba(6,182,212,.3)}
.pos-bar{background:var(--card);border-radius:4px;height:3px;margin-top:12px;
  border:1px solid var(--border);overflow:hidden}
.pos-bar-fill{height:100%;transition:width .5s;border-radius:4px}

/* ── LIST ROWS ───────────────────────────────────────── */
.row{padding:12px 0;border-bottom:1px solid var(--border);
  display:flex;justify-content:space-between;align-items:flex-start;gap:10px}
.row:last-child{border:none;padding-bottom:0}
.row-left{flex:1;min-width:0}
.row-sym{font-size:15px;font-weight:800}
.row-meta{font-size:10.5px;color:var(--text3);margin-top:3px}
.row-content{font-size:10.5px;color:var(--text2);margin-top:2px;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:220px}
.badge{padding:4px 10px;border-radius:8px;font-size:9.5px;font-weight:800;
  text-transform:uppercase;letter-spacing:.2px;white-space:nowrap;flex-shrink:0}
.b-exec{background:var(--greenbg);color:var(--green);border:1px solid var(--greenb)}
.b-skip{background:var(--redbg);color:var(--red);border:1px solid var(--redb)}
.b-fail{background:var(--redbg);color:var(--red);border:1px solid var(--redb)}
.b-rec{background:var(--cyanbg);color:var(--cyan);border:1px solid rgba(6,182,212,.3)}
.b-pause{background:var(--yellowbg);color:var(--yellow);border:1px solid rgba(245,158,11,.25)}
.b-open{background:var(--accentbg);color:var(--accent2);border:1px solid var(--accentbrd)}
.b-close{background:var(--card2);color:var(--text3);border:1px solid var(--border)}
.b-new{background:linear-gradient(135deg,rgba(59,130,246,.2),rgba(6,182,212,.2));
  color:var(--cyan);border:1px solid rgba(6,182,212,.4);animation:glow 1.5s ease 4}
.b-parse{background:rgba(245,158,11,.12);color:var(--yellow);border:1px solid rgba(245,158,11,.3)}
.b-nofunds{background:rgba(239,68,68,.12);color:var(--red);border:1px solid rgba(239,68,68,.3)}
@keyframes glow{0%,100%{box-shadow:none}50%{box-shadow:0 0 8px rgba(6,182,212,.5)}}
.live-dot{display:inline-block;width:6px;height:6px;border-radius:50%;
  background:var(--green);margin-right:5px;box-shadow:0 0 6px var(--green);animation:pulse 2s infinite}

/* ── ACCOUNTS ────────────────────────────────────────── */
.acc-card{background:var(--card);border:1px solid var(--border);border-radius:16px;
  padding:16px;margin-bottom:12px;position:relative;overflow:hidden}
.acc-card.disabled{opacity:.5}
.acc-head{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:14px}
.acc-name{font-size:16px;font-weight:800}
.acc-meta-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.acc-meta{background:var(--card2);border:1px solid var(--border);border-radius:10px;padding:10px}
.acc-meta-lbl{font-size:8.5px;font-weight:800;text-transform:uppercase;letter-spacing:1px;
  color:var(--text3);margin-bottom:3px}
.acc-meta-val{font-size:18px;font-weight:800}
.acc-meta-val.pos{color:var(--green)}.acc-meta-val.neg{color:var(--red)}
.acc-meta-val.blue{color:var(--accent2)}
.acc-badges{display:flex;gap:6px;margin-top:10px;flex-wrap:wrap}
.pill{padding:4px 10px;border-radius:8px;font-size:10px;font-weight:700}
.pill-blue{background:var(--accentbg);color:var(--accent2);border:1px solid var(--accentbrd)}
.pill-green{background:var(--greenbg);color:var(--green);border:1px solid var(--greenb)}
.pill-red{background:var(--redbg);color:var(--red);border:1px solid var(--redb)}
.pill-gray{background:var(--card2);color:var(--text3);border:1px solid var(--border)}
.pill-cyan{background:var(--cyanbg);color:var(--cyan);border:1px solid rgba(6,182,212,.3)}

/* ── FORMS ───────────────────────────────────────────── */
.section-lbl{font-size:9.5px;font-weight:800;text-transform:uppercase;
  letter-spacing:1.5px;color:var(--text3);margin-bottom:10px;display:flex;align-items:center;gap:6px}
.section-lbl svg{width:12px;height:12px;stroke-width:2.5}
.toggle-row{display:flex;justify-content:space-between;align-items:center;
  padding:14px 16px;background:var(--card);border:1px solid var(--border);
  border-radius:14px;margin-bottom:10px}
.toggle-info strong{font-size:13.5px;font-weight:700;display:block}
.toggle-info span{font-size:11px;color:var(--text3);margin-top:1px;display:block}
.switch{position:relative;width:48px;height:26px;flex-shrink:0}
.switch input{opacity:0;width:0;height:0}
.sw-track{position:absolute;inset:0;background:var(--card2);border-radius:26px;
  cursor:pointer;transition:.3s;border:1px solid var(--border)}
.sw-track::before{content:'';position:absolute;width:20px;height:20px;
  left:2px;top:2px;background:var(--text3);border-radius:50%;transition:.3s}
input:checked+.sw-track{background:var(--accentbg);border-color:var(--accent)}
input:checked+.sw-track::before{transform:translateX(22px);background:var(--accent)}
.inp-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px}
.inp-full{grid-column:1/-1}
.inp-wrap{display:flex;flex-direction:column;gap:5px}
.inp-lbl{font-size:9px;font-weight:800;color:var(--text3);text-transform:uppercase;letter-spacing:.8px}
.inp{width:100%;background:var(--input-bg);border:1px solid var(--border);border-radius:11px;
  padding:12px 14px;color:var(--text);font-size:14px;font-weight:700;
  -webkit-appearance:none;transition:all .2s}
.inp:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 3px var(--accentbg)}
select.inp option{background:var(--card);color:var(--text)}
.btn{width:100%;padding:15px;border:none;border-radius:13px;font-size:14px;
  font-weight:800;cursor:pointer;transition:all .15s;display:flex;
  align-items:center;justify-content:center;gap:8px}
.btn:active{transform:scale(.97);opacity:.88}
.btn svg{width:16px;height:16px;stroke-width:2.2}
.btn-primary{background:linear-gradient(135deg,var(--accent),#1d4ed8);color:#fff;
  box-shadow:0 4px 20px rgba(59,130,246,.3)}
.btn-green{background:var(--greenbg);color:var(--green);border:1px solid var(--greenb)}
.btn-red{background:var(--redbg);color:var(--red);border:1px solid var(--redb)}
.btn-ghost{background:var(--card);color:var(--text2);border:1px solid var(--border)}
.btn-sm{padding:10px 14px;font-size:12.5px;border-radius:10px}
.btn-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.mb{margin-bottom:10px}
.div{height:1px;background:var(--border);margin:16px 0}

/* ── INFO ROWS ───────────────────────────────────────── */
.info-row{display:flex;justify-content:space-between;align-items:center;
  padding:10px 0;border-bottom:1px solid var(--border)}
.info-row:last-child{border:none;padding-bottom:0}
.info-key{font-size:12.5px;color:var(--text2)}
.info-val{font-size:12.5px;font-weight:700}

/* ── LOG ─────────────────────────────────────────────── */
.log-box{background:var(--input-bg);border:1px solid var(--border);border-radius:12px;
  padding:12px;max-height:calc(100dvh - 280px);overflow-y:auto}
.log-line{font-size:9.5px;font-family:'SF Mono','Fira Code',monospace;
  padding:2px 0;line-height:1.6;color:var(--text3);word-break:break-all}
.log-line.err{color:var(--red)}.log-line.warn{color:var(--yellow)}
.log-line.good{color:var(--green)}.log-line.info{color:var(--accent2)}
.log-line.sig{color:var(--cyan);font-weight:700}
.fpills{display:flex;gap:6px;margin-bottom:12px;overflow-x:auto;padding-bottom:2px}
.fpill{padding:6px 13px;border-radius:20px;font-size:10.5px;font-weight:700;
  border:1px solid var(--border);background:var(--card);color:var(--text3);
  cursor:pointer;white-space:nowrap;transition:all .2s}
.fpill.active{background:var(--accentbg);color:var(--accent2);border-color:var(--accentbrd)}

/* ── MODAL ───────────────────────────────────────────── */
.modal-overlay{position:fixed;inset:0;z-index:200;background:var(--modal-bg);
  backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px);
  display:none;align-items:flex-end;justify-content:center}
.modal-overlay.open{display:flex}
.modal{background:var(--surface);border:1px solid var(--border);
  border-radius:24px 24px 0 0;padding:20px;width:100%;max-height:92dvh;
  overflow-y:auto;padding-bottom:calc(20px + env(safe-area-inset-bottom,0px));
  animation:slideup .25s cubic-bezier(.4,0,.2,1)}
@keyframes slideup{from{transform:translateY(100%);opacity:0}to{transform:none;opacity:1}}
.modal-handle{width:40px;height:4px;background:var(--border2);border-radius:4px;margin:0 auto 18px}
.modal-title{font-size:18px;font-weight:800;margin-bottom:18px;
  display:flex;align-items:center;gap:8px}
.modal-title svg{width:20px;height:20px;stroke-width:2;color:var(--accent)}

/* ── TOAST ───────────────────────────────────────────── */
#toast{position:fixed;bottom:calc(64px + 14px + env(safe-area-inset-bottom,0px));
  left:14px;right:14px;background:var(--card);color:var(--text);
  padding:14px 18px;border-radius:14px;font-size:13.5px;font-weight:600;
  text-align:center;z-index:500;display:none;border:1px solid var(--border);
  box-shadow:0 8px 32px var(--shadow);animation:toastIn .25s ease}
@keyframes toastIn{from{transform:translateY(16px);opacity:0}to{transform:none;opacity:1}}

/* ── MISC ────────────────────────────────────────────── */
.countdown{text-align:center;font-size:10px;color:var(--text3);padding:4px 0 2px}
.empty{text-align:center;padding:36px 20px;color:var(--text3)}
.empty svg{display:block;margin:0 auto 10px;color:var(--border2)}
.qa-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px}
.qa{background:var(--card);border:1px solid var(--border);border-radius:14px;
  padding:14px 6px;text-align:center;cursor:pointer;display:flex;
  flex-direction:column;align-items:center;gap:6px;transition:all .15s}
.qa:active{transform:scale(.95);background:var(--card2)}
.qa svg{width:22px;height:22px;stroke-width:1.8;color:var(--accent2)}
.qa-lbl{font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:var(--text3)}
::-webkit-scrollbar{width:3px;height:3px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
</style>
</head>
<body>
<div class="app">

<!-- TOP BAR -->
<div class="topbar">
  <div>
    <div class="brand-name">Prolific</div>
    <div class="brand-sub">Signals Bot</div>
  </div>
  <div class="top-right">
    <span class="sig-flash" id="sig-flash"></span>
    <button class="icon-btn" onclick="toggleTheme()" id="theme-btn">
      <svg id="theme-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path stroke-linecap="round" stroke-linejoin="round" d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>
      </svg>
    </button>
    <div class="status-pill">
      <span class="dot dot-off" id="dot"></span>
      <span class="status-txt" id="status-txt">—</span>
    </div>
  </div>
</div>

<!-- PAGES -->
<div class="pages">

<!-- ① HOME -->
<div class="page active" id="page-home"><div class="pad">
  <div class="hero">
    <div class="hero-lbl">Total Balance</div>
    <div class="hero-amt" id="b-equity">— USDT</div>
    <div class="hero-sub">LIVE · Bybit Unified · <span id="b-ts">—</span></div>
    <div class="bal-grid">
      <div class="bal-box"><div class="bal-lbl">Available</div><div class="bal-val cyan" id="b-avail">—</div><div style="font-size:9px;color:var(--text3);margin-top:2px">USDT</div></div>
      <div class="bal-box"><div class="bal-lbl">Used Margin</div><div class="bal-val blue" id="b-margin">—</div><div style="font-size:9px;color:var(--text3);margin-top:2px">USDT</div></div>
      <div class="bal-box"><div class="bal-lbl">Unrealised PnL</div><div class="bal-val" id="b-upnl">—</div><div style="font-size:9px;color:var(--text3);margin-top:2px">USDT</div></div>
      <div class="bal-box"><div class="bal-lbl">Per Trade</div><div class="bal-val blue" id="b-pertrade">—</div><div style="font-size:9px;color:var(--text3);margin-top:2px">@ <span id="b-lev">—</span>×</div></div>
    </div>
  </div>

  <div class="stat-grid">
    <div class="stat-box"><div class="stat-num" style="color:var(--green)" id="s-wins">—</div><div class="stat-lbl">Wins</div></div>
    <div class="stat-box"><div class="stat-num" style="color:var(--red)" id="s-losses">—</div><div class="stat-lbl">Losses</div></div>
    <div class="stat-box"><div class="stat-num" style="color:var(--accent2)" id="s-total">—</div><div class="stat-lbl">Signals</div></div>
  </div>

  <div class="card">
    <div class="card-label">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M9 19v-6a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v6a2 2 0 0 0 2 2h2a2 2 0 0 0 2-2zm0 0V9a2 2 0 0 1 2-2h2a2 2 0 0 1 2 2v10m-6 0a2 2 0 0 0 2 2h2a2 2 0 0 0 2-2m0 0V5a2 2 0 0 1 2-2h2a2 2 0 0 1 2 2v14a2 2 0 0 0-2 2h-2a2 2 0 0 0-2-2z"/></svg>
      Win Rate · ≥70% Filter
    </div>
    <div class="wr-row"><div class="wr-big" id="h-wr">—</div><span class="wr-badge" id="h-wr-badge">—</span></div>
    <div class="bar-track" style="margin-bottom:8px"><div class="bar-fill" id="h-wr-bar" style="width:0"></div></div>
    <div style="font-size:11px;color:var(--text3)" id="h-wr-sub">—</div>
  </div>

  <div class="qa-grid">
    <div class="qa" onclick="goTab('positions')">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><polyline stroke-linecap="round" stroke-linejoin="round" points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
      <span class="qa-lbl">Positions</span>
    </div>
    <div class="qa" onclick="goTab('signals')">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M8.111 16.404a5.5 5.5 0 0 1 7.778 0M12 20h.01M1.394 9.393c5.857-5.857 15.355-5.857 21.213 0M5.105 12.682a9.5 9.5 0 0 1 13.79 0"/></svg>
      <span class="qa-lbl">Signals</span>
    </div>
    <div class="qa" onclick="goTab('accounts')">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4" stroke-linecap="round" stroke-linejoin="round"/><path stroke-linecap="round" stroke-linejoin="round" d="M23 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75"/></svg>
      <span class="qa-lbl">Accounts</span>
    </div>
  </div>

  <div class="card">
    <div class="card-label">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><circle cx="12" cy="12" r="3" stroke-linecap="round" stroke-linejoin="round"/><path stroke-linecap="round" stroke-linejoin="round" d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>
      Configuration
    </div>
    <div class="info-row"><span class="info-key">Auto Execute</span><span class="info-val" id="cfg-auto">—</span></div>
    <div class="info-row"><span class="info-key">Equity / Trade</span><span class="info-val" id="cfg-eq">—</span></div>
    <div class="info-row"><span class="info-key">Leverage</span><span class="info-val" id="cfg-lev">—</span></div>
    <div class="info-row"><span class="info-key">Channel</span><span class="info-val" style="color:var(--cyan)">#daily-signals</span></div>
    <div class="info-row"><span class="info-key">Mode</span><span class="info-val" style="color:var(--red)">LIVE 🔴</span></div>
    <div class="info-row"><span class="info-key">Updated</span><span class="info-val" id="cfg-ts">—</span></div>
  </div>
  <div class="countdown" id="cd">—</div>
</div></div>

<!-- ② POSITIONS -->
<div class="page" id="page-positions"><div class="pad">
  <div class="card mb">
    <div class="card-label">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><rect x="2" y="3" width="20" height="14" rx="2" stroke-linecap="round" stroke-linejoin="round"/><line x1="8" y1="21" x2="16" y2="21" stroke-linecap="round" stroke-linejoin="round"/><line x1="12" y1="17" x2="12" y2="21" stroke-linecap="round" stroke-linejoin="round"/></svg>
      Portfolio
    </div>
    <div style="display:flex;justify-content:space-between;align-items:center">
      <div><div style="font-size:36px;font-weight:900;color:var(--accent2)" id="p-count">0</div><div style="font-size:11px;color:var(--text3)">open positions</div></div>
      <div style="text-align:right"><div style="font-size:26px;font-weight:900" id="p-tpnl">—</div><div style="font-size:11px;color:var(--text3)">unrealised PnL</div></div>
    </div>
  </div>
  <div id="pos-list"><div class="empty"><svg width="40" height="40" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M5 8h14M5 8a2 2 0 1 0 0-4h14a2 2 0 1 0 0 4M5 8v10a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8m-9 4h4"/></svg>No open positions</div></div>
  <div class="div"></div>
  <div class="card-label" style="margin-bottom:10px">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M12 8v4l3 3m6-3a9 9 0 1 1-18 0 9 9 0 0 1 18 0z"/></svg>
    Trade History
  </div>
  <div id="hist-list"><div class="empty"><svg width="40" height="40" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M9 5H7a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2h-2M9 5a2 2 0 0 0 2 2h2a2 2 0 0 0 2-2M9 5a2 2 0 0 1 2-2h2a2 2 0 0 1 2 2"/></svg>No history yet</div></div>
</div></div>

<!-- ③ SIGNALS -->
<div class="page" id="page-signals"><div class="pad">
  <div class="card mb">
    <div class="card-label"><span class="live-dot"></span>Live Signal Feed · Real-time · <span id="sg-total-lbl" style="color:var(--accent2)">0 total</span></div>
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:12px">
      <div style="text-align:center"><div style="font-size:24px;font-weight:900;color:var(--green)" id="sg-exec">0</div><div style="font-size:8.5px;text-transform:uppercase;letter-spacing:.8px;color:var(--text3);margin-top:3px">Executed</div></div>
      <div style="text-align:center"><div style="font-size:24px;font-weight:900;color:var(--red)" id="sg-skip">0</div><div style="font-size:8.5px;text-transform:uppercase;letter-spacing:.8px;color:var(--text3);margin-top:3px">Skipped</div></div>
      <div style="text-align:center"><div style="font-size:24px;font-weight:900;color:var(--yellow)" id="sg-parse">0</div><div style="font-size:8.5px;text-transform:uppercase;letter-spacing:.8px;color:var(--text3);margin-top:3px">Parse Err</div></div>
      <div style="text-align:center"><div style="font-size:24px;font-weight:900;color:var(--cyan)" id="sg-rec">0</div><div style="font-size:8.5px;text-transform:uppercase;letter-spacing:.8px;color:var(--text3);margin-top:3px">Recovered</div></div>
    </div>
    <!-- Filter pills -->
    <div class="fpills" id="sig-filter-pills">
      <span class="fpill active" onclick="setSigFilter('all',this)">All</span>
      <span class="fpill" onclick="setSigFilter('executed',this)">✅ Executed</span>
      <span class="fpill" onclick="setSigFilter('skipped',this)">⛔ Skipped</span>
      <span class="fpill" onclick="setSigFilter('nofunds',this)">💸 No Funds</span>
      <span class="fpill" onclick="setSigFilter('parse',this)">⚠️ Parse Err</span>
      <span class="fpill" onclick="setSigFilter('recovered',this)">⏪ Recovered</span>
    </div>
  </div>
  <div id="sig-list"><div class="empty"><svg width="40" height="40" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M8.111 16.404a5.5 5.5 0 0 1 7.778 0M12 20h.01M1.394 9.393c5.857-5.857 15.355-5.857 21.213 0M5.105 12.682a9.5 9.5 0 0 1 13.79 0"/></svg>Waiting for signals…</div></div>
  <div id="sig-loadmore" style="display:none;margin-top:10px">
    <button class="btn btn-ghost btn-sm" onclick="loadMoreSignals()">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" style="width:14px;height:14px;stroke-width:2.5"><polyline stroke-linecap="round" stroke-linejoin="round" points="6 9 12 15 18 9"/></svg>
      Load older signals
    </button>
  </div>
</div></div>

<!-- ④ ACCOUNTS -->
<div class="page" id="page-accounts"><div class="pad">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
    <div>
      <div style="font-size:17px;font-weight:800">Trading Accounts</div>
      <div style="font-size:11px;color:var(--text3);margin-top:2px">Each account trades every signal independently</div>
    </div>
    <button class="btn btn-primary btn-sm" onclick="openAddAccount()" style="width:auto;padding:9px 14px;gap:5px">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" style="width:14px;height:14px;stroke-width:2.5"><line x1="12" y1="5" x2="12" y2="19" stroke-linecap="round" stroke-linejoin="round"/><line x1="5" y1="12" x2="19" y2="12" stroke-linecap="round" stroke-linejoin="round"/></svg>
      Add
    </button>
  </div>

  <div class="acc-card">
    <div class="top-stripe"></div>
    <div class="acc-head">
      <div><div class="acc-name">Primary Account</div><div style="font-size:10px;color:var(--text3);margin-top:2px">Main bot (env vars)</div></div>
      <span class="pill pill-green">Active</span>
    </div>
    <div class="acc-meta-grid">
      <div class="acc-meta"><div class="acc-meta-lbl">Balance</div><div class="acc-meta-val blue" id="pa-equity">—</div><div style="font-size:9px;color:var(--text3)">USDT</div></div>
      <div class="acc-meta"><div class="acc-meta-lbl">Unrealised PnL</div><div class="acc-meta-val" id="pa-pnl">—</div><div style="font-size:9px;color:var(--text3)">USDT</div></div>
    </div>
    <div class="acc-badges">
      <span class="pill pill-blue" id="pa-eq-pill">10% equity</span>
      <span class="pill pill-cyan" id="pa-lev-pill">5× leverage</span>
      <span class="pill pill-gray">LIVE</span>
    </div>
  </div>

  <div id="acc-list"></div>

  <div class="div"></div>
  <div style="background:var(--card2);border:1px solid var(--border);border-radius:14px;padding:14px">
    <div style="font-size:11px;font-weight:700;color:var(--accent2);margin-bottom:6px">ℹ️ How multi-account works</div>
    <div style="font-size:11px;color:var(--text3);line-height:1.6">When a signal arrives in Discord, it executes on the primary account first, then on all enabled accounts simultaneously — each using its own equity % and leverage settings.</div>
  </div>
</div></div>

<!-- ⑤ SETTINGS -->
<div class="page" id="page-settings"><div class="pad">
  <div class="section-lbl">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M9.663 17h4.673M12 3v1m6.364 1.636-.707.707M21 12h-1M4 12H3m3.343-5.657-.707-.707m2.828 9.9a5 5 0 1 1 7.072 0l-.548.547A3.374 3.374 0 0 0 14 18.469V19a2 2 0 1 1-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z"/></svg>
    Bot Settings
  </div>
  <div class="toggle-row">
    <div class="toggle-info"><strong>Auto Execute Signals</strong><span>Trade every signal automatically</span></div>
    <label class="switch"><input type="checkbox" id="tog-auto" onchange="setAutoExecute(this)"><span class="sw-track"></span></label>
  </div>
  <div class="inp-grid mb">
    <div class="inp-wrap"><label class="inp-lbl">Equity % / trade</label><input class="inp" type="number" id="inp-eq" min="1" max="100" placeholder="10"></div>
    <div class="inp-wrap"><label class="inp-lbl">Leverage (×)</label><input class="inp" type="number" id="inp-lev" min="1" max="100" placeholder="5"></div>
  </div>
  <button class="btn btn-primary mb" onclick="applySettings()">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"/></svg>
    Apply Settings
  </button>

  <div class="div"></div>
  <div class="section-lbl">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M7 16V4m0 0L3 8m4-4 4 4m6 0v12m0 0 4-4m-4 4-4-4"/></svg>
    Manual Trade
  </div>
  <div class="inp-grid mb">
    <div class="inp-wrap"><label class="inp-lbl">Symbol</label><input class="inp" type="text" id="inp-sym" placeholder="BTC" autocapitalize="characters" autocomplete="off"></div>
    <div class="inp-wrap"><label class="inp-lbl">Direction</label><select class="inp" id="inp-side"><option value="Buy">Long ↑</option><option value="Sell">Short ↓</option></select></div>
  </div>
  <div class="btn-grid mb">
    <button class="btn btn-green btn-sm" onclick="openTrade()">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><polyline stroke-linecap="round" stroke-linejoin="round" points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline stroke-linecap="round" stroke-linejoin="round" points="17 6 23 6 23 12"/></svg>
      Open
    </button>
    <button class="btn btn-ghost btn-sm" onclick="closeTrade()">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><polyline stroke-linecap="round" stroke-linejoin="round" points="23 18 13.5 8.5 8.5 13.5 1 6"/><polyline stroke-linecap="round" stroke-linejoin="round" points="17 18 23 18 23 12"/></svg>
      Close
    </button>
  </div>

  <div class="div"></div>
  <div class="section-lbl">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/></svg>
    Emergency
  </div>
  <button class="btn btn-red mb" onclick="closeAll()">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><circle cx="12" cy="12" r="10" stroke-linecap="round" stroke-linejoin="round"/><line x1="15" y1="9" x2="9" y2="15" stroke-linecap="round" stroke-linejoin="round"/><line x1="9" y1="9" x2="15" y2="15" stroke-linecap="round" stroke-linejoin="round"/></svg>
    Close All Positions
  </button>
  <button class="btn btn-ghost btn-sm" onclick="refreshNow()">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><polyline stroke-linecap="round" stroke-linejoin="round" points="23 4 23 10 17 10"/><polyline stroke-linecap="round" stroke-linejoin="round" points="1 20 1 14 7 14"/><path stroke-linecap="round" stroke-linejoin="round" d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>
    Force Refresh
  </button>
</div></div>

<!-- ⑥ LOGS -->
<div class="page" id="page-logs"><div class="pad">
  <div class="card mb">
    <div class="card-label">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M4 6h16M4 10h16M4 14h16M4 18h16"/></svg>
      Activity Log · <span id="log-count">0</span> entries
    </div>
    <div class="fpills">
      <span class="fpill active" onclick="filterLogs('all',this)">All</span>
      <span class="fpill" onclick="filterLogs('trade',this)">Trades</span>
      <span class="fpill" onclick="filterLogs('signal',this)">Signals</span>
      <span class="fpill" onclick="filterLogs('multi',this)">Multi-Acct</span>
      <span class="fpill" onclick="filterLogs('error',this)">Errors</span>
    </div>
  </div>
  <div class="log-box" id="log-box"><div class="log-line">Loading…</div></div>
</div></div>

</div><!-- /pages -->

<!-- BOTTOM NAV -->
<nav class="nav">
  <button class="nav-btn active" id="nav-home" onclick="goTab('home')">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M3 9.5L12 3l9 6.5V20a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V9.5z"/><path stroke-linecap="round" stroke-linejoin="round" d="M9 21V12h6v9"/></svg>
    <span>Home</span><span class="tab-line"></span>
  </button>
  <button class="nav-btn" id="nav-positions" onclick="goTab('positions')">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><polyline stroke-linecap="round" stroke-linejoin="round" points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
    <span>Trades</span><span class="tab-line"></span>
  </button>
  <button class="nav-btn" id="nav-signals" onclick="goTab('signals')">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M8.111 16.404a5.5 5.5 0 0 1 7.778 0M12 20h.01M1.394 9.393c5.857-5.857 15.355-5.857 21.213 0M5.105 12.682a9.5 9.5 0 0 1 13.79 0"/></svg>
    <span>Signals</span><span class="nav-dot" id="sig-nav-dot"></span><span class="tab-line"></span>
  </button>
  <button class="nav-btn" id="nav-accounts" onclick="goTab('accounts')">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4" stroke-linecap="round" stroke-linejoin="round"/><path stroke-linecap="round" stroke-linejoin="round" d="M23 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75"/></svg>
    <span>Accounts</span><span class="tab-line"></span>
  </button>
  <button class="nav-btn" id="nav-settings" onclick="goTab('settings')">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><circle cx="12" cy="12" r="3" stroke-linecap="round" stroke-linejoin="round"/><path stroke-linecap="round" stroke-linejoin="round" d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>
    <span>Settings</span><span class="tab-line"></span>
  </button>
  <button class="nav-btn" id="nav-logs" onclick="goTab('logs')">
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M4 6h16M4 10h16M4 14h16M4 18h16"/></svg>
    <span>Logs</span><span class="tab-line"></span>
  </button>
</nav>
</div><!-- /app -->

<!-- ADD / EDIT ACCOUNT MODAL -->
<div class="modal-overlay" id="acc-modal" onclick="if(event.target===this)closeModal()">
  <div class="modal">
    <div class="modal-handle"></div>
    <div class="modal-title">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M16 7a4 4 0 1 1-8 0 4 4 0 0 1 8 0zM12 14a7 7 0 0 0-7 7h14a7 7 0 0 0-7-7z"/></svg>
      <span id="modal-title-txt">Add Account</span>
    </div>
    <div class="inp-grid">
      <div class="inp-wrap inp-full"><label class="inp-lbl">Account Name</label><input class="inp" type="text" id="m-name" placeholder="Client A"></div>
      <div class="inp-wrap inp-full"><label class="inp-lbl">Bybit API Key</label><input class="inp" type="text" id="m-key" placeholder="API key..." autocomplete="off"></div>
      <div class="inp-wrap inp-full"><label class="inp-lbl">Bybit API Secret</label><input class="inp" type="password" id="m-secret" placeholder="API secret..." autocomplete="off"></div>
      <div class="inp-wrap"><label class="inp-lbl">Equity % / trade</label><input class="inp" type="number" id="m-eq" value="10" min="1" max="100"></div>
      <div class="inp-wrap"><label class="inp-lbl">Leverage (×)</label><input class="inp" type="number" id="m-lev" value="5" min="1" max="100"></div>
      <div class="inp-wrap inp-full"><label class="inp-lbl">Note (optional)</label><input class="inp" type="text" id="m-note" placeholder="e.g. Client managed account"></div>
    </div>
    <div class="toggle-row" style="margin-bottom:16px">
      <div class="toggle-info"><strong>Testnet / Demo Mode</strong><span>Enable for paper trading</span></div>
      <label class="switch"><input type="checkbox" id="m-testnet"><span class="sw-track"></span></label>
    </div>
    <input type="hidden" id="m-edit-id" value="">
    <div class="btn-grid">
      <button class="btn btn-ghost btn-sm" onclick="closeModal()">Cancel</button>
      <button class="btn btn-primary btn-sm" onclick="saveAccount()">
        <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" style="width:14px;height:14px;stroke-width:2.5"><path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"/></svg>
        Save
      </button>
    </div>
  </div>
</div>

<div id="toast"></div>

<script>
/* ── Theme ───────────────────────────────────────────── */
const _th = localStorage.getItem('theme') || 'dark';
document.documentElement.setAttribute('data-theme', _th);
setThemeIcon(_th);
function setThemeIcon(t) {
  const ic = document.getElementById('theme-icon');
  const mt = document.getElementById('theme-meta');
  if (t === 'dark') {
    ic.innerHTML = `<path stroke-linecap="round" stroke-linejoin="round" d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>`;
    mt.content = '#060a14';
  } else {
    ic.innerHTML = `<circle cx="12" cy="12" r="5" stroke-linecap="round" stroke-linejoin="round"/>
      <line x1="12" y1="1" x2="12" y2="3" stroke-linecap="round"/>
      <line x1="12" y1="21" x2="12" y2="23" stroke-linecap="round"/>
      <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" stroke-linecap="round"/>
      <line x1="18.36" y1="18.36" x2="19.78" y2="19.78" stroke-linecap="round"/>
      <line x1="1" y1="12" x2="3" y2="12" stroke-linecap="round"/>
      <line x1="21" y1="12" x2="23" y2="12" stroke-linecap="round"/>
      <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" stroke-linecap="round"/>
      <line x1="18.36" y1="5.64" x2="19.78" y2="4.22" stroke-linecap="round"/>`;
    mt.content = '#eef2ff';
  }
}
function toggleTheme() {
  const t = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', t);
  localStorage.setItem('theme', t);
  setThemeIcon(t);
}

/* ── State ───────────────────────────────────────────── */
let DATA = null, countdown = 12, activeTab = 'home', allLogs = [], logFilter = 'all';
let _accounts = [];
let _sigFilter = 'all';
let _sigOffset = 0;
const _SIG_PAGE = 200;

/* ── Tabs ────────────────────────────────────────────── */
function goTab(tab) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('page-' + tab)?.classList.add('active');
  document.getElementById('nav-' + tab)?.classList.add('active');
  activeTab = tab;
  if (tab === 'signals') {
    document.getElementById('sig-nav-dot').classList.remove('show');
  }
  if (tab === 'logs') renderLogs(logFilter);
}

/* ── Toast ───────────────────────────────────────────── */
function toast(msg, ok = true) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.borderColor = ok ? 'rgba(34,197,94,.4)' : 'rgba(239,68,68,.4)';
  t.style.display = 'block';
  clearTimeout(t._tid);
  t._tid = setTimeout(() => t.style.display = 'none', 3200);
}

/* ── SSE ─────────────────────────────────────────────── */
function connectSSE() {
  const es = new EventSource('/api/events');
  es.onmessage = e => {
    const ev = JSON.parse(e.data);
    if (ev.type === 'ping') return;
    if (ev.type === 'message') {
      // Raw Discord msg — flash cyan indicator
      const sf = document.getElementById('sig-flash');
      sf.style.display = 'inline-block';
      sf.style.animation = 'none';
      void sf.offsetWidth;
      sf.style.animation = 'sigblink .4s ease 3';
      setTimeout(() => sf.style.display = 'none', 1400);
      return;
    }
    if (ev.type === 'signal') {
      const entry = ev.entry;
      if (DATA) {
        DATA.signals = DATA.signals || [];
        // avoid dupes (same msg_id already in list)
        const dup = DATA.signals.some(x => x.msg_id && x.msg_id === entry.msg_id);
        if (!dup) {
          DATA.signals.unshift(entry);
          // keep max 500 in memory
          if (DATA.signals.length > 500) DATA.signals.length = 500;
        }
        renderSignals();
      }
      if (activeTab !== 'signals') {
        document.getElementById('sig-nav-dot').classList.add('show');
      }
      const sig = entry.signal || {};
      const label = sig.action === 'open' ? `${sig.side} ${sig.symbol}` : sig.action || 'signal';
      toast(entry.executed ? `✅ Executed: ${label}` : `⚠️ Logged: ${label}`, entry.executed);
    }
    if (ev.type === 'account_trade') {
      toast(`[${ev.account}] ${ev.status === 'opened' ? '✅' : '❌'} ${ev.side} ${ev.symbol}`,
            ev.status === 'opened');
    }
  };
  es.onerror = () => { es.close(); setTimeout(connectSSE, 5000); };
}

/* ── Fetch ───────────────────────────────────────────── */
async function fetchData() {
  try {
    const r = await fetch('/api/status');
    const fresh = await r.json();
    // Merge: keep any SSE-pushed signals newer than what the API returned
    if (DATA && DATA.signals && fresh.signals) {
      const apiIds = new Set(fresh.signals.map(x => x.msg_id).filter(Boolean));
      const liveOnly = DATA.signals.filter(x => x.msg_id && !apiIds.has(x.msg_id));
      fresh.signals = [...liveOnly, ...fresh.signals];
    }
    DATA = fresh;
    _sigOffset = 0; // reset pagination on full refresh
    render();
  } catch(e) { console.error(e); }
}

/* ── Render ──────────────────────────────────────────── */
function render() {
  if (!DATA) return;
  const d = DATA;

  // Status
  const on = d.bot_online;
  document.getElementById('dot').className = 'dot ' + (on ? 'dot-on' : 'dot-off');
  document.getElementById('status-txt').textContent = on ? 'Online' : 'Offline';

  // Balance
  const acc = d.account;
  document.getElementById('b-equity').textContent  = acc.equity.toFixed(2) + ' USDT';
  document.getElementById('b-avail').textContent   = (acc.available || 0).toFixed(2);
  document.getElementById('b-margin').textContent  = (acc.used_margin || 0).toFixed(2);
  const upnl = acc.unrealised_pnl || acc.total_pnl || 0;
  const upEl = document.getElementById('b-upnl');
  upEl.textContent = (upnl >= 0 ? '+' : '') + upnl.toFixed(2);
  upEl.className   = 'bal-val ' + (upnl >= 0 ? 'pos' : 'neg');
  document.getElementById('b-pertrade').textContent = (acc.equity * d.equity_fraction).toFixed(2);
  document.getElementById('b-lev').textContent      = d.default_leverage;
  document.getElementById('b-ts').textContent       = new Date(d.timestamp).toLocaleTimeString();

  // Stats
  document.getElementById('s-wins').textContent   = d.stats.wins;
  document.getElementById('s-losses').textContent = d.stats.losses;
  document.getElementById('s-total').textContent  = d.stats.total;

  // Win rate
  const wr = d.stats.win_rate;
  document.getElementById('h-wr').textContent     = wr.toFixed(1) + '%';
  document.getElementById('h-wr-bar').style.width = Math.min(wr, 100) + '%';
  document.getElementById('h-wr-sub').textContent = `${d.stats.wins}W / ${d.stats.losses}L of ${d.stats.total}`;
  const pass = wr >= 70;
  const wb = document.getElementById('h-wr-badge');
  wb.textContent = pass ? '✅ PASSING' : '❌ FAILING';
  wb.className   = 'wr-badge ' + (pass ? 'wr-pass' : 'wr-fail');

  // Config
  document.getElementById('cfg-auto').innerHTML = d.auto_execute
    ? '<span style="color:var(--green)">Enabled ✅</span>'
    : '<span style="color:var(--red)">Disabled 🔕</span>';
  document.getElementById('cfg-eq').textContent  = (d.equity_fraction * 100).toFixed(0) + '% / trade';
  document.getElementById('cfg-lev').textContent = d.default_leverage + '× cross';
  document.getElementById('cfg-ts').textContent  = new Date(d.timestamp).toLocaleTimeString();
  document.getElementById('tog-auto').checked    = d.auto_execute;
  if (!document.getElementById('inp-eq').value)  document.getElementById('inp-eq').value  = (d.equity_fraction*100).toFixed(0);
  if (!document.getElementById('inp-lev').value) document.getElementById('inp-lev').value = d.default_leverage;

  // Accounts page — primary
  document.getElementById('pa-equity').textContent = acc.equity.toFixed(2);
  const pEl = document.getElementById('pa-pnl');
  pEl.textContent = (upnl >= 0 ? '+' : '') + upnl.toFixed(2);
  pEl.className   = 'acc-meta-val ' + (upnl >= 0 ? 'pos' : 'neg');
  document.getElementById('pa-eq-pill').textContent  = (d.equity_fraction*100).toFixed(0) + '% equity';
  document.getElementById('pa-lev-pill').textContent = d.default_leverage + '× leverage';

  renderPositions(acc);
  renderHistory(d.history || []);
  renderSignals();

  allLogs = d.logs || [];
  document.getElementById('log-count').textContent = allLogs.length;
  if (activeTab === 'logs') renderLogs(logFilter);
}

/* ── Positions ───────────────────────────────────────── */
function renderPositions(acc) {
  const pos = (acc && acc.positions) || [];
  document.getElementById('p-count').textContent = pos.length;
  const tpnl = (acc && acc.total_pnl) || 0;
  const tpEl = document.getElementById('p-tpnl');
  tpEl.textContent = (tpnl >= 0 ? '+' : '') + tpnl.toFixed(2) + ' USDT';
  tpEl.style.color = tpnl >= 0 ? 'var(--green)' : 'var(--red)';
  const pl = document.getElementById('pos-list');
  if (!pos.length) {
    pl.innerHTML = `<div class="empty"><svg width="40" height="40" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M5 8h14M5 8a2 2 0 1 0 0-4h14a2 2 0 1 0 0 4M5 8v10a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8m-9 4h4"/></svg>No open positions</div>`;
    return;
  }
  pl.innerHTML = pos.map(p => {
    const pnl = p.pnl, pct = p.pct || 0;
    const c   = pnl >= 0 ? 'var(--green)' : 'var(--red)';
    const sc  = p.side === 'Buy' ? 'long' : 'short';
    return `<div class="pos-card ${sc}">
      <div class="side-bar"></div>
      <div class="pos-head">
        <div>
          <div class="pos-sym">${p.symbol}</div>
          <div class="tags">
            <span class="tag ${sc}">${p.side === 'Buy' ? 'Long ↑' : 'Short ↓'}</span>
            <span class="tag blue">${p.leverage}×</span>
            <span class="tag">Qty ${p.size}</span>
            <span class="tag cyan">@ ${parseFloat(p.entry).toFixed(4)}</span>
          </div>
        </div>
        <div>
          <div class="pos-pnl" style="color:${c}">${(pnl>=0?'+':'')+pnl.toFixed(2)}</div>
          <div class="pos-pct" style="color:${c}">${(pct>=0?'+':'')+pct.toFixed(2)}%</div>
        </div>
      </div>
      <div class="pos-bar"><div class="pos-bar-fill" style="width:${Math.min(Math.abs(pct)*5,100)}%;background:${c}"></div></div>
    </div>`;
  }).join('');
}

/* ── History ─────────────────────────────────────────── */
function renderHistory(hist) {
  const hl = document.getElementById('hist-list');
  if (!hist.length) { hl.innerHTML = `<div class="empty"><svg width="40" height="40" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M9 5H7a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2h-2M9 5a2 2 0 0 0 2 2h2a2 2 0 0 0 2-2M9 5a2 2 0 0 1 2-2h2a2 2 0 0 1 2 2"/></svg>No history yet</div>`; return; }
  hl.innerHTML = hist.map(t => `<div class="row">
    <div class="row-left">
      <div class="row-sym">${t.symbol} <span style="font-size:10.5px;color:${t.side==='Buy'?'var(--green)':'var(--red)'};font-weight:700">${t.side}</span></div>
      <div class="row-meta">${new Date(t.timestamp).toLocaleString()}</div>
    </div>
    <span class="badge ${t.action==='open'?'b-open':'b-close'}">${t.action.toUpperCase()}</span>
  </div>`).join('');
}

/* ── Signals ─────────────────────────────────────────── */
function _sigCategory(s) {
  const act = (s.signal || {}).action || '';
  if (act === 'parse_failed')                         return 'parse';
  if (s.executed && s.source === 'recovery')          return 'recovered';
  if (s.executed)                                     return 'executed';
  if (!s.executed && s.error && s.error.includes('Insufficient')) return 'nofunds';
  return 'skipped';
}

function setSigFilter(f, el) {
  _sigFilter = f;
  document.querySelectorAll('#sig-filter-pills .fpill').forEach(p => p.classList.remove('active'));
  if (el) el.classList.add('active');
  renderSignals();
}

async function loadMoreSignals() {
  _sigOffset += _SIG_PAGE;
  const r = await fetch(`/api/signals?limit=${_SIG_PAGE}&offset=${_sigOffset}`);
  const d = await r.json();
  if (d.signals && d.signals.length) {
    if (!DATA) DATA = {};
    DATA.signals = DATA.signals || [];
    // append older signals (avoid dupes)
    const existing = new Set(DATA.signals.map(x => x.msg_id).filter(Boolean));
    const fresh = d.signals.filter(x => !x.msg_id || !existing.has(x.msg_id));
    DATA.signals.push(...fresh);
    renderSignals();
    if (_sigOffset + _SIG_PAGE >= d.total) {
      document.getElementById('sig-loadmore').style.display = 'none';
    }
  } else {
    document.getElementById('sig-loadmore').style.display = 'none';
  }
}

function _sigBadge(s, isNew) {
  const act = (s.signal || {}).action || '';
  const cat = _sigCategory(s);
  if (cat === 'parse')     return '<span class="badge b-parse">⚠️ Parse Error</span>';
  if (isNew && s.executed) return '<span class="badge b-new">🔵 Live</span>';
  if (cat === 'recovered') return '<span class="badge b-rec">⏪ Recovered</span>';
  if (cat === 'executed')  return '<span class="badge b-exec">✅ Executed</span>';
  if (s.reason === 'AUTO_EXECUTE=off') return '<span class="badge b-pause">⏸ Paused</span>';
  if (cat === 'nofunds')   return '<span class="badge b-nofunds">💸 No Funds</span>';
  if (s.reason && s.reason.includes('execution_failed')) return '<span class="badge b-fail">❌ Failed</span>';
  return '<span class="badge b-skip">⛔ Skipped</span>';
}

function renderSignals() {
  const all = (DATA && DATA.signals) || [];

  // Stats always from full list
  document.getElementById('sg-exec').textContent  = all.filter(s => s.executed).length;
  document.getElementById('sg-parse').textContent = all.filter(s => _sigCategory(s) === 'parse').length;
  document.getElementById('sg-skip').textContent  = all.filter(s => _sigCategory(s) === 'skipped').length;
  document.getElementById('sg-rec').textContent   = all.filter(s => s.source === 'recovery').length;
  document.getElementById('sg-total-lbl').textContent = all.length + ' total';

  // Filter
  const sigs = _sigFilter === 'all' ? all : all.filter(s => _sigCategory(s) === _sigFilter);

  const sl = document.getElementById('sig-list');
  if (!sigs.length) {
    sl.innerHTML = `<div class="empty"><svg width="40" height="40" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M8.111 16.404a5.5 5.5 0 0 1 7.778 0M12 20h.01M1.394 9.393c5.857-5.857 15.355-5.857 21.213 0M5.105 12.682a9.5 9.5 0 0 1 13.79 0"/></svg>${_sigFilter==='all'?'Waiting for signals…':'No signals match this filter'}</div>`;
    return;
  }

  sl.innerHTML = sigs.map((s, i) => {
    const sig  = s.signal || {};
    const act  = sig.action || '';
    const isParseFail = act === 'parse_failed';
    const sym  = isParseFail ? '⚠️ Unrecognised' : (sig.symbol || '?');
    const side = isParseFail ? '' : (sig.side || (act !== 'close_all' ? act : '') || '?');
    const isRec   = s.source === 'recovery';
    const isNew   = i === 0 && (Date.now() - new Date(s.timestamp).getTime()) < 30000;
    const badge   = _sigBadge(s, isNew);
    const sc = side==='Buy'?'var(--green)':side==='Sell'?'var(--red)':'var(--text3)';

    // TP/SL tags
    let tpsl = '';
    if (!isParseFail) {
      const parts = [];
      if (sig.tp)  parts.push(`<span class="tag" style="color:var(--green);background:var(--greenbg);border-color:var(--greenb)">TP ${sig.tp}</span>`);
      if (sig.sl)  parts.push(`<span class="tag" style="color:var(--red);background:var(--redbg);border-color:var(--redb)">SL ${sig.sl}</span>`);
      if (sig.leverage) parts.push(`<span class="tag blue">${sig.leverage}×</span>`);
      if (parts.length) tpsl = `<div class="tags" style="margin-top:6px">${parts.join('')}</div>`;
    }

    const errLine = (s.error && !s.executed)
      ? `<div style="font-size:10px;color:var(--red);margin-top:3px;word-break:break-all">⚠ ${s.error.slice(0,140)}</div>`
      : '';
    const reasonLine = (!s.executed && s.reason && !s.error)
      ? `<div style="font-size:10px;color:var(--text3);margin-top:2px">${s.reason.slice(0,120)}</div>`
      : '';
    const contentLine = isParseFail
      ? `<div style="font-size:10.5px;color:var(--text2);margin-top:3px;white-space:pre-wrap;word-break:break-all;background:var(--card2);border:1px solid var(--border);border-radius:6px;padding:6px 8px">${(s.content||'').slice(0,400).replace(/</g,'&lt;')}</div>`
      : `<div class="row-content">${(s.content||'').slice(0,100).replace(/</g,'&lt;')}</div>`;

    const srcTag = s.source === 'recovery' ? ' · ⏪ recovered'
                  : s.source === 'live'    ? ' · 🔴 live' : '';

    return `<div class="row" style="flex-direction:column;align-items:stretch;gap:5px">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:8px">
        <div class="row-left">
          <div><span class="row-sym">${sym}</span>${side?` <span style="font-size:11px;font-weight:800;color:${sc}">${side}</span>`:''}</div>
          <div class="row-meta">${new Date(s.timestamp).toLocaleString()}${srcTag}</div>
        </div>${badge}
      </div>
      ${contentLine}${tpsl}${errLine}${reasonLine}
    </div>`;
  }).join('');

  // Show load-more if there could be older signals not yet fetched
  const lmBtn = document.getElementById('sig-loadmore');
  if (all.length >= _SIG_PAGE && _sigFilter === 'all') {
    lmBtn.style.display = 'block';
  }
}

/* ── Logs ────────────────────────────────────────────── */
function filterLogs(f, el) {
  logFilter = f;
  document.querySelectorAll('.fpill').forEach(p => p.classList.remove('active'));
  if (el) el.classList.add('active');
  renderLogs(f);
}
function renderLogs(f) {
  const filters = {
    error:  l => l.includes('ERROR')||l.includes('FAIL')||l.includes('❌'),
    trade:  l => l.includes('OPENED')||l.includes('CLOSED'),
    signal: l => l.includes('SIGNAL')||l.includes('SKIP')||l.includes('RECOVERED')||l.includes('MSG ['),
    multi:  l => l.includes('multi-account')||l.includes('broadcast')||l.includes('[multi'),
  };
  const lines = filters[f] ? allLogs.filter(filters[f]) : allLogs;
  const lb = document.getElementById('log-box');
  if (!lines.length) { lb.innerHTML = '<div class="log-line" style="color:var(--text3)">No entries match</div>'; return; }
  lb.innerHTML = lines.map(l => {
    let cls = 'log-line';
    if (l.includes('ERROR')||l.includes('FAIL')||l.includes('❌')) cls += ' err';
    else if (l.includes('WARN')||l.includes('SKIP')) cls += ' warn';
    else if (l.includes('OPENED')||l.includes('connected')||l.includes('✅')||l.includes('HEARTBEAT')) cls += ' good';
    else if (l.includes('SIGNAL')||l.includes('MSG [')) cls += ' sig';
    else if (l.includes('INFO')) cls += ' info';
    return `<div class="${cls}">${l.replace(/</g,'&lt;')}</div>`;
  }).join('');
  lb.scrollTop = lb.scrollHeight;
}

/* ── Accounts ────────────────────────────────────────── */
async function loadAccounts() {
  const r = await fetch('/api/accounts');
  _accounts = await r.json();
  renderAccList();
}
function renderAccList() {
  const el = document.getElementById('acc-list');
  if (!_accounts.length) {
    el.innerHTML = `<div class="card" style="text-align:center;padding:24px;color:var(--text3)">
      <svg width="36" height="36" fill="none" viewBox="0 0 24 24" stroke="currentColor" style="margin:0 auto 10px;color:var(--border2)"><path stroke-linecap="round" stroke-linejoin="round" d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4" stroke-linecap="round" stroke-linejoin="round"/><path stroke-linecap="round" stroke-linejoin="round" d="M23 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75"/></svg>
      <div style="font-size:13px;font-weight:600">No additional accounts yet</div>
      <div style="font-size:11px;margin-top:4px">Tap "Add" to connect a client account</div>
    </div>`;
    return;
  }
  el.innerHTML = _accounts.map(a => {
    const enabled = a.enabled !== false;
    return `<div class="acc-card ${enabled?'':'disabled'}">
      <div class="top-stripe" style="${enabled?'':'background:var(--border)'}"></div>
      <div class="acc-head">
        <div>
          <div class="acc-name">${a.name}</div>
          <div style="font-size:10px;color:var(--text3);margin-top:2px">${a.api_key||'No key set'}</div>
        </div>
        <div style="display:flex;align-items:center;gap:8px">
          <button class="btn btn-ghost btn-sm" onclick="editAccount('${a.id}')" style="width:auto;padding:6px 10px">
            <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" style="width:13px;height:13px;stroke-width:2"><path stroke-linecap="round" stroke-linejoin="round" d="M11 5H6a2 2 0 0 0-2 2v11a2 2 0 0 0 2 2h11a2 2 0 0 0 2-2v-5m-1.414-9.414a2 2 0 1 1 2.828 2.828L11.828 15H9v-2.828l8.586-8.586z"/></svg>
          </button>
          <label class="switch"><input type="checkbox" ${enabled?'checked':''} onchange="toggleAcc('${a.id}',this)"><span class="sw-track"></span></label>
        </div>
      </div>
      <div class="acc-meta-grid" id="acc-bal-${a.id}">
        <div class="acc-meta"><div class="acc-meta-lbl">Balance</div><div class="acc-meta-val blue">—</div></div>
        <div class="acc-meta"><div class="acc-meta-lbl">Unreal. PnL</div><div class="acc-meta-val">—</div></div>
      </div>
      <div class="acc-badges">
        <span class="pill pill-blue">${(a.equity_fraction*100).toFixed(0)}% equity</span>
        <span class="pill pill-cyan">${a.leverage}× lev</span>
        <span class="pill pill-gray">${a.testnet?'Testnet':'LIVE'}</span>
        ${a.note?`<span class="pill pill-gray">${a.note.slice(0,22)}</span>`:''}
      </div>
      <button class="btn btn-ghost btn-sm" onclick="fetchAccBal('${a.id}')" style="margin-top:10px">
        <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" style="width:12px;height:12px;stroke-width:2.5"><polyline stroke-linecap="round" stroke-linejoin="round" points="23 4 23 10 17 10"/><path stroke-linecap="round" stroke-linejoin="round" d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>
        Load Balance
      </button>
    </div>`;
  }).join('');
}
async function fetchAccBal(id) {
  const el = document.getElementById('acc-bal-' + id);
  if (!el) return;
  el.innerHTML = `<div style="font-size:11px;color:var(--text3);padding:8px 0">Loading…</div>`;
  const r = await fetch('/api/accounts/' + id + '/balance');
  const b = await r.json();
  const pnl = b.unrealised_pnl || 0;
  el.innerHTML = `
    <div class="acc-meta"><div class="acc-meta-lbl">Balance</div><div class="acc-meta-val blue">${(b.equity||0).toFixed(2)}</div><div style="font-size:9px;color:var(--text3)">USDT</div></div>
    <div class="acc-meta"><div class="acc-meta-lbl">Unreal. PnL</div><div class="acc-meta-val ${pnl>=0?'pos':'neg'}">${(pnl>=0?'+':'')+pnl.toFixed(2)}</div><div style="font-size:9px;color:var(--text3)">USDT</div></div>`;
}
function openAddAccount() {
  document.getElementById('modal-title-txt').textContent = 'Add Account';
  ['m-name','m-key','m-secret','m-note'].forEach(i => document.getElementById(i).value = '');
  document.getElementById('m-eq').value = '10';
  document.getElementById('m-lev').value = '5';
  document.getElementById('m-testnet').checked = false;
  document.getElementById('m-edit-id').value = '';
  document.getElementById('acc-modal').classList.add('open');
}
function editAccount(id) {
  const a = _accounts.find(x => x.id === id);
  if (!a) return;
  document.getElementById('modal-title-txt').textContent = 'Edit Account';
  document.getElementById('m-name').value  = a.name;
  document.getElementById('m-key').value   = '';
  document.getElementById('m-secret').value = '';
  document.getElementById('m-eq').value    = (a.equity_fraction*100).toFixed(0);
  document.getElementById('m-lev').value   = a.leverage;
  document.getElementById('m-note').value  = a.note || '';
  document.getElementById('m-testnet').checked = a.testnet || false;
  document.getElementById('m-edit-id').value = id;
  document.getElementById('acc-modal').classList.add('open');
}
function closeModal() { document.getElementById('acc-modal').classList.remove('open'); }
async function saveAccount() {
  const eid = document.getElementById('m-edit-id').value;
  const data = {
    name:            document.getElementById('m-name').value.trim(),
    equity_fraction: parseFloat(document.getElementById('m-eq').value) / 100,
    leverage:        parseFloat(document.getElementById('m-lev').value),
    note:            document.getElementById('m-note').value.trim(),
    testnet:         document.getElementById('m-testnet').checked,
  };
  const key = document.getElementById('m-key').value.trim();
  const sec = document.getElementById('m-secret').value.trim();
  if (key) data.api_key    = key;
  if (sec) data.api_secret = sec;
  if (!data.name) { toast('Enter account name', false); return; }
  if (!eid && (!key || !sec)) { toast('API key & secret required', false); return; }
  const url = eid ? '/api/accounts/' + eid : '/api/accounts';
  const mth = eid ? 'PUT' : 'POST';
  const r = await fetch(url, {method:mth, headers:{'Content-Type':'application/json'}, body:JSON.stringify(data)});
  const res = await r.json();
  if (res.success) { toast(eid ? '✅ Account updated' : '✅ Account added'); closeModal(); loadAccounts(); }
  else toast('❌ ' + (res.error || 'Failed'), false);
}
async function toggleAcc(id, el) {
  const r = await fetch('/api/accounts/' + id + '/toggle', {method:'POST'});
  const d = await r.json();
  toast(d.enabled ? '✅ Account enabled' : '🔕 Account disabled', d.enabled);
  loadAccounts();
}

/* ── Settings ────────────────────────────────────────── */
async function setAutoExecute(el) {
  await fetch('/api/settings', {method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({auto_execute: el.checked})});
  toast(el.checked ? '✅ Auto Execute ON' : '🔕 Auto Execute OFF', el.checked);
  countdown = 2;
}
async function applySettings() {
  const eq = parseFloat(document.getElementById('inp-eq').value) / 100;
  const lv = parseFloat(document.getElementById('inp-lev').value);
  if (!eq || eq <= 0 || eq > 1 || !lv || lv < 1) { toast('Invalid values', false); return; }
  await fetch('/api/settings', {method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({equity_fraction: eq, default_leverage: lv})});
  toast(`✅ ${(eq*100).toFixed(0)}% equity · ${lv}× applied`);
  countdown = 2;
}
async function openTrade() {
  let sym = document.getElementById('inp-sym').value.trim().toUpperCase();
  const side = document.getElementById('inp-side').value;
  if (!sym) { toast('Enter a symbol', false); return; }
  toast(`Opening ${side} ${sym}…`);
  const r = await fetch('/api/trade', {method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({symbol: sym, side})});
  const d = await r.json();
  d.success ? (toast(`✅ ${side} ${d.symbol} @ ${parseFloat(d.entry).toFixed(4)}`), countdown=2)
            : toast('❌ ' + (d.error||'Failed'), false);
}
async function closeTrade() {
  let sym = document.getElementById('inp-sym').value.trim().toUpperCase();
  if (!sym) { toast('Enter symbol', false); return; }
  if (!sym.endsWith('USDT')) sym += 'USDT';
  const r = await fetch('/api/close', {method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({symbol: sym})});
  const d = await r.json();
  toast(d.success ? `✅ Closed ${sym}` : '❌ ' + (d.error||'Failed'), d.success);
  if (d.success) countdown = 2;
}
async function closeAll() {
  if (!confirm('Close ALL open positions now?')) return;
  toast('Closing all…');
  const r = await fetch('/api/close-all', {method:'POST'});
  const d = await r.json();
  toast(d.success ? '✅ All positions closed' : '❌ Some failed', d.success);
  countdown = 2;
}
function refreshNow() { countdown = 12; _sigOffset = 0; fetchData(); toast('Refreshed ✅'); }

/* ── Ticker ──────────────────────────────────────────── */
function tick() {
  countdown--;
  const el = document.getElementById('cd');
  if (el) el.textContent = `Auto-refresh in ${countdown}s · ${new Date().toLocaleTimeString()}`;
  if (countdown <= 0) { countdown = 12; fetchData(); }
}

/* ── Boot ────────────────────────────────────────────── */
fetchData();
loadAccounts();
connectSSE();
setInterval(tick, 1000);
</script>
</body>
</html>"""


if __name__ == "__main__":
    port = int(os.environ.get("PORT", os.environ.get("DASHBOARD_PORT", 8080)))
    print(f"\n  Prolific -> http://0.0.0.0:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
