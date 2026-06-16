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


_momentum_alerts: list[dict] = []  # in-memory store, newest first
_momentum_checked: dict[str, float] = {}  # symbol_side → last-checked timestamp

def _momentum_monitor_loop():
    """
    Background thread: every 5 min, check every open position for momentum reversal.
    Pushes SSE + DM when Claude detects close_now / close_soon conditions.
    SAFETY: never executes closes — notification only.
    """
    import time as _t
    _t.sleep(120)  # give bot time to start
    while True:
        try:
            from signal_analyzer import analyze_momentum
            from trade_executor import TradeExecutor
            import config as cfg
            positions_to_check = []

            # Primary account
            try:
                ex = TradeExecutor()
                for pos in ex.get_my_positions().values():
                    positions_to_check.append({**pos, "account_name": "Primary"})
            except Exception:
                pass

            # Extra accounts
            try:
                from accounts_manager import get_enabled_accounts
                for acc in get_enabled_accounts():
                    try:
                        ex2 = TradeExecutor(api_key=acc["api_key"],
                                            api_secret=acc["api_secret"],
                                            testnet=acc.get("testnet", False))
                        for pos in ex2.get_my_positions().values():
                            positions_to_check.append({**pos, "account_name": acc.get("name", acc["id"])})
                    except Exception:
                        pass
            except Exception:
                pass

            for pos in positions_to_check:
                key = f"{pos['symbol']}_{pos['side']}"
                last = _momentum_checked.get(key, 0)
                if _t.time() - last < 270:  # max once per ~4.5 min
                    continue
                _momentum_checked[key] = _t.time()
                try:
                    result = analyze_momentum(pos)
                    if result.get("alert"):
                        alert = {
                            "type":         "momentum_alert",
                            "ts":           datetime.now().isoformat(),
                            "symbol":       pos["symbol"],
                            "side":         pos["side"],
                            "account":      pos.get("account_name", ""),
                            "action":       result.get("action", "monitor"),
                            "urgency":      result.get("urgency", "medium"),
                            "reason":       result.get("reason", ""),
                            "signals":      result.get("signals", []),
                            "confidence":   result.get("confidence", ""),
                            "ai":           result.get("ai", False),
                            "pnl":          float(pos.get("unrealisedPnl", 0)),
                        }
                        _momentum_alerts.insert(0, alert)
                        if len(_momentum_alerts) > 50:
                            _momentum_alerts.pop()
                        from signal_listener import _push_sse
                        _push_sse(alert)
                        log.warning(
                            f"[MOMENTUM] {alert['symbol']} {alert['side']} — "
                            f"{alert['action']} ({alert['urgency']}) | {alert['reason'][:80]}"
                        )
                except Exception as _me:
                    log.debug(f"[MOMENTUM] check error {pos.get('symbol')}: {_me}")
        except Exception as _outer:
            log.error(f"[MOMENTUM] monitor loop error: {_outer}")
        _t.sleep(300)


threading.Thread(target=_run_bot_forever,      daemon=True, name="bot").start()
threading.Thread(target=_keepalive_loop,       daemon=True, name="keepalive").start()
threading.Thread(target=_momentum_monitor_loop, daemon=True, name="momentum").start()
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


def get_history(limit=50):
    """Return trade history — prefers trade_history.json, falls back to executed signals."""
    hist = []
    # 1. Try dedicated history file
    if HISTORY_FILE.exists():
        try:
            hist = list(reversed(json.loads(HISTORY_FILE.read_text())))
        except Exception:
            pass
    # 2. Augment / fallback with executed signals from signals.json
    if SIGNALS_FILE.exists():
        try:
            all_sigs = json.loads(SIGNALS_FILE.read_text())
            existing_ids = {h.get("msg_id") for h in hist}
            for s in all_sigs:
                if not s.get("executed"):
                    continue
                mid = str(s.get("msg_id", ""))
                if mid and mid in existing_ids:
                    continue
                sig = s.get("signal", {})
                hist.append({
                    "timestamp": s.get("timestamp", ""),
                    "msg_id":    mid,
                    "symbol":    sig.get("symbol", "?"),
                    "side":      sig.get("side", "?"),
                    "action":    sig.get("action", "open"),
                    "leverage":  str(sig.get("leverage") or "—"),
                    "take_profit": str(sig.get("take_profit") or ""),
                    "stop_loss":   str(sig.get("stop_loss") or ""),
                    "source":    s.get("source", "live"),
                    "analysis":  s.get("analysis"),
                })
        except Exception as e:
            log.error(f"get_history signals fallback: {e}")
    hist.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return hist[:limit]


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
        "risk_pct":         cfg.RISK_PCT,
        "auto_sl_pct":      cfg.AUTO_SL_PCT,
        "min_ai_score":     cfg.MIN_AI_SCORE,
        "phase_2_equity":   cfg.PHASE_2_EQUITY,
        "phase_3_equity":   cfg.PHASE_3_EQUITY,
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


@app.route("/api/momentum-alerts", methods=["GET"])
def api_momentum_alerts():
    return jsonify(_momentum_alerts[:20])


@app.route("/api/reanalyze", methods=["POST"])
def api_reanalyze():
    """Backfill AI analysis on any open-signal entries that don't have one yet."""
    from signal_analyzer import analyze_signal
    from signal_listener import get_win_rate
    if not SIGNALS_FILE.exists():
        return jsonify({"updated": 0, "total": 0})
    try:
        all_sigs = json.loads(SIGNALS_FILE.read_text())
    except Exception as e:
        return jsonify({"updated": 0, "error": str(e)})
    try:
        _, wins, total = get_win_rate()
        wr = wins / total if total > 0 else 0.0
    except Exception:
        wr = 0.0
    updated = 0
    for s in all_sigs:
        if s.get("analysis") and s["analysis"].get("enabled"):
            continue
        sig = s.get("signal", {})
        if sig.get("action") != "open":
            continue
        try:
            s["analysis"] = analyze_signal(dict(sig), wr)
            updated += 1
        except Exception as e:
            log.error(f"reanalyze: {e}")
    SIGNALS_FILE.write_text(json.dumps(all_sigs, default=str))
    return jsonify({"updated": updated, "total": len(all_sigs)})


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
    if "risk_pct" in d:
        v = float(d["risk_pct"])
        if 0 < v <= 0.20: c.RISK_PCT = v
    if "auto_sl_pct" in d:
        v = float(d["auto_sl_pct"])
        if 0 < v <= 0.20: c.AUTO_SL_PCT = v
    if "min_ai_score" in d:
        v = int(d["min_ai_score"])
        if 0 <= v <= 100: c.MIN_AI_SCORE = v
    return jsonify({"success": True, "auto_execute": c.AUTO_EXECUTE,
                    "equity_fraction": c.EQUITY_FRACTION,
                    "default_leverage": c.DEFAULT_LEVERAGE,
                    "risk_pct": c.RISK_PCT, "auto_sl_pct": c.AUTO_SL_PCT,
                    "min_ai_score": c.MIN_AI_SCORE})


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


@app.route("/api/accounts/<acc_id>/auto-execute", methods=["POST"])
def api_toggle_auto_execute(acc_id):
    from accounts_manager import load_accounts, update_account
    accounts = load_accounts()
    acc = next((a for a in accounts if a["id"] == acc_id), None)
    if not acc:
        return jsonify({"success": False, "error": "Not found"})
    new_val = not acc.get("auto_execute", True)
    updated = update_account(acc_id, {"auto_execute": new_val})
    return jsonify({"success": True, "auto_execute": new_val})


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
        equity = ex.get_equity()
        cost   = equity * eq_frac
        if cost <= 0:
            return jsonify({"success": False, "error": f"Account equity is {float(equity):.2f} USDT — deposit funds first"})
        if sym not in ex.instruments:
            return jsonify({"success": False, "error": f"{sym} is not a tradeable USDT perpetual on Bybit"})
        ok   = ex.open_position(sym, side, cost, lev)
        if ok:
            return jsonify({"success": True, "symbol": sym, "side": side,
                            "entry": str(ex.get_mark_price(sym)),
                            "cost": str(round(float(cost), 2))})
        err = getattr(ex, "last_open_error", "") or "Order placement failed — check Railway logs"
        return jsonify({"success": False, "error": err})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/close", methods=["POST"])
def api_close():
    d      = request.get_json() or {}
    sym    = d.get("symbol", "").upper().strip()
    acc_id = d.get("account_id")
    if not sym: return jsonify({"success": False, "error": "Symbol required"})
    if not sym.endswith("USDT"): sym += "USDT"
    try:
        if acc_id:
            from accounts_manager import load_accounts, get_executor
            accs = load_accounts()
            acc  = next((a for a in accs if a["id"] == acc_id), None)
            if not acc: return jsonify({"success": False, "error": "Account not found"})
            ex = get_executor(acc)
            positions = ex.get_my_positions()
            closed = False
            for key, pos in positions.items():
                if pos["symbol"] == sym:
                    closed = ex.close_position(sym, pos["side"])
                    break
            return jsonify({"success": closed, "symbol": sym})
        from signal_listener import SignalExecutor
        return jsonify({"success": SignalExecutor()._close(sym), "symbol": sym})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/close-all", methods=["POST"])
def api_close_all():
    d      = request.get_json() or {}
    acc_id = d.get("account_id")
    try:
        if acc_id:
            from accounts_manager import load_accounts, get_executor
            accs = load_accounts()
            acc  = next((a for a in accs if a["id"] == acc_id), None)
            if not acc: return jsonify({"success": False, "error": "Account not found"})
            ex        = get_executor(acc)
            positions = ex.get_my_positions()
            for _, pos in positions.items():
                ex.close_position(pos["symbol"], pos["side"])
            return jsonify({"success": True})
        from signal_listener import SignalExecutor
        return jsonify({"success": SignalExecutor()._close_all()})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


_mkt_cache = {"data": [], "ts": 0}
_fng_cache = {"data": None, "ts": 0}

@app.route("/api/market-ticker")
def api_market_ticker():
    """Top 5 gainers + top 5 losers from USDT perpetuals — cached 30s."""
    import time, requests as _req
    global _mkt_cache
    if time.time() - _mkt_cache["ts"] < 30 and _mkt_cache["data"]:
        return jsonify(_mkt_cache["data"])
    try:
        r = _req.get("https://api.bybit.com/v5/market/tickers?category=linear", timeout=10)
        items = r.json().get("result", {}).get("list", [])
        usdt  = [t for t in items if t["symbol"].endswith("USDT")
                 and float(t.get("volume24h") or 0) > 500000]
        mapped = [{
            "symbol": t["symbol"].replace("USDT", ""),
            "price":  float(t.get("lastPrice") or 0),
            "change": round(float(t.get("price24hPcnt") or 0) * 100, 2),
            "vol":    float(t.get("volume24h") or 0),
        } for t in usdt]
        mapped.sort(key=lambda x: x["change"], reverse=True)
        gainers = mapped[:5]
        losers  = list(reversed(mapped[-5:]))
        result  = {"gainers": gainers, "losers": losers, "ts": time.time()}
        _mkt_cache = {"data": result, "ts": time.time()}
        return jsonify(result)
    except Exception as e:
        log.error(f"market-ticker: {e}")
        return jsonify(_mkt_cache["data"] or {"gainers": [], "losers": []})


@app.route("/api/fear-greed")
def api_fear_greed():
    """Crypto Fear & Greed Index from alternative.me — cached 5 min."""
    global _fng_cache
    if time.time() - _fng_cache["ts"] < 300 and _fng_cache["data"]:
        return jsonify(_fng_cache["data"])
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=8)
        d = r.json()["data"][0]
        result = {
            "value":          int(d["value"]),
            "classification": d["value_classification"],
            "updated":        int(d["timestamp"]),
        }
        _fng_cache = {"data": result, "ts": time.time()}
        return jsonify(result)
    except Exception as e:
        log.error(f"fear-greed: {e}")
        if _fng_cache["data"]:
            return jsonify(_fng_cache["data"])
        return jsonify({"value": None, "classification": "Unknown", "updated": 0})


@app.route("/api/set-sl-tp", methods=["POST"])
def api_set_sl_tp():
    d      = request.get_json() or {}
    sym    = d.get("symbol", "").upper().strip()
    side   = d.get("side", "")
    sl     = d.get("stop_loss")
    tp     = d.get("take_profit")
    acc_id = d.get("account_id")
    if not sym: return jsonify({"success": False, "error": "Symbol required"})
    if not sym.endswith("USDT"): sym += "USDT"
    try:
        if acc_id and acc_id != "primary":
            from accounts_manager import load_accounts, get_executor
            accs = load_accounts()
            acc  = next((a for a in accs if a["id"] == acc_id), None)
            if not acc: return jsonify({"success": False, "error": "Account not found"})
            ex = get_executor(acc)
        else:
            ex = _executor()
        return jsonify(ex.set_trading_stop(sym, side, sl, tp))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/positions")
def api_positions():
    """Return all open positions across primary + every enabled extra account in parallel."""
    import concurrent.futures
    from decimal import Decimal, InvalidOperation
    from accounts_manager import load_accounts, get_executor as _get_acc_ex

    def _pct(pnl, entry, size, leverage):
        try:
            margin = Decimal(str(entry)) * Decimal(str(size)) / Decimal(str(leverage))
            return float(Decimal(str(pnl)) / margin * 100) if margin else 0
        except (InvalidOperation, ZeroDivisionError):
            return 0

    def _map_pos(pos_map, account_id, account_name, is_demo):
        out = []
        for p in pos_map.values():
            out.append({
                "account_id":   account_id,
                "account_name": account_name,
                "is_demo":      is_demo,
                "symbol":       p["symbol"],
                "side":         p["side"],
                "size":         str(p["size"]),
                "leverage":     str(p["leverage"]),
                "entry":        str(p["avgPrice"]),
                "pnl":          float(p["unrealisedPnl"]),
                "pct":          _pct(p["unrealisedPnl"], p["avgPrice"],
                                     p["size"], p["leverage"]),
                "stop_loss":    str(p.get("stopLoss", "") or ""),
                "take_profit":  str(p.get("takeProfit", "") or ""),
            })
        return out

    def fetch_primary():
        try:
            ex = _executor()
            return _map_pos(ex.get_my_positions(), "primary", "Primary",
                            _cfg().USE_TESTNET)
        except Exception as e:
            log.error(f"[positions] primary: {e}")
            return []

    def fetch_extra(acc):
        try:
            ex = _get_acc_ex(acc)
            return _map_pos(ex.get_my_positions(), acc["id"], acc["name"],
                            acc.get("testnet", False))
        except Exception as e:
            log.error(f"[positions] {acc['name']}: {e}")
            return []

    extras = [a for a in load_accounts()
              if a.get("enabled") and a.get("api_key") and a.get("api_secret")]
    all_pos = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futs = [pool.submit(fetch_primary)] + [pool.submit(fetch_extra, a) for a in extras]
        for f in concurrent.futures.as_completed(futs):
            all_pos.extend(f.result())

    account_meta = [{"id": a["id"], "name": a["name"],
                     "is_demo": a.get("testnet", False)} for a in extras]
    return jsonify({"positions": all_pos, "total": len(all_pos),
                    "accounts": account_meta})


# ── HTML ──────────────────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="en" data-theme="light">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="default">
<meta name="theme-color" id="theme-meta" content="#F2F2F7">
<title>Prolific</title>
<style>
/* ── TOKENS — iOS GLASS ──────────────────────────────── */
/* Dark — iOS glass */
:root[data-theme="dark"]{
  --bg:#000000;
  --surface:rgba(28,28,30,0.95);
  --card:rgba(28,28,30,0.72);
  --card2:rgba(44,44,46,0.65);
  --card3:rgba(58,58,60,0.55);
  --border:rgba(255,255,255,0.1);
  --border2:rgba(255,255,255,0.18);
  --accent:#007AFF;
  --accent2:#409CFF;
  --accentbg:rgba(0,122,255,0.12);
  --accentbrd:rgba(0,122,255,0.35);
  --cyan:#5AC8FA;
  --cyanbg:rgba(90,200,250,0.1);
  --indigo:#5E5CE6;
  --indigobg:rgba(94,92,230,0.1);
  --green:#34C759;
  --greenbg:rgba(52,199,89,0.1);
  --greenb:rgba(52,199,89,0.3);
  --red:#FF3B30;
  --redbg:rgba(255,59,48,0.1);
  --redb:rgba(255,59,48,0.3);
  --yellow:#FFD60A;
  --yellowbg:rgba(255,214,10,0.1);
  --text:#FFFFFF;
  --text2:rgba(235,235,245,0.6);
  --text3:rgba(235,235,245,0.3);
  --shadow:rgba(0,0,0,0.6);
  --nav-bg:rgba(28,28,30,0.88);
  --top-bg:rgba(28,28,30,0.88);
  --input-bg:rgba(118,118,128,0.12);
  --modal-bg:rgba(0,0,0,0.72);
  --card-shadow:0 8px 32px rgba(0,0,0,0.4),inset 0 1px 0 rgba(255,255,255,0.06);
  --glass-blur:blur(24px) saturate(180%);
}
/* Light — iOS glass */
:root[data-theme="light"]{
  --bg:#F2F2F7;
  --surface:rgba(255,255,255,0.95);
  --card:rgba(255,255,255,0.75);
  --card2:rgba(242,242,247,0.72);
  --card3:rgba(229,229,234,0.65);
  --border:rgba(60,60,67,0.08);
  --border2:rgba(60,60,67,0.15);
  --accent:#007AFF;
  --accent2:#0055D4;
  --accentbg:rgba(0,122,255,0.08);
  --accentbrd:rgba(0,122,255,0.25);
  --cyan:#32ADE6;
  --cyanbg:rgba(50,173,230,0.08);
  --indigo:#5E5CE6;
  --indigobg:rgba(94,92,230,0.08);
  --green:#34C759;
  --greenbg:rgba(52,199,89,0.08);
  --greenb:rgba(52,199,89,0.25);
  --red:#FF3B30;
  --redbg:rgba(255,59,48,0.08);
  --redb:rgba(255,59,48,0.25);
  --yellow:#FF9500;
  --yellowbg:rgba(255,149,0,0.1);
  --text:#000000;
  --text2:rgba(60,60,67,0.6);
  --text3:rgba(60,60,67,0.3);
  --shadow:rgba(0,0,0,0.06);
  --nav-bg:rgba(249,249,249,0.94);
  --top-bg:rgba(249,249,249,0.94);
  --input-bg:rgba(118,118,128,0.12);
  --modal-bg:rgba(0,0,0,0.45);
  --card-shadow:0 2px 16px rgba(0,0,0,0.06),inset 0 1px 0 rgba(255,255,255,0.8);
  --glass-blur:blur(24px) saturate(180%);
}

/* ── RESET ───────────────────────────────────────────── */
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
html,body{height:100%;overflow:hidden;background:var(--bg);color:var(--text);
  font-family:-apple-system,'Segoe UI',system-ui,sans-serif;
  -webkit-font-smoothing:antialiased;transition:background .3s,color .3s}
body::before{content:'';position:fixed;inset:0;z-index:-1;pointer-events:none;
  background:
    radial-gradient(ellipse 70% 50% at 15% 10%, rgba(0,122,255,0.18) 0%, transparent 60%),
    radial-gradient(ellipse 60% 55% at 85% 85%, rgba(94,92,230,0.14) 0%, transparent 60%),
    radial-gradient(ellipse 50% 40% at 50% 50%, rgba(0,122,255,0.06) 0%, transparent 70%)}
[data-theme="light"] body::before{
  background:
    radial-gradient(ellipse 70% 50% at 10% 5%, rgba(0,122,255,0.1) 0%, transparent 60%),
    radial-gradient(ellipse 60% 55% at 90% 90%, rgba(94,92,230,0.08) 0%, transparent 60%)}
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
.topbar{background:var(--top-bg);backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur);
  border-bottom:1px solid var(--border);
  padding:11px 16px;padding-top:calc(11px + env(safe-area-inset-top,0px));
  display:flex;align-items:center;justify-content:space-between;
  position:sticky;top:0;z-index:60;transition:background .3s,border-color .3s;
  overflow:hidden;box-shadow:0 1px 0 var(--border),0 2px 12px var(--shadow)}
.brand-name{font-size:22px;font-weight:900;letter-spacing:-.5px;
  background:linear-gradient(180deg,#FFE566 0%,#FFAB00 45%,#E06800 100%);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;
  filter:drop-shadow(0 1px 0 rgba(100,42,0,.95)) drop-shadow(0 3px 8px rgba(0,0,0,.3))}
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
  background:var(--nav-bg);backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur);
  border-top:1px solid var(--border);
  padding-bottom:env(safe-area-inset-bottom,0px);
  display:grid;grid-template-columns:repeat(6,1fr);
  box-shadow:0 -2px 16px rgba(0,122,255,.06)}
.nav-btn{display:flex;flex-direction:column;align-items:center;justify-content:center;
  gap:2px;padding:7px 2px;border:none;background:none;color:var(--text3);
  cursor:pointer;font-size:8px;font-weight:700;letter-spacing:.3px;
  text-transform:uppercase;transition:color .2s;position:relative;min-height:54px}
.nav-btn.active{color:#007AFF}
.nav-btn svg{width:19px;height:19px;stroke-width:1.8;transition:transform .2s}
.nav-btn.active svg{transform:translateY(-1px)}
.nav-dot{position:absolute;top:5px;right:calc(50% - 16px);width:6px;height:6px;
  border-radius:50%;background:var(--cyan);display:none}
.nav-dot.show{display:block;animation:pulse 1.2s ease 4}
.tab-line{position:absolute;bottom:0;left:50%;transform:translateX(-50%);
  width:20px;height:3px;background:#007AFF;border-radius:3px 3px 0 0;
  opacity:0;transition:opacity .2s}
.nav-btn.active .tab-line{opacity:1}

/* ── CARDS ───────────────────────────────────────────── */
.card{background:var(--card);border:1px solid var(--border);border-radius:16px;
  padding:16px;margin-bottom:12px;position:relative;overflow:hidden;
  transition:background .3s,border-color .3s;box-shadow:var(--card-shadow);
  backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur)}
.card-label{font-size:9.5px;font-weight:800;text-transform:uppercase;letter-spacing:1.5px;
  color:var(--text3);margin-bottom:12px;display:flex;align-items:center;gap:6px}
.card-label svg{width:13px;height:13px;stroke-width:2}
.top-stripe{position:absolute;top:0;left:0;right:0;height:3px;
  background:linear-gradient(90deg,#007AFF,#5AC8FA)}

/* ── HERO BALANCE ────────────────────────────────────── */
.hero{background:linear-gradient(135deg,rgba(0,122,255,0.9),rgba(0,85,212,0.85));
  border-radius:18px;padding:20px;margin-bottom:12px;position:relative;overflow:hidden;
  box-shadow:0 8px 32px rgba(0,122,255,.35);
  backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur)}
.hero::before{content:'';position:absolute;top:-60px;right:-40px;width:200px;height:200px;
  border-radius:50%;background:rgba(255,255,255,.08);pointer-events:none}
.hero::after{content:'';position:absolute;bottom:-40px;left:-30px;width:140px;height:140px;
  border-radius:50%;background:rgba(255,255,255,.05);pointer-events:none}
.hero-lbl{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;color:rgba(255,255,255,.7)}
.hero-amt{font-size:44px;font-weight:900;letter-spacing:-2px;line-height:1;margin:6px 0 2px;color:#FFFFFF}
.hero-sub{font-size:11px;color:rgba(255,255,255,.65)}
.bal-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:16px}
.bal-box{background:rgba(255,255,255,.15);border:1px solid rgba(255,255,255,.2);
  border-radius:12px;padding:12px;backdrop-filter:blur(8px)}
.bal-lbl{font-size:9px;font-weight:800;text-transform:uppercase;letter-spacing:1px;color:rgba(255,255,255,.75);margin-bottom:4px}
.bal-val{font-size:20px;font-weight:900;color:#FFFFFF}
.bal-val.pos{color:#BBF7D0}.bal-val.neg{color:#FCA5A5}
.bal-sub{font-size:9px;color:rgba(255,255,255,.6);margin-top:2px;font-weight:600}

/* ── STATS ───────────────────────────────────────────── */
.stat-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px}
.stat-box{background:var(--card);border:1px solid var(--border);border-radius:12px;
  padding:14px 8px;text-align:center;box-shadow:var(--card-shadow);
  backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur)}
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
  background:linear-gradient(90deg,#007AFF,#5AC8FA);
  transition:width .8s cubic-bezier(.4,0,.2,1)}

/* ── POSITIONS ───────────────────────────────────────── */
.pos-card{background:var(--card);border:1px solid var(--border);border-radius:14px;
  padding:14px;margin-bottom:10px;position:relative;overflow:hidden;
  box-shadow:var(--card-shadow);
  backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur)}
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
.pos-action-row{display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-top:8px}
.pos-sl-btn{background:var(--redbg);color:var(--red);border:1px solid var(--redb);
  border-radius:8px;padding:7px 10px;font-size:10.5px;font-weight:700;cursor:pointer;
  display:flex;align-items:center;justify-content:center;gap:4px;transition:all .15s;line-height:1.2;
  flex-direction:column}
.pos-tp-btn{background:var(--greenbg);color:var(--green);border:1px solid var(--greenb);
  border-radius:8px;padding:7px 10px;font-size:10.5px;font-weight:700;cursor:pointer;
  display:flex;align-items:center;justify-content:center;gap:4px;transition:all .15s;line-height:1.2;
  flex-direction:column}
.pos-sl-btn:hover{background:var(--red);color:#fff;border-color:var(--red)}
.pos-tp-btn:hover{background:var(--green);color:#fff;border-color:var(--green)}
.pos-sl-btn .sltp-val,.pos-tp-btn .sltp-val{font-size:9px;font-weight:600;opacity:.8;margin-top:2px}
.pos-close-btn{width:100%;margin-top:6px;background:var(--redbg);
  color:var(--red);border:1px solid var(--redb);border-radius:8px;
  padding:8px 14px;font-size:11px;font-weight:700;cursor:pointer;
  display:flex;align-items:center;justify-content:center;gap:5px;
  transition:all .15s;line-height:1}
.pos-close-btn:hover{background:var(--red);color:#fff;border-color:var(--red)}
.pos-close-btn:active{transform:scale(.98)}
.pos-close-btn:disabled{opacity:.55;pointer-events:none}
.pos-close-btn svg{width:11px;height:11px;stroke-width:3;flex-shrink:0}
.pos-demo-badge,.pos-live-badge{display:inline-block;border-radius:5px;
  padding:1px 6px;font-size:9px;font-weight:800;letter-spacing:.4px;vertical-align:middle}
.pos-demo-badge{background:rgba(139,92,246,.15);color:#a78bfa;border:1px solid rgba(139,92,246,.3)}
.pos-live-badge{background:var(--greenbg);color:var(--green);border:1px solid var(--greenb)}
.acct-tabs{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:12px}
.acct-tab{padding:6px 13px;border-radius:9px;font-size:10.5px;font-weight:700;cursor:pointer;
  border:1px solid var(--border);background:var(--card2);color:var(--text3);
  transition:all .15s;line-height:1.2}
.acct-tab.active{background:var(--accentbg);color:var(--accent2);border-color:var(--accentbrd)}
.acct-tab:active{transform:scale(.95)}
.cs-overlay{position:fixed;inset:0;z-index:300;background:var(--modal-bg);
  backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);
  display:none;align-items:flex-end;justify-content:center}
.cs-overlay.open{display:flex}
.cs-sheet{background:var(--surface);border:1px solid var(--border);
  border-radius:24px 24px 0 0;padding:20px;width:100%;max-height:90dvh;overflow-y:auto;
  padding-bottom:calc(20px + env(safe-area-inset-bottom,0px));
  animation:slideup .25s cubic-bezier(.4,0,.2,1);
  box-shadow:0 -8px 40px rgba(0,122,255,.12);
  backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur)}
.cs-handle{width:40px;height:4px;background:var(--border2);border-radius:4px;margin:0 auto 18px}
.cs-stats{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin:14px 0}
.cs-stat{background:var(--card2);border-radius:10px;padding:10px;border:1px solid var(--border)}
.cs-stat-lbl{font-size:8.5px;font-weight:800;text-transform:uppercase;letter-spacing:.8px;
  color:var(--text3);margin-bottom:4px}
.cs-stat-val{font-size:16px;font-weight:800}
.cs-warn{background:rgba(239,68,68,.08);border:1px solid rgba(239,68,68,.2);
  border-radius:10px;padding:10px 12px;font-size:11px;color:var(--red);margin-bottom:14px}
.cs-demo-note{background:rgba(139,92,246,.08);border:1px solid rgba(139,92,246,.25);
  border-radius:10px;padding:10px 12px;font-size:11px;color:#a78bfa;margin-bottom:10px}

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
  padding:16px;margin-bottom:12px;position:relative;overflow:hidden;
  box-shadow:var(--card-shadow);
  backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur)}
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
  border-radius:14px;margin-bottom:10px;
  backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur)}
.toggle-info strong{font-size:13.5px;font-weight:700;display:block}
.toggle-info span{font-size:11px;color:var(--text3);margin-top:1px;display:block}
.switch{position:relative;width:48px;height:26px;flex-shrink:0}
.switch input{opacity:0;width:0;height:0}
.sw-track{position:absolute;inset:0;background:var(--card2);border-radius:26px;
  cursor:pointer;transition:.3s;border:1px solid var(--border)}
.sw-track::before{content:'';position:absolute;width:20px;height:20px;
  left:2px;top:2px;background:var(--text3);border-radius:50%;transition:.3s}
input:checked+.sw-track{background:#007AFF;border-color:#007AFF}
input:checked+.sw-track::before{transform:translateX(22px);background:#FFFFFF}
.inp-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px}
.inp-full{grid-column:1/-1}
.inp-wrap{display:flex;flex-direction:column;gap:5px}
.inp-lbl{font-size:9px;font-weight:800;color:var(--text3);text-transform:uppercase;letter-spacing:.8px}
.inp{width:100%;background:var(--input-bg);border:1px solid var(--border);border-radius:11px;
  padding:12px 14px;color:var(--text);font-size:14px;font-weight:700;
  -webkit-appearance:none;transition:all .2s}
.inp:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 4px rgba(0,122,255,0.15)}
select.inp option{background:var(--card);color:var(--text)}
.btn{width:100%;padding:15px;border:none;border-radius:13px;font-size:14px;
  font-weight:800;cursor:pointer;transition:all .15s;display:flex;
  align-items:center;justify-content:center;gap:8px}
.btn:active{transform:scale(.97);opacity:.88}
.btn svg{width:16px;height:16px;stroke-width:2.2}
.btn-primary{background:rgba(0,122,255,0.95);color:#fff;box-shadow:0 4px 20px rgba(0,122,255,0.4)}
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

/* ── ACCOUNT MINI CARDS (home screen) ───────────────── */
.acct-mini{display:flex;align-items:center;gap:12px;background:var(--card);
  border:1px solid var(--border);border-radius:16px;padding:14px 16px;
  margin-bottom:8px;cursor:pointer;transition:all .15s;position:relative;overflow:hidden;
  box-shadow:var(--card-shadow);
  backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur)}
.acct-mini::before{content:'';position:absolute;left:0;top:0;bottom:0;width:3px;
  background:var(--accent);border-radius:3px 0 0 3px}
.acct-mini.demo::before{background:var(--cyan)}
.acct-mini:active{transform:scale(.98);background:var(--card2)}
.acct-mini-icon{width:36px;height:36px;border-radius:10px;background:var(--accentbg);
  display:flex;align-items:center;justify-content:center;flex-shrink:0}
.acct-mini.demo .acct-mini-icon{background:var(--cyanbg)}
.acct-mini-icon svg{width:18px;height:18px;stroke-width:2;color:var(--accent)}
.acct-mini.demo .acct-mini-icon svg{color:var(--cyan)}
.acct-mini-body{flex:1;min-width:0}
.acct-mini-name{font-size:13px;font-weight:800;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.acct-mini-sub{font-size:10px;color:var(--text3);margin-top:1px}
.acct-mini-right{text-align:right;flex-shrink:0}
.acct-mini-eq{font-size:16px;font-weight:900;letter-spacing:-.3px}
.acct-mini-pnl{font-size:10.5px;font-weight:700;margin-top:1px}
.acct-mini-arrow{font-size:18px;color:var(--text3);margin-left:6px;flex-shrink:0}
.acct-mini-badge{display:inline-block;padding:2px 7px;border-radius:5px;font-size:8.5px;
  font-weight:800;letter-spacing:.5px;margin-left:5px;vertical-align:middle}
.badge-live{background:var(--greenbg);color:var(--green);border:1px solid var(--greenb)}
.badge-demo{background:var(--cyanbg);color:var(--cyan);border:1px solid rgba(34,211,238,.3)}

/* ── ARC GAUGE ───────────────────────────────────────── */
.gauge-wrap{display:flex;flex-direction:column;align-items:center;margin:4px 0 8px}
.gauge-svg{width:140px;height:76px;overflow:visible}
.gauge-track{fill:none;stroke:var(--card2);stroke-width:10;stroke-linecap:round}
.gauge-fill{fill:none;stroke-width:10;stroke-linecap:round;
  transition:stroke-dashoffset .8s cubic-bezier(.4,0,.2,1)}
.gauge-center{font-size:28px;font-weight:900;fill:var(--text);text-anchor:middle}
.gauge-sub{font-size:9px;fill:var(--text3);text-anchor:middle;
  font-weight:700;text-transform:uppercase;letter-spacing:1px}

/* ── ACCOUNT DETAIL SHEET ────────────────────────────── */
.ad-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin:14px 0}
.ad-stat{background:var(--card2);border-radius:12px;padding:12px;border:1px solid var(--border);
  backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur)}
.ad-stat-lbl{font-size:8.5px;font-weight:800;text-transform:uppercase;letter-spacing:.8px;
  color:var(--text3);margin-bottom:4px}
.ad-stat-val{font-size:18px;font-weight:900}
.ad-pos-item{background:var(--card2);border-radius:10px;padding:10px 12px;margin-bottom:8px;border:1px solid var(--border)}
.ad-pos-item:last-child{margin-bottom:0}
.ad-pos-sym{font-size:14px;font-weight:800}
.ad-pos-pnl{font-size:13px;font-weight:800;text-align:right}
.ad-stat-sub{font-size:9px;color:var(--text3)}
/* account panel tabs */
.ad-tab{flex:1;background:none;border:none;cursor:pointer;padding:7px 4px;border-radius:8px;
  font-size:12px;font-weight:700;color:var(--text3);transition:all .15s}
.ad-tab.active{background:var(--accent);color:#fff;box-shadow:0 2px 8px rgba(0,122,255,0.3)}
/* full position cards in account panel */
.adp-card{background:var(--card2);border:1px solid var(--border);border-radius:12px;
  padding:12px;margin-bottom:10px;
  backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur)}
.adp-card:last-child{margin-bottom:0}
.adp-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:8px}
.adp-sym{font-size:14px;font-weight:900}
.adp-pnl{font-size:14px;font-weight:800}
.adp-row{display:flex;gap:8px;font-size:10px;color:var(--text3);margin-bottom:8px;flex-wrap:wrap}
.adp-row span{font-weight:700;color:var(--text)}
.adp-sltp{display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:8px}
.adp-sltp input{background:var(--bg);border:1px solid var(--border);border-radius:7px;
  padding:6px 8px;font-size:11px;color:var(--text);font-family:inherit;width:100%;box-sizing:border-box}
.adp-sltp label{font-size:9px;font-weight:800;color:var(--text3);letter-spacing:.07em;
  text-transform:uppercase;display:block;margin-bottom:3px}
.adp-actions{display:flex;gap:6px}
/* prop firm bars */
.pf-bar-wrap{height:5px;background:var(--border);border-radius:3px;overflow:hidden;margin-bottom:4px}
.pf-bar{height:100%;border-radius:3px;transition:width .4s,background .3s}
.mb{margin-bottom:12px}

/* ── MARKET TICKER RIBBON ────────────────────────────── */
.market-panel{display:grid;grid-template-columns:1fr 1fr;
  background:var(--card);border-bottom:1px solid var(--border)}
.market-col{padding:4px 0}
.market-col:first-child{border-right:1px solid var(--border)}
.market-col-hd{display:flex;align-items:center;gap:5px;
  padding:4px 10px 3px;font-size:9px;font-weight:800;letter-spacing:.1em;text-transform:uppercase}
.market-col-hd.gain{color:var(--green)}
.market-col-hd.lose{color:var(--red)}
.mkt-row{display:flex;align-items:center;justify-content:space-between;
  padding:2px 10px;gap:6px;transition:background .1s}
.mkt-row:hover{background:var(--accentbg)}
.mkt-left{display:flex;align-items:center;gap:5px;min-width:0}
.mkt-sym{font-size:10.5px;font-weight:700;color:var(--text1);letter-spacing:.15px}
.mkt-price{font-size:9px;color:var(--text3)}
.mkt-chg{font-size:11px;font-weight:900;flex-shrink:0}
.mkt-chg.up{color:var(--green)}
.mkt-chg.dn{color:var(--red)}
.mkt-pulse{display:inline-block;width:5px;height:5px;border-radius:50%;
  flex-shrink:0;animation:mktPulse 2s ease-in-out infinite}
.mkt-pulse.gain{background:var(--green)}
.mkt-pulse.lose{background:var(--red)}
@keyframes mktPulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.35;transform:scale(.6)}}

/* ── AI ANALYSIS ─────────────────────────────────────── */
.ai-badge{display:inline-flex;align-items:center;gap:4px;border-radius:8px;
  padding:3px 8px;font-size:10px;font-weight:800;letter-spacing:.3px;border:1px solid}
.ai-badge.s-strong-win{background:rgba(74,222,128,.15);color:var(--green);border-color:var(--greenb)}
.ai-badge.s-likely-win{background:rgba(74,222,128,.08);color:var(--green);border-color:rgba(74,222,128,.2)}
.ai-badge.s-neutral{background:var(--yellowbg);color:var(--yellow);border-color:rgba(251,191,36,.3)}
.ai-badge.s-likely-loss{background:rgba(248,113,113,.1);color:var(--red);border-color:var(--redb)}
.ai-badge.s-strong-loss{background:rgba(248,113,113,.18);color:var(--red);border-color:var(--redb)}
.ai-score-ring{position:relative;width:52px;height:52px;flex-shrink:0}
.ai-score-ring svg{transform:rotate(-90deg)}
.ai-score-num{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;
  font-size:13px;font-weight:900}
.ai-panel{background:var(--card);border:1px solid var(--border);border-radius:14px;
  margin-top:10px;overflow:hidden;box-shadow:var(--card-shadow)}
.ai-panel-head{display:flex;align-items:center;gap:10px;padding:12px 14px;cursor:pointer}
.ai-panel-body{padding:0 14px 14px;display:none}
.ai-panel.open .ai-panel-body{display:block}
.ai-verdict-row{display:flex;align-items:center;gap:8px;margin-bottom:10px;flex-wrap:wrap}
.ai-rec{display:inline-flex;align-items:center;gap:5px;border-radius:8px;
  padding:5px 12px;font-size:11px;font-weight:800;border:1px solid}
.ai-rec.take{background:rgba(74,222,128,.12);color:var(--green);border-color:var(--greenb)}
.ai-rec.caution{background:var(--yellowbg);color:var(--yellow);border-color:rgba(251,191,36,.3)}
.ai-rec.skip{background:rgba(248,113,113,.12);color:var(--red);border-color:var(--redb)}
.ai-factor{display:flex;align-items:flex-start;gap:6px;padding:5px 0;
  border-bottom:1px solid var(--border);font-size:11px;color:var(--text2)}
.ai-factor:last-child{border:none}
.ai-factor-dot{width:6px;height:6px;border-radius:50%;background:var(--accent);
  flex-shrink:0;margin-top:4px}
.ai-meta-row{display:flex;gap:6px;flex-wrap:wrap;margin-top:10px}
.ai-meta-chip{background:var(--card2);border:1px solid var(--border);border-radius:6px;
  padding:3px 8px;font-size:10px;font-weight:600;color:var(--text3)}
.ai-summary{font-size:11.5px;color:var(--text2);margin:10px 0 6px;line-height:1.5}
/* ── Signal Analysis Full-Screen Overlay ── */
.sa-overlay{position:fixed;inset:0;z-index:500;background:var(--modal-bg);
  backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);
  display:none;align-items:flex-end;justify-content:center}
.sa-overlay.open{display:flex}
.sa-sheet{background:var(--surface);border:1px solid var(--border);
  border-radius:28px 28px 0 0;width:100%;max-height:92dvh;overflow-y:auto;
  padding:20px;padding-bottom:calc(24px + env(safe-area-inset-bottom,0px));
  animation:slideup .28s cubic-bezier(.4,0,.2,1);
  box-shadow:0 -8px 48px rgba(0,122,255,.15);
  backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur)}
.sa-hero{display:flex;align-items:center;gap:16px;margin-bottom:18px;padding-bottom:16px;
  border-bottom:1px solid var(--border)}
.sa-score-ring{position:relative;width:72px;height:72px;flex-shrink:0}
.sa-score-ring svg{transform:rotate(-90deg)}
.sa-score-num{position:absolute;inset:0;display:flex;align-items:center;
  justify-content:center;font-size:18px;font-weight:900}
.sa-symbol{font-size:24px;font-weight:900;letter-spacing:-.5px}
.sa-section-lbl{font-size:9px;font-weight:800;text-transform:uppercase;letter-spacing:1px;
  color:var(--text3);margin:14px 0 8px}
.sa-factor{display:flex;align-items:flex-start;gap:10px;padding:10px 12px;
  background:var(--card2);border:1px solid var(--border);border-radius:10px;margin-bottom:6px}
.sa-factor:last-child{margin-bottom:0}
.sa-factor-icon{width:22px;height:22px;border-radius:6px;background:var(--accentbg);
  display:flex;align-items:center;justify-content:center;flex-shrink:0;margin-top:1px}
.sa-factor-icon svg{width:12px;height:12px;stroke:var(--accent);stroke-width:2.5}
.sa-factor-txt{font-size:12px;color:var(--text2);line-height:1.5;flex:1}
.sa-summary-box{background:var(--card2);border:1px solid var(--border);border-radius:12px;
  padding:14px;font-size:12.5px;color:var(--text2);line-height:1.6;margin-bottom:10px}
.sa-meta-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:14px}
.sa-meta-box{background:var(--card2);border:1px solid var(--border);border-radius:10px;padding:10px}
.sa-meta-lbl{font-size:9px;font-weight:800;text-transform:uppercase;letter-spacing:.8px;
  color:var(--text3);margin-bottom:3px}
.sa-meta-val{font-size:14px;font-weight:800}
.home-ai-section{margin-top:4px}
.home-ai-card{background:var(--card);border:1px solid var(--border);border-radius:14px;
  padding:13px 14px;margin-bottom:8px;cursor:pointer;transition:all .15s}
.home-ai-card:active{transform:scale(.98)}
/* ── Score Guide ───── */
.score-guide-row{display:grid;grid-template-columns:110px 60px 1fr;gap:4px 8px;
  padding:5px 0;border-bottom:1px solid var(--border);align-items:center}
.score-guide-row:last-child{border-bottom:none}
.sg-label{font-size:11px;font-weight:700;color:var(--text1)}
.sg-pts{font-size:11px;font-weight:800;text-align:right}
.sg-desc{font-size:10px;color:var(--text3);line-height:1.4}
/* ── Momentum Alert Card ─── */
.mo-alert-card{border-radius:12px;padding:11px 13px;margin-bottom:8px;
  border-left:3px solid var(--red);background:rgba(248,113,113,.07)}
.mo-alert-card.urgency-medium{border-color:var(--yellow);background:rgba(251,191,36,.07)}
.mo-alert-card.action-monitor{border-color:var(--border2);background:var(--card)}
.mo-alert-head{display:flex;align-items:center;gap:8px;margin-bottom:5px}
.mo-alert-sym{font-size:14px;font-weight:800;color:var(--text1)}
.mo-alert-action{font-size:10px;font-weight:700;padding:2px 7px;border-radius:6px;
  background:rgba(248,113,113,.2);color:var(--red)}
.mo-alert-action.action-close_soon{background:rgba(251,191,36,.2);color:var(--yellow)}
.mo-alert-action.action-monitor{background:var(--border);color:var(--text3)}

/* ── ANIMATED BACKGROUND ─────────────────────────────── */
.bg-canvas{position:fixed;inset:0;z-index:-1;pointer-events:none;overflow:hidden}
.bg-grid{position:absolute;inset:0;
  background-image:linear-gradient(var(--grid-line) 1px,transparent 1px),
                   linear-gradient(90deg,var(--grid-line) 1px,transparent 1px);
  background-size:44px 44px;
  animation:gridDrift 60s linear infinite}
:root[data-theme="dark"]{--grid-line:rgba(0,122,255,.05)}
:root[data-theme="light"]{--grid-line:rgba(0,122,255,.04)}
@keyframes gridDrift{from{background-position:0 0}to{background-position:44px 44px}}
.bg-orb{position:absolute;border-radius:50%;filter:blur(90px);animation:orbFloat linear infinite}
.bg-orb-1{width:340px;height:340px;top:-80px;left:-60px;
  background:var(--accent);opacity:.07;animation-duration:28s}
.bg-orb-2{width:260px;height:260px;bottom:10%;right:-80px;
  background:var(--cyan);opacity:.05;animation-duration:22s;animation-delay:-8s}
.bg-orb-3{width:200px;height:200px;bottom:40%;left:30%;
  background:var(--accent2);opacity:.045;animation-duration:35s;animation-delay:-14s}
@keyframes orbFloat{
  0%  {transform:translate(0,0) scale(1)}
  25% {transform:translate(20px,-30px) scale(1.08)}
  50% {transform:translate(-15px,20px) scale(.95)}
  75% {transform:translate(25px,10px) scale(1.04)}
  100%{transform:translate(0,0) scale(1)}
}
/* ── ACCOUNT CONTROL CARDS ───────────────────────────── */
.acct-ctrl-card{background:var(--card2);border:1px solid var(--border);border-radius:14px;margin-bottom:10px;overflow:hidden;
  backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur)}
.acct-ctrl-head{display:flex;align-items:center;justify-content:space-between;padding:14px;cursor:pointer;transition:background .15s}
.acct-ctrl-head:active{background:var(--card)}
.acct-ctrl-body{padding:0 14px 14px}
.accc-tab{flex:1;background:none;border:none;cursor:pointer;padding:7px;border-radius:7px;font-size:11px;font-weight:700;color:var(--text3);transition:all .15s}
.accc-tab.active{background:var(--accent);color:#fff}

/* Scan line shimmer on topbar */
.topbar::after{content:'';position:absolute;inset:0;pointer-events:none;
  background:linear-gradient(105deg,transparent 40%,rgba(255,107,53,.04) 50%,transparent 60%);
  background-size:200% 100%;animation:shimmer 6s ease-in-out infinite}
@keyframes shimmer{0%,100%{background-position:200% 0}50%{background-position:-200% 0}}
@keyframes fng-spin{to{transform:rotate(360deg)}}
</style>
</head>
<body>
<!-- ANIMATED BACKGROUND -->
<div class="bg-canvas" aria-hidden="true">
  <div class="bg-grid"></div>
  <div class="bg-orb bg-orb-1"></div>
  <div class="bg-orb bg-orb-2"></div>
  <div class="bg-orb bg-orb-3"></div>
</div>
<div class="app">

<!-- TOP BAR -->
<div class="topbar">
  <div style="display:flex;align-items:center;gap:10px">
    <!-- Prolific 3D logo mark -->
    <svg width="42" height="42" viewBox="0 0 42 42" fill="none" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <linearGradient id="lg-face" x1="0" y1="0" x2="0.6" y2="1" gradientUnits="objectBoundingBox">
          <stop offset="0%"   stop-color="#FFE566"/>
          <stop offset="45%"  stop-color="#FFAB00"/>
          <stop offset="100%" stop-color="#E06800"/>
        </linearGradient>
        <linearGradient id="lg-right" x1="0" y1="0" x2="1" y2="0" gradientUnits="objectBoundingBox">
          <stop offset="0%"   stop-color="#B36200"/>
          <stop offset="100%" stop-color="#4A2A00"/>
        </linearGradient>
        <linearGradient id="lg-bot" x1="0" y1="0" x2="0" y2="1" gradientUnits="objectBoundingBox">
          <stop offset="0%"   stop-color="#7A4400"/>
          <stop offset="100%" stop-color="#3D1E00"/>
        </linearGradient>
        <radialGradient id="lg-shine" cx="30%" cy="28%" r="55%">
          <stop offset="0%"   stop-color="rgba(255,255,230,0.65)"/>
          <stop offset="100%" stop-color="rgba(255,255,255,0)"/>
        </radialGradient>
        <filter id="lg-shad" x="-25%" y="-25%" width="170%" height="170%">
          <feDropShadow dx="0" dy="5" stdDeviation="5" flood-color="#000" flood-opacity="0.42"/>
        </filter>
      </defs>
      <!-- 3D right face -->
      <path d="M32 5 L39 10 L39 35 L32 30 Z" fill="url(#lg-right)"/>
      <!-- 3D bottom face -->
      <path d="M5 32 L32 32 L39 37 L12 37 Z" fill="url(#lg-bot)"/>
      <!-- Main front face -->
      <rect x="3" y="3" width="29" height="29" rx="8" fill="url(#lg-face)" filter="url(#lg-shad)"/>
      <!-- Top-edge highlight -->
      <rect x="3" y="3" width="29" height="4" rx="4" fill="rgba(255,255,210,0.55)"/>
      <!-- Left-edge highlight -->
      <rect x="3" y="3" width="4" height="29" rx="4" fill="rgba(255,255,210,0.35)"/>
      <!-- Glass shine spot -->
      <ellipse cx="13" cy="12" rx="9" ry="5.5" fill="url(#lg-shine)"/>
      <!-- P letterform -->
      <path d="M8 10h9a5.5 5.5 0 0 1 0 11h-4.5v7H8V10z" fill="white" opacity="0.95"/>
      <!-- Trend arrow (right area) -->
      <polyline points="20,25 22,20 24,23 28,14" stroke="white" stroke-width="2.2"
        stroke-linecap="round" stroke-linejoin="round" opacity="0.9"/>
      <polygon points="30,12 26,13 28,16" fill="white" opacity="0.9"/>
    </svg>
    <div>
      <div class="brand-name">Prolific</div>
      <div class="brand-sub">Signals Bot</div>
    </div>
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

<!-- MARKET PANEL: TOP GAINERS / LOSERS -->
<!-- Market panel toggle bar -->
<div style="display:flex;align-items:center;justify-content:space-between;
  background:var(--card);border-bottom:1px solid var(--border);padding:0 12px;height:28px">
  <span style="font-size:9.5px;font-weight:800;letter-spacing:.1em;color:var(--text3);text-transform:uppercase">
    <span class="mkt-pulse gain" style="margin-right:5px"></span>Market Movers
  </span>
  <button id="mkt-toggle-btn" onclick="toggleMarketPanel()"
    style="font-size:10px;font-weight:700;color:var(--accent2);background:none;border:none;
    cursor:pointer;padding:4px 0;letter-spacing:.03em">Show ▾</button>
</div>
<div class="market-panel" id="market-panel" style="display:none">
  <div class="market-col">
    <div class="market-col-hd gain">
      <span class="mkt-pulse gain"></span>Top Gainers
    </div>
    <div id="mkt-gainers">
      <div class="mkt-row"><span class="mkt-sym" style="color:var(--text3)">Loading…</span></div>
    </div>
  </div>
  <div class="market-col">
    <div class="market-col-hd lose">
      <span class="mkt-pulse lose"></span>Top Losers
    </div>
    <div id="mkt-losers">
      <div class="mkt-row"><span class="mkt-sym" style="color:var(--text3)">Loading…</span></div>
    </div>
  </div>
</div>

<!-- PAGES -->
<div class="pages">

<!-- ① HOME -->
<div class="page active" id="page-home"><div class="pad">

  <!-- Hero balance -->
  <div class="hero">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:2px">
      <div class="hero-lbl">Primary Balance</div>
      <button onclick="toggleBalVis()" id="bal-vis-btn" style="background:rgba(255,255,255,.15);
        border:1px solid rgba(255,255,255,.2);border-radius:8px;padding:4px 8px;cursor:pointer;
        display:flex;align-items:center;gap:4px;font-size:9px;font-weight:700;color:rgba(255,255,255,.8)">
        <svg id="bal-vis-icon" width="13" height="13" fill="none" viewBox="0 0 24 24"
          stroke="currentColor" stroke-width="2.2">
          <path stroke-linecap="round" stroke-linejoin="round"
            d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/>
          <circle cx="12" cy="12" r="3" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
        <span id="bal-vis-lbl">Hide</span>
      </button>
    </div>
    <div class="hero-amt" id="b-equity">— USDT</div>
    <div class="hero-sub">Bybit Unified · <span id="b-ts">—</span></div>
    <div class="bal-grid">
      <div class="bal-box">
        <div class="bal-lbl">Available</div>
        <div class="bal-val" style="color:#BAE6FD" id="b-avail">—</div>
        <div class="bal-sub">USDT</div>
      </div>
      <div class="bal-box">
        <div class="bal-lbl">Used Margin</div>
        <div class="bal-val" style="color:#FDE68A" id="b-margin">—</div>
        <div class="bal-sub">USDT</div>
      </div>
      <div class="bal-box">
        <div class="bal-lbl">Unrealised PnL</div>
        <div class="bal-val" id="b-upnl">—</div>
        <div class="bal-sub">USDT</div>
      </div>
      <div class="bal-box">
        <div class="bal-lbl">Per Trade</div>
        <div class="bal-val" style="color:#A5F3FC" id="b-pertrade">—</div>
        <div class="bal-sub">@ <span id="b-lev">—</span>×</div>
      </div>
    </div>
  </div>

  <!-- Win rate + signal stats -->
  <div style="display:grid;grid-template-columns:auto 1fr;gap:10px;margin-bottom:12px;align-items:center">
    <div class="card" style="margin-bottom:0;padding:14px 16px;text-align:center;min-width:130px">
      <div class="gauge-wrap">
        <svg class="gauge-svg" viewBox="0 0 140 76">
          <path class="gauge-track" d="M14,70 A56,56 0 0,1 126,70"/>
          <path class="gauge-fill" id="h-wr-arc" d="M14,70 A56,56 0 0,1 126,70"
            stroke="var(--accent)" stroke-dasharray="176" stroke-dashoffset="176"/>
          <text class="gauge-center" x="70" y="66" id="h-wr">—</text>
          <text class="gauge-sub" x="70" y="80">Win Rate</text>
        </svg>
      </div>
      <span class="wr-badge" id="h-wr-badge" style="display:inline-block">—</span>
    </div>
    <div style="display:flex;flex-direction:column;gap:8px">
      <div class="stat-grid" style="grid-template-columns:repeat(3,1fr);margin-bottom:0">
        <div class="stat-box"><div class="stat-num" style="color:var(--green)" id="s-wins">—</div><div class="stat-lbl">Wins</div></div>
        <div class="stat-box"><div class="stat-num" style="color:var(--red)" id="s-losses">—</div><div class="stat-lbl">Losses</div></div>
        <div class="stat-box"><div class="stat-num" style="color:var(--accent2)" id="s-total">—</div><div class="stat-lbl">Signals</div></div>
      </div>
      <!-- hidden elements kept for JS compat -->
      <div style="display:none" id="h-wr-bar"></div>
      <div style="display:none" id="h-wr-sub"></div>
      <!-- Phase Tracker card -->
      <div class="card" style="margin-bottom:0;padding:10px 12px" id="phase-card">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">
          <span style="font-size:10px;font-weight:800;letter-spacing:.08em;color:var(--text3)">RISK STRATEGY</span>
          <span id="phase-badge" style="font-size:10px;font-weight:800;padding:2px 8px;border-radius:6px;
            background:var(--accentbg);color:var(--accent2)">Phase —</span>
        </div>
        <div id="phase-bar-wrap" style="height:4px;background:var(--border);border-radius:2px;margin-bottom:6px;overflow:hidden">
          <div id="phase-bar" style="height:100%;background:var(--accent);border-radius:2px;
            transition:width .6s ease;width:0%"></div>
        </div>
        <div style="display:flex;justify-content:space-between;font-size:10px;color:var(--text3);margin-bottom:5px">
          <span id="phase-eq-lbl">$— of $—</span>
          <span id="phase-next-lbl"></span>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:4px">
          <div style="text-align:center;background:var(--card2);border-radius:6px;padding:4px 2px">
            <div style="font-size:11px;font-weight:800;color:var(--accent2)" id="ph-risk">—</div>
            <div style="font-size:9px;color:var(--text3)">Risk/trade</div>
          </div>
          <div style="text-align:center;background:var(--card2);border-radius:6px;padding:4px 2px">
            <div style="font-size:11px;font-weight:800;color:var(--cyan)" id="ph-score">—</div>
            <div style="font-size:9px;color:var(--text3)">Score gate</div>
          </div>
          <div style="text-align:center;background:var(--card2);border-radius:6px;padding:4px 2px">
            <div style="font-size:11px;font-weight:800;color:var(--text2)" id="ph-sl">—</div>
            <div style="font-size:9px;color:var(--text3)">Auto-SL</div>
          </div>
        </div>
      </div>
      <!-- hidden elements kept for JS compat -->
      <div style="display:none" id="cfg-auto"></div>
      <div style="display:none" id="cfg-eq"></div>
      <div style="display:none" id="cfg-lev"></div>
    </div>
  </div>

  <!-- Fear & Greed Index -->
  <div class="card" id="fng-card" style="margin-bottom:12px;padding:14px 16px">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
      <div style="font-size:10px;font-weight:800;letter-spacing:.08em;color:var(--text3)">CRYPTO FEAR &amp; GREED</div>
      <div style="display:flex;align-items:center;gap:6px">
        <div style="font-size:9px;color:var(--text3)" id="fng-updated">Updating…</div>
        <button onclick="fngRefresh(this)" style="background:none;border:none;padding:2px;cursor:pointer;
          color:var(--text3);display:flex;align-items:center;line-height:1" title="Refresh">
          <svg id="fng-refresh-icon" width="12" height="12" viewBox="0 0 24 24" fill="none"
            stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
            <polyline points="23 4 23 10 17 10"/>
            <path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/>
          </svg>
        </button>
      </div>
    </div>
    <div style="display:flex;align-items:center;gap:16px">
      <!-- Dial -->
      <div style="position:relative;width:80px;height:44px;flex-shrink:0">
        <svg width="80" height="44" viewBox="0 0 80 44">
          <!-- track arc: semicircle radius 36, centre 40,40 -->
          <path d="M4,40 A36,36 0 0,1 76,40" fill="none" stroke="rgba(128,128,128,.15)" stroke-width="8" stroke-linecap="round"/>
          <!-- coloured fill arc -->
          <path id="fng-arc" d="M4,40 A36,36 0 0,1 76,40" fill="none"
            stroke="#FF3B30" stroke-width="8" stroke-linecap="round"
            stroke-dasharray="113" stroke-dashoffset="113"
            style="transition:stroke-dashoffset .8s cubic-bezier(.4,0,.2,1),stroke .4s"/>
          <!-- needle -->
          <line id="fng-needle" x1="40" y1="40" x2="40" y2="8"
            stroke="var(--text)" stroke-width="2" stroke-linecap="round"
            style="transform-origin:40px 40px;transform:rotate(-90deg);transition:transform .8s cubic-bezier(.4,0,.2,1)"/>
          <circle cx="40" cy="40" r="3" fill="var(--text)"/>
        </svg>
        <div id="fng-val" style="position:absolute;bottom:0;left:0;right:0;text-align:center;
          font-size:22px;font-weight:900;letter-spacing:-1px;color:var(--text);line-height:1">—</div>
      </div>
      <!-- Label + zone bar -->
      <div style="flex:1;min-width:0">
        <div id="fng-label" style="font-size:16px;font-weight:900;letter-spacing:-.4px;
          color:var(--text);margin-bottom:6px">—</div>
        <!-- 5-zone gradient bar -->
        <div style="height:5px;border-radius:3px;background:linear-gradient(90deg,
          #FF3B30 0%,#FF9500 25%,#FFD60A 50%,#34C759 75%,#00C7BE 100%);
          margin-bottom:6px;position:relative">
          <div id="fng-marker" style="position:absolute;top:-3px;width:10px;height:10px;
            border-radius:50%;background:#fff;border:2px solid var(--text);
            transform:translateX(-50%);left:50%;
            transition:left .8s cubic-bezier(.4,0,.2,1)"></div>
        </div>
        <!-- Zone labels -->
        <div style="display:flex;justify-content:space-between;font-size:8px;color:var(--text3);font-weight:700">
          <span>Ext. Fear</span><span>Fear</span><span>Neutral</span><span>Greed</span><span>Ext. Greed</span>
        </div>
      </div>
    </div>
  </div>

  <!-- Accounts Overview -->
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
    <div class="section-lbl" style="margin-bottom:0">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4" stroke-linecap="round" stroke-linejoin="round"/><path stroke-linecap="round" stroke-linejoin="round" d="M23 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75"/></svg>
      Accounts
    </div>
    <button class="btn btn-ghost btn-sm" onclick="goTab('accounts')" style="width:auto;padding:5px 10px;font-size:10px">Manage ›</button>
  </div>
  <div id="home-accounts">
    <div class="acct-mini" style="cursor:default;opacity:.5"><div class="acct-mini-body"><div class="acct-mini-name">Loading accounts…</div></div></div>
  </div>

  <!-- Quick actions -->
  <div class="qa-grid" style="margin-top:12px">
    <div class="qa" onclick="goTab('positions')">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><polyline stroke-linecap="round" stroke-linejoin="round" points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
      <span class="qa-lbl">Positions</span>
    </div>
    <div class="qa" onclick="goTab('signals')">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M8.111 16.404a5.5 5.5 0 0 1 7.778 0M12 20h.01M1.394 9.393c5.857-5.857 15.355-5.857 21.213 0M5.105 12.682a9.5 9.5 0 0 1 13.79 0"/></svg>
      <span class="qa-lbl">Signals</span>
    </div>
    <div class="qa" onclick="refreshNow()">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><polyline stroke-linecap="round" stroke-linejoin="round" points="23 4 23 10 17 10"/><path stroke-linecap="round" stroke-linejoin="round" d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>
      <span class="qa-lbl">Refresh</span>
    </div>
  </div>

  <!-- Config detail row (hidden fields kept for JS compat) -->
  <div style="display:none"><span id="cfg-ts">—</span></div>

  <!-- AI Signal Analysis section -->
  <div style="display:flex;justify-content:space-between;align-items:center;margin:14px 0 8px">
    <div class="section-lbl" style="margin-bottom:0">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17H3a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2h-2"/></svg>
      AI Analysis
    </div>
    <button class="btn btn-ghost btn-sm" onclick="goTab('signals')" style="width:auto;padding:5px 10px;font-size:10px">All Signals ›</button>
  </div>
  <div class="home-ai-section" id="home-ai-section">
    <div style="text-align:center;padding:20px;color:var(--text3);font-size:12px">No signals analysed yet</div>
  </div>

  <!-- MOMENTUM ALERTS -->
  <div style="display:flex;align-items:center;justify-content:space-between;margin:14px 0 6px">
    <div class="card-label" style="margin:0">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9"/></svg>
      Momentum Alerts
      <span id="home-mo-dot" class="nav-dot" style="display:none"></span>
    </div>
    <button class="btn btn-ghost btn-sm" onclick="loadMomentumAlerts()" style="width:auto;padding:5px 10px;font-size:10px">Refresh ↺</button>
  </div>
  <div id="home-momentum-alerts">
    <div style="text-align:center;padding:16px;color:var(--text3);font-size:12px">No alerts — positions momentum is stable</div>
  </div>

  <!-- AI SCORE GUIDE -->
  <div style="display:flex;align-items:center;justify-content:space-between;margin:14px 0 8px">
    <div class="card-label" style="margin:0">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"/></svg>
      AI Score Guide
    </div>
    <button id="guide-toggle-btn" onclick="toggleScoreGuide()"
      style="font-size:10px;font-weight:700;color:var(--accent2);background:none;border:none;
      cursor:pointer;padding:4px 0;letter-spacing:.03em">Show ▾</button>
  </div>
  <div id="score-guide-body" style="display:none">
  <div class="card" style="padding:0;overflow:hidden">
    <div style="padding:10px 12px 6px;background:var(--card2)">
      <div style="font-size:10px;font-weight:700;color:var(--text3);letter-spacing:.08em">HOW SIGNALS ARE SCORED (0 – 100)</div>
    </div>
    <div style="padding:8px 12px 4px">
      <div class="score-guide-row"><span class="sg-label">R:R ≥ 2:1</span><span class="sg-pts pos">+20 pts</span><span class="sg-desc">Reward far outweighs risk — ideal setup</span></div>
      <div class="score-guide-row"><span class="sg-label">R:R 1:1 – 2:1</span><span class="sg-pts" style="color:var(--accent)">+10 pts</span><span class="sg-desc">Acceptable but not optimal</span></div>
      <div class="score-guide-row"><span class="sg-label">R:R &lt; 1:1</span><span class="sg-pts neg">−15 pts</span><span class="sg-desc">Risk exceeds reward — red flag</span></div>
      <div class="score-guide-row"><span class="sg-label">Stop Loss set</span><span class="sg-pts pos">+10 pts</span><span class="sg-desc">Downside is defined and managed</span></div>
      <div class="score-guide-row"><span class="sg-label">Take Profit set</span><span class="sg-pts pos">+10 pts</span><span class="sg-desc">Exit plan exists — disciplined trade</span></div>
      <div class="score-guide-row"><span class="sg-label">Trend alignment</span><span class="sg-pts pos">+15 pts</span><span class="sg-desc">Signal direction matches 24h momentum</span></div>
      <div class="score-guide-row"><span class="sg-label">Trend counter</span><span class="sg-pts neg">−20 pts</span><span class="sg-desc">Fighting current market momentum</span></div>
      <div class="score-guide-row"><span class="sg-label">Leverage ≤ 5×</span><span class="sg-pts pos">+5 pts</span><span class="sg-desc">Conservative — lower liquidation risk</span></div>
      <div class="score-guide-row"><span class="sg-label">Leverage &gt; 15×</span><span class="sg-pts neg">−10 pts</span><span class="sg-desc">High risk of liquidation on volatility</span></div>
      <div class="score-guide-row"><span class="sg-label">Volume &gt; $50M</span><span class="sg-pts pos">+5 pts</span><span class="sg-desc">Liquid market — tighter spreads</span></div>
      <div class="score-guide-row"><span class="sg-label">Win rate &gt; 70%</span><span class="sg-pts pos">+5 pts</span><span class="sg-desc">Channel has strong historical accuracy</span></div>
    </div>
    <div style="padding:6px 12px 10px;display:flex;gap:8px;flex-wrap:wrap">
      <span class="pill" style="background:rgba(0,200,83,.12);color:#00c853;border:1px solid rgba(0,200,83,.3)">≥ 80 — Strong Win</span>
      <span class="pill" style="background:rgba(0,200,83,.08);color:#4caf50;border:1px solid rgba(0,200,83,.2)">75–79 — Likely Win</span>
      <span class="pill" style="background:rgba(255,107,53,.1);color:var(--accent);border:1px solid rgba(255,107,53,.3)">60–74 — Neutral/Caution</span>
      <span class="pill" style="background:rgba(244,67,54,.1);color:#ef5350;border:1px solid rgba(244,67,54,.3)">&lt;60 — Skip / Loss Risk</span>
    </div>
  </div>
  </div><!-- /score-guide-body -->

  <div class="countdown" id="cd">—</div>
</div></div>

<!-- ② POSITIONS -->
<div class="page" id="page-positions"><div class="pad">
  <div class="card mb">
    <div class="card-label">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><rect x="2" y="3" width="20" height="14" rx="2" stroke-linecap="round" stroke-linejoin="round"/><line x1="8" y1="21" x2="16" y2="21" stroke-linecap="round" stroke-linejoin="round"/><line x1="12" y1="17" x2="12" y2="21" stroke-linecap="round" stroke-linejoin="round"/></svg>
      Portfolio · <span class="live-dot"></span>All Accounts
    </div>
    <div class="acct-tabs" id="pos-acct-tabs">
      <button class="acct-tab active" onclick="setPosAccount('all',this)">All</button>
      <button class="acct-tab" onclick="setPosAccount('primary',this)">Primary</button>
    </div>
    <div style="display:flex;justify-content:space-between;align-items:center">
      <div><div style="font-size:36px;font-weight:900;color:var(--accent2)" id="p-count">0</div><div style="font-size:11px;color:var(--text3)">open positions</div></div>
      <div style="text-align:right">
        <div style="font-size:26px;font-weight:900" id="p-tpnl">—</div>
        <div style="font-size:11px;color:var(--text3)">unrealised PnL</div>
      </div>
    </div>
  </div>
  <div id="pos-list"><div class="empty"><svg width="40" height="40" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M5 8h14M5 8a2 2 0 1 0 0-4h14a2 2 0 1 0 0 4M5 8v10a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8m-9 4h4"/></svg><div style="margin-top:8px">Fetching positions…</div></div></div>
  <div id="pos-close-all-wrap" style="display:none;margin:8px 0 4px">
    <button class="btn btn-red btn-sm" style="width:auto;padding:9px 18px" onclick="closeAllVisible()">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><circle cx="12" cy="12" r="10" stroke-linecap="round" stroke-linejoin="round"/><line x1="15" y1="9" x2="9" y2="15" stroke-linecap="round" stroke-linejoin="round"/><line x1="9" y1="9" x2="15" y2="15" stroke-linecap="round" stroke-linejoin="round"/></svg>
      Close All Visible
    </button>
  </div>
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
    <div class="inp-wrap"><label class="inp-lbl">Leverage (×)</label><input class="inp" type="number" id="inp-lev" min="1" max="100" placeholder="5"></div>
    <div class="inp-wrap"><label class="inp-lbl" style="display:none">Equity %</label><input class="inp" type="number" id="inp-eq" min="1" max="100" placeholder="10" style="display:none"></div>
  </div>

  <div class="div"></div>
  <!-- Risk Strategy — tap to open detail sheet -->
  <div onclick="openRiskDetail()" style="display:flex;align-items:center;justify-content:space-between;
    background:var(--card);border:1px solid var(--border);border-radius:14px;
    padding:14px 16px;margin-bottom:10px;cursor:pointer;transition:all .15s;
    backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur)">
    <div style="display:flex;align-items:center;gap:10px">
      <div style="width:36px;height:36px;border-radius:10px;background:var(--accentbg);
        display:flex;align-items:center;justify-content:center;flex-shrink:0">
        <svg width="18" height="18" fill="none" viewBox="0 0 24 24" stroke="var(--accent)" stroke-width="2">
          <path stroke-linecap="round" stroke-linejoin="round" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"/>
        </svg>
      </div>
      <div>
        <div style="font-size:13px;font-weight:800">Risk Strategy</div>
        <div style="font-size:10px;color:var(--text3);margin-top:1px">Phase · Sizing · Score gate</div>
      </div>
    </div>
    <div style="display:flex;align-items:center;gap:8px">
      <span id="settings-risk-preview" style="font-size:11px;font-weight:700;color:var(--accent)">2% risk</span>
      <svg width="16" height="16" fill="none" viewBox="0 0 24 24" stroke="currentColor"
        stroke-width="2.5" style="color:var(--text3)">
        <polyline points="9 18 15 12 9 6" stroke-linecap="round" stroke-linejoin="round"/>
      </svg>
    </div>
  </div>
  <!-- Hidden inputs still needed for applySettings() -->
  <input type="hidden" id="inp-risk" value="2">
  <input type="hidden" id="inp-sl"   value="3">
  <input type="hidden" id="inp-score" value="60">

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
    <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4" stroke-linecap="round" stroke-linejoin="round"/><path stroke-linecap="round" stroke-linejoin="round" d="M23 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75"/></svg>
    Account Controls
  </div>
  <div id="acct-ctrl-list">
    <div style="font-size:12px;color:var(--text3);text-align:center;padding:12px">Loading…</div>
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

<!-- CLOSE POSITION CONFIRM SHEET -->
<div class="cs-overlay" id="cs-overlay" onclick="if(event.target===this)cancelClose()">
  <div class="cs-sheet">
    <div class="cs-handle"></div>
    <div style="font-size:10.5px;font-weight:800;text-transform:uppercase;letter-spacing:1px;color:var(--text3);margin-bottom:6px">Close Position</div>
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
      <div style="font-size:26px;font-weight:900" id="cs-symbol">—</div>
      <span id="cs-mode-badge"></span>
    </div>
    <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:4px">
      <span class="tag" id="cs-side-tag">—</span>
      <span class="tag blue" id="cs-lev-tag">—</span>
      <span class="tag" id="cs-acct-tag" style="color:var(--accent2)">—</span>
    </div>
    <div class="cs-stats">
      <div class="cs-stat">
        <div class="cs-stat-lbl">Entry Price</div>
        <div class="cs-stat-val" id="cs-entry">—</div>
      </div>
      <div class="cs-stat">
        <div class="cs-stat-lbl">Size</div>
        <div class="cs-stat-val" id="cs-size">—</div>
      </div>
      <div class="cs-stat">
        <div class="cs-stat-lbl">Unrealised PnL</div>
        <div class="cs-stat-val" id="cs-pnl">—</div>
      </div>
      <div class="cs-stat">
        <div class="cs-stat-lbl">PnL %</div>
        <div class="cs-stat-val" id="cs-pct">—</div>
      </div>
    </div>
    <div id="cs-demo-note" class="cs-demo-note" style="display:none">
      🎮 <strong>Demo position</strong> — no real funds at risk
    </div>
    <div class="cs-warn">
      ⚠️ Closes immediately at market price. Cannot be undone.
    </div>
    <button class="btn btn-red" id="cs-confirm-btn" onclick="confirmClose()">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><circle cx="12" cy="12" r="10" stroke-linecap="round" stroke-linejoin="round"/><line x1="15" y1="9" x2="9" y2="15" stroke-linecap="round" stroke-linejoin="round"/><line x1="9" y1="9" x2="15" y2="15" stroke-linecap="round" stroke-linejoin="round"/></svg>
      Close Position
    </button>
    <button class="btn btn-ghost btn-sm" onclick="cancelClose()" style="margin-top:8px">Cancel</button>
  </div>
</div>

<!-- SL / TP SHEET -->
<div class="cs-overlay" id="sltp-overlay" onclick="if(event.target===this)cancelSLTP()">
  <div class="cs-sheet">
    <div class="cs-handle"></div>
    <div style="font-size:10.5px;font-weight:800;text-transform:uppercase;letter-spacing:1px;color:var(--text3);margin-bottom:6px" id="sltp-mode-lbl">Set Stop Loss / Take Profit</div>
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:14px">
      <div style="font-size:22px;font-weight:900" id="sltp-symbol">—</div>
      <span id="sltp-side-tag" class="tag"></span>
    </div>

    <!-- SL row -->
    <div style="margin-bottom:12px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:5px">
        <label style="font-size:11px;font-weight:700;color:var(--red)">🛑 Stop Loss</label>
        <span style="font-size:10px;color:var(--text3)" id="sltp-cur-sl">current: —</span>
      </div>
      <div style="display:flex;gap:6px">
        <input class="inp" id="sltp-sl-inp" type="number" step="any" placeholder="Price (0 to clear)" style="flex:1;font-size:14px"/>
        <button class="btn btn-ghost btn-sm" onclick="clearSLTP('sl')" style="width:auto;padding:0 12px;font-size:11px;color:var(--text3)">Clear</button>
      </div>
    </div>

    <!-- TP row -->
    <div style="margin-bottom:16px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:5px">
        <label style="font-size:11px;font-weight:700;color:var(--green)">🎯 Take Profit</label>
        <span style="font-size:10px;color:var(--text3)" id="sltp-cur-tp">current: —</span>
      </div>
      <div style="display:flex;gap:6px">
        <input class="inp" id="sltp-tp-inp" type="number" step="any" placeholder="Price (0 to clear)" style="flex:1;font-size:14px"/>
        <button class="btn btn-ghost btn-sm" onclick="clearSLTP('tp')" style="width:auto;padding:0 12px;font-size:11px;color:var(--text3)">Clear</button>
      </div>
    </div>

    <div class="cs-warn" style="margin-bottom:12px">Orders execute at market when price is reached.</div>
    <button class="btn btn-primary" id="sltp-confirm-btn" onclick="confirmSLTP()">
      <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" style="width:14px;height:14px"><path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"/></svg>
      Set SL / TP
    </button>
    <button class="btn btn-ghost btn-sm" onclick="cancelSLTP()" style="margin-top:8px">Cancel</button>
  </div>
</div>

<!-- ACCOUNT DETAIL SHEET -->
<!-- ── Full Account Trading Panel ────────────────────── -->
<div class="cs-overlay" id="ad-overlay" onclick="if(event.target===this)closeAccountDetail()">
  <div class="cs-sheet" style="max-height:92vh;overflow-y:auto;padding-bottom:20px">
    <div class="cs-handle"></div>

    <!-- Header -->
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px">
      <div id="ad-icon" style="width:40px;height:40px;border-radius:12px;background:var(--accentbg);
        display:flex;align-items:center;justify-content:center;flex-shrink:0">
        <svg width="20" height="20" fill="none" viewBox="0 0 24 24" stroke="var(--accent)" stroke-width="2">
          <path stroke-linecap="round" stroke-linejoin="round" d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/>
          <circle cx="9" cy="7" r="4" stroke-linecap="round" stroke-linejoin="round"/>
          <path stroke-linecap="round" stroke-linejoin="round" d="M23 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75"/>
        </svg>
      </div>
      <div style="flex:1;min-width:0">
        <div style="font-size:16px;font-weight:900;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" id="ad-name">—</div>
        <div id="ad-badge-wrap" style="margin-top:3px;display:flex;align-items:center;gap:6px">
          <span id="ad-type-badge"></span>
          <span id="ad-prop-badge" style="display:none;font-size:9px;font-weight:800;
            padding:2px 7px;border-radius:5px;background:rgba(251,191,36,.15);color:var(--yellow)">PROP FIRM</span>
        </div>
      </div>
      <button onclick="adRefresh()" style="background:none;border:none;cursor:pointer;color:var(--text3);padding:6px">
        <svg width="16" height="16" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2.5">
          <polyline stroke-linecap="round" stroke-linejoin="round" points="23 4 23 10 17 10"/>
          <path stroke-linecap="round" stroke-linejoin="round" d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/>
        </svg>
      </button>
    </div>

    <!-- Balance grid -->
    <div class="ad-grid" style="margin-bottom:10px">
      <div class="ad-stat">
        <div class="ad-stat-lbl">Equity</div>
        <div class="ad-stat-val" id="ad-equity">—</div>
        <div class="ad-stat-sub">USDT</div>
      </div>
      <div class="ad-stat">
        <div class="ad-stat-lbl">Available</div>
        <div class="ad-stat-val" style="color:var(--cyan)" id="ad-avail">—</div>
        <div class="ad-stat-sub">USDT</div>
      </div>
      <div class="ad-stat">
        <div class="ad-stat-lbl">Margin</div>
        <div class="ad-stat-val" style="color:var(--accent2)" id="ad-margin">—</div>
        <div class="ad-stat-sub">USDT</div>
      </div>
      <div class="ad-stat">
        <div class="ad-stat-lbl">Unreal. PnL</div>
        <div class="ad-stat-val" id="ad-pnl">—</div>
        <div class="ad-stat-sub">USDT</div>
      </div>
    </div>

    <!-- Prop firm risk meters -->
    <div id="ad-prop-meters" style="display:none;margin-bottom:10px">
      <div style="font-size:10px;font-weight:800;letter-spacing:.08em;color:var(--yellow);
        text-transform:uppercase;margin-bottom:6px">Prop Firm Risk</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
        <div style="background:var(--card2);border:1px solid var(--border);border-radius:10px;padding:10px">
          <div style="font-size:10px;color:var(--text3);margin-bottom:4px;font-weight:700">Daily Loss</div>
          <div style="height:5px;background:var(--border);border-radius:3px;margin-bottom:4px;overflow:hidden">
            <div id="ad-dl-bar" style="height:100%;border-radius:3px;background:var(--green);transition:width .4s;width:0%"></div>
          </div>
          <div style="display:flex;justify-content:space-between;font-size:10px">
            <span id="ad-dl-val" style="font-weight:800">0.00%</span>
            <span style="color:var(--text3)">limit: <span id="ad-dl-limit">5%</span></span>
          </div>
        </div>
        <div style="background:var(--card2);border:1px solid var(--border);border-radius:10px;padding:10px">
          <div style="font-size:10px;color:var(--text3);margin-bottom:4px;font-weight:700">Max Drawdown</div>
          <div style="height:5px;background:var(--border);border-radius:3px;margin-bottom:4px;overflow:hidden">
            <div id="ad-dd-bar" style="height:100%;border-radius:3px;background:var(--green);transition:width .4s;width:0%"></div>
          </div>
          <div style="display:flex;justify-content:space-between;font-size:10px">
            <span id="ad-dd-val" style="font-weight:800">0.00%</span>
            <span style="color:var(--text3)">limit: <span id="ad-dd-limit">10%</span></span>
          </div>
        </div>
      </div>
    </div>

    <!-- Tab bar -->
    <div style="display:flex;gap:0;background:var(--card2);border:1px solid var(--border);
      border-radius:10px;padding:3px;margin-bottom:12px" id="ad-tab-bar">
      <button class="ad-tab active" onclick="adTab('positions',this)">Positions</button>
      <button class="ad-tab" onclick="adTab('trade',this)">Trade</button>
      <button class="ad-tab" onclick="adTab('risk',this)">Risk</button>
    </div>

    <!-- TAB: Positions -->
    <div id="ad-tab-positions">
      <div id="ad-positions">
        <div style="text-align:center;padding:20px;color:var(--text3);font-size:12px">Loading…</div>
      </div>
      <button class="btn btn-red btn-sm" style="width:100%;margin-top:10px" onclick="adCloseAll()">
        <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" style="width:13px;height:13px;stroke-width:2.5">
          <circle cx="12" cy="12" r="10" stroke-linecap="round"/><line x1="15" y1="9" x2="9" y2="15" stroke-linecap="round"/><line x1="9" y1="9" x2="15" y2="15" stroke-linecap="round"/>
        </svg>
        Close All Positions
      </button>
    </div>

    <!-- TAB: Trade -->
    <div id="ad-tab-trade" style="display:none">
      <div style="font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.8px;
        color:var(--text3);margin-bottom:10px">Manual Trade</div>
      <div class="inp-grid mb">
        <div class="inp-wrap">
          <label class="inp-lbl">Symbol</label>
          <input class="inp" type="text" id="ad-sym" placeholder="BTC" autocapitalize="characters" autocomplete="off">
        </div>
        <div class="inp-wrap">
          <label class="inp-lbl">Direction</label>
          <select class="inp" id="ad-side">
            <option value="Buy">Long ↑</option>
            <option value="Sell">Short ↓</option>
          </select>
        </div>
      </div>
      <div class="inp-grid mb">
        <div class="inp-wrap">
          <label class="inp-lbl">Size % of equity</label>
          <input class="inp" type="number" id="ad-size-pct" min="1" max="100" placeholder="10" value="10">
        </div>
        <div class="inp-wrap">
          <label class="inp-lbl">Leverage ×</label>
          <input class="inp" type="number" id="ad-lev" min="1" max="100" placeholder="5" value="5">
        </div>
      </div>
      <div class="inp-grid mb">
        <div class="inp-wrap">
          <label class="inp-lbl">Stop Loss (optional)</label>
          <input class="inp" type="number" id="ad-sl" placeholder="0.0000" step="any">
        </div>
        <div class="inp-wrap">
          <label class="inp-lbl">Take Profit (optional)</label>
          <input class="inp" type="number" id="ad-tp" placeholder="0.0000" step="any">
        </div>
      </div>
      <div id="ad-trade-preview" style="background:var(--card2);border:1px solid var(--border);
        border-radius:10px;padding:10px;margin-bottom:12px;font-size:11px;display:none">
        <div style="display:flex;justify-content:space-between;margin-bottom:4px">
          <span style="color:var(--text3)">Est. margin</span><span id="ad-prev-margin" style="font-weight:800">—</span>
        </div>
        <div style="display:flex;justify-content:space-between;margin-bottom:4px">
          <span style="color:var(--text3)">Position notional</span><span id="ad-prev-notional" style="font-weight:800">—</span>
        </div>
        <div style="display:flex;justify-content:space-between">
          <span style="color:var(--text3)">Max loss (SL hit)</span><span id="ad-prev-loss" style="font-weight:800;color:var(--red)">—</span>
        </div>
      </div>
      <div class="btn-grid">
        <button class="btn btn-green" onclick="adOpenTrade()">
          <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" style="width:14px;height:14px;stroke-width:2.5">
            <polyline stroke-linecap="round" stroke-linejoin="round" points="23 6 13.5 15.5 8.5 10.5 1 18"/>
            <polyline stroke-linecap="round" stroke-linejoin="round" points="17 6 23 6 23 12"/>
          </svg>
          Open Trade
        </button>
        <button class="btn btn-ghost" onclick="adClearTradeForm()">Clear</button>
      </div>
      <div id="ad-trade-msg" style="margin-top:10px;font-size:11px;text-align:center;display:none"></div>
    </div>

    <!-- TAB: Risk -->
    <div id="ad-tab-risk" style="display:none">
      <div style="font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.8px;
        color:var(--text3);margin-bottom:10px">Prop Firm Settings</div>
      <div class="toggle-row" style="margin-bottom:12px">
        <div class="toggle-info"><strong>Prop Firm Mode</strong><span>Enables daily loss & drawdown meters</span></div>
        <label class="switch"><input type="checkbox" id="ad-prop-toggle" onchange="adTogglePropMode(this)"><span class="sw-track"></span></label>
      </div>
      <div class="inp-grid mb">
        <div class="inp-wrap">
          <label class="inp-lbl">Daily Loss Limit %</label>
          <input class="inp" type="number" id="ad-dl-inp" min="1" max="20" step="0.5" placeholder="5" value="5">
        </div>
        <div class="inp-wrap">
          <label class="inp-lbl">Max Drawdown Limit %</label>
          <input class="inp" type="number" id="ad-dd-inp" min="1" max="30" step="0.5" placeholder="10" value="10">
        </div>
      </div>
      <div class="inp-wrap mb">
        <label class="inp-lbl">Account Size (starting capital $)</label>
        <input class="inp" type="number" id="ad-cap-inp" min="100" placeholder="10000">
      </div>
      <button class="btn btn-primary" onclick="adSavePropSettings()">Save Settings</button>
      <div style="background:var(--card2);border:1px solid var(--border);border-radius:10px;
        padding:12px;margin-top:14px;font-size:11px;color:var(--text3);line-height:1.7">
        <strong style="color:var(--yellow)">Prop firm rules enforced here:</strong><br>
        • Daily loss meter tracks unrealised PnL vs starting day balance<br>
        • Drawdown meter tracks equity drop from account starting capital<br>
        • Visual warning at 80% of each limit — red at 100%<br>
        • Auto Execute is automatically disabled if either limit is breached
      </div>
    </div>

    <button class="btn btn-ghost btn-sm" onclick="closeAccountDetail()" style="margin-top:14px;width:100%">Done</button>
  </div>
</div>

<script>
/* ── Theme ───────────────────────────────────────────── */
const _th = localStorage.getItem('theme') || 'light';
document.documentElement.setAttribute('data-theme', _th);
setThemeIcon(_th);
function setThemeIcon(t) {
  const ic = document.getElementById('theme-icon');
  const mt = document.getElementById('theme-meta');
  if (t === 'dark') {
    ic.innerHTML = `<path stroke-linecap="round" stroke-linejoin="round" d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>`;
    mt.content = '#000000';
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
    mt.content = '#F2F2F7';
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
let _momentumAlerts = [];
const _SIG_PAGE = 200;
let _allPositions = [];
let _posAccFilter = 'all';
let _closePending  = null;

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
  if (tab === 'positions') fetchPositions();
  if (tab === 'logs') renderLogs(logFilter);
  if (tab === 'settings') renderAcctControls();
}

/* ── Toast ───────────────────────────────────────────── */
function toast(msg, ok = true, dur = 3200) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.style.borderColor = ok ? 'rgba(34,197,94,.4)' : 'rgba(239,68,68,.4)';
  t.style.display = 'block';
  clearTimeout(t._tid);
  t._tid = setTimeout(() => t.style.display = 'none', dur);
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
    if (ev.type === 'momentum_alert') {
      const urgencyIcon = ev.urgency === 'high' ? '🔴' : '🟡';
      const actionMap   = {close_now:'CLOSE NOW',close_soon:'CLOSE SOON',monitor:'Monitor'};
      toast(`${urgencyIcon} ${actionMap[ev.action]||ev.action}: ${ev.symbol} — ${ev.reason.slice(0,60)}`, false, 6000);
      // Push into alerts store and re-render
      _momentumAlerts.unshift(ev);
      if (_momentumAlerts.length > 20) _momentumAlerts.length = 20;
      renderMomentumAlerts();
      // Flash the nav dot on home tab
      document.getElementById('home-mo-dot') && document.getElementById('home-mo-dot').classList.add('show');
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
  if (!_balVisible) _applyBalVis();  // re-mask after data refresh

  // Stats
  document.getElementById('s-wins').textContent   = d.stats.wins;
  document.getElementById('s-losses').textContent = d.stats.losses;
  document.getElementById('s-total').textContent  = d.stats.total;

  // Win rate + arc gauge
  const wr = d.stats.win_rate;
  document.getElementById('h-wr').textContent     = wr.toFixed(1) + '%';
  document.getElementById('h-wr-bar').style.width = Math.min(wr, 100) + '%';
  document.getElementById('h-wr-sub').textContent = `${d.stats.wins}W / ${d.stats.losses}L of ${d.stats.total}`;
  const pass = wr >= 70;
  const wb = document.getElementById('h-wr-badge');
  wb.textContent = pass ? '✅ PASSING' : '❌ FAILING';
  wb.className   = 'wr-badge ' + (pass ? 'wr-pass' : 'wr-fail');
  // Arc gauge: total arc = 176, offset 176=empty 0=full
  const arcEl = document.getElementById('h-wr-arc');
  if (arcEl) {
    const pct = Math.min(Math.max(wr, 0), 100) / 100;
    arcEl.style.strokeDashoffset = (176 * (1 - pct)).toFixed(1);
    arcEl.style.stroke = pass ? 'var(--green)' : 'var(--accent)';
  }

  // Home accounts (primary data is ready now)
  renderHomeAccounts();

  // Config
  document.getElementById('cfg-auto').innerHTML = d.auto_execute
    ? '<span style="color:var(--green)">Enabled ✅</span>'
    : '<span style="color:var(--red)">Disabled 🔕</span>';
  document.getElementById('cfg-eq').textContent  = (d.equity_fraction * 100).toFixed(0) + '% / trade';
  document.getElementById('cfg-lev').textContent = d.default_leverage + '× cross';
  document.getElementById('cfg-ts').textContent  = new Date(d.timestamp).toLocaleTimeString();
  document.getElementById('tog-auto').checked    = d.auto_execute;
  if (!document.getElementById('inp-eq').value)    document.getElementById('inp-eq').value    = (d.equity_fraction*100).toFixed(0);
  if (!document.getElementById('inp-lev').value)   document.getElementById('inp-lev').value   = d.default_leverage;
  document.getElementById('inp-risk').value  = ((d.risk_pct||0.02)*100).toFixed(1);
  document.getElementById('inp-sl').value    = ((d.auto_sl_pct||0.03)*100).toFixed(0);
  document.getElementById('inp-score').value = d.min_ai_score ?? 60;
  const _prevEl = document.getElementById('settings-risk-preview');
  if (_prevEl) _prevEl.textContent = ((d.risk_pct||0.02)*100).toFixed(1) + '% risk';

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
  renderHomeAI();
  renderPhase(d, acc.equity || 0);

  allLogs = d.logs || [];
  document.getElementById('log-count').textContent = allLogs.length;
  if (activeTab === 'logs') renderLogs(logFilter);
}

/* ── Positions ───────────────────────────────────────── */
async function fetchPositions() {
  try {
    const r = await fetch('/api/positions');
    const d = await r.json();
    _allPositions = d.positions || [];
    // rebuild account filter tabs
    const tabs = document.getElementById('pos-acct-tabs');
    const extras = d.accounts || [];
    let html = `<button class="acct-tab${_posAccFilter==='all'?' active':''}" onclick="setPosAccount('all',this)">All</button>`;
    html += `<button class="acct-tab${_posAccFilter==='primary'?' active':''}" onclick="setPosAccount('primary',this)">Primary</button>`;
    extras.forEach(a => {
      const mode = a.is_demo ? ' DEMO' : ' LIVE';
      html += `<button class="acct-tab${_posAccFilter===a.id?' active':''}" onclick="setPosAccount('${a.id}',this)">${a.name}${mode}</button>`;
    });
    tabs.innerHTML = html;
    renderPositionCards();
  } catch(e) { console.error('[pos]', e); }
}

function setPosAccount(id, el) {
  _posAccFilter = id;
  document.querySelectorAll('.acct-tab').forEach(t => t.classList.remove('active'));
  if (el) el.classList.add('active');
  renderPositionCards();
}

function _visiblePos() {
  if (_posAccFilter === 'all') return _allPositions;
  if (_posAccFilter === 'primary') return _allPositions.filter(p => p.account_id === 'primary');
  return _allPositions.filter(p => p.account_id === _posAccFilter);
}

function renderPositionCards() {
  const pos = _visiblePos();
  document.getElementById('p-count').textContent = pos.length;
  const tpnl = pos.reduce((s, p) => s + (p.pnl || 0), 0);
  const tpEl = document.getElementById('p-tpnl');
  tpEl.textContent = (tpnl >= 0 ? '+' : '') + tpnl.toFixed(2) + ' USDT';
  tpEl.style.color = tpnl >= 0 ? 'var(--green)' : 'var(--red)';
  document.getElementById('pos-close-all-wrap').style.display = pos.length > 1 ? 'block' : 'none';
  const pl = document.getElementById('pos-list');
  if (!pos.length) {
    pl.innerHTML = `<div class="empty"><svg width="40" height="40" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M5 8h14M5 8a2 2 0 1 0 0-4h14a2 2 0 1 0 0 4M5 8v10a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8m-9 4h4"/></svg>No open positions</div>`;
    return;
  }
  pl.innerHTML = pos.map((p, i) => {
    const pnl = p.pnl || 0, pct = p.pct || 0;
    const c  = pnl >= 0 ? 'var(--green)' : 'var(--red)';
    const sc = p.side === 'Buy' ? 'long' : 'short';
    const badge = p.is_demo
      ? '<span class="pos-demo-badge">DEMO</span>'
      : '<span class="pos-live-badge">LIVE</span>';
    return `<div class="pos-card ${sc}" id="poscard-${i}">
      <div class="side-bar"></div>
      <div class="pos-head">
        <div>
          <div class="pos-sym">${p.symbol} ${badge}</div>
          <div style="font-size:10px;color:var(--text3);margin-top:1px">${p.account_name || 'Primary'}</div>
        </div>
        <div style="flex-shrink:0;text-align:right">
          <div class="pos-pnl" style="color:${c}">${(pnl>=0?'+':'')+pnl.toFixed(2)}</div>
          <div class="pos-pct" style="color:${c}">${(pct>=0?'+':'')+pct.toFixed(2)}%</div>
        </div>
      </div>
      <div class="tags" style="margin-top:8px">
        <span class="tag ${sc}">${p.side === 'Buy' ? 'Long ↑' : 'Short ↓'}</span>
        <span class="tag blue">${p.leverage}×</span>
        <span class="tag">Qty ${p.size}</span>
        <span class="tag cyan">@ ${parseFloat(p.entry).toFixed(4)}</span>
      </div>
      <div class="pos-bar" style="margin-top:10px"><div class="pos-bar-fill" style="width:${Math.min(Math.abs(pct)*5,100)}%;background:${c}"></div></div>
      <div class="pos-action-row">
        <button class="pos-sl-btn" onclick="openSLTP(${i})">
          🛑 Stop Loss
          <span class="sltp-val">${p.stop_loss ? parseFloat(p.stop_loss).toFixed(4) : 'Not set'}</span>
        </button>
        <button class="pos-tp-btn" onclick="openSLTP(${i})">
          🎯 Take Profit
          <span class="sltp-val">${p.take_profit ? parseFloat(p.take_profit).toFixed(4) : 'Not set'}</span>
        </button>
      </div>
      <button class="pos-close-btn" id="pcb-${i}" onclick="closePosition(${i})">
        <svg fill="none" viewBox="0 0 24 24" stroke="currentColor"><line x1="18" y1="6" x2="6" y2="18" stroke-linecap="round"/><line x1="6" y1="6" x2="18" y2="18" stroke-linecap="round"/></svg>
        Close Position
      </button>
    </div>`;
  }).join('');
}

function renderPositions(acc) {
  // called from render() with primary account data — merge into _allPositions
  const primPos = (acc && acc.positions) || [];
  // replace primary entries with fresh data from /api/status
  _allPositions = _allPositions.filter(p => p.account_id !== 'primary');
  primPos.forEach(p => {
    _allPositions.unshift({
      account_id: 'primary', account_name: 'Primary',
      is_demo: false,
      symbol: p.symbol, side: p.side, size: String(p.size),
      leverage: String(p.leverage), entry: String(p.entry),
      pnl: p.pnl || 0, pct: p.pct || 0,
    });
  });
  renderPositionCards();
}

/* ── Close single position ───────────────────────────── */
function closePosition(idx) {
  const p = _visiblePos()[idx];
  if (!p) return;
  _closePending = {...p, _idx: idx};
  // populate sheet
  document.getElementById('cs-symbol').textContent  = p.symbol;
  const sc = p.side === 'Buy' ? 'long' : 'short';
  const sideTag = document.getElementById('cs-side-tag');
  sideTag.textContent  = p.side === 'Buy' ? 'Long ↑' : 'Short ↓';
  sideTag.className    = `tag ${sc}`;
  document.getElementById('cs-lev-tag').textContent  = `${p.leverage}×`;
  document.getElementById('cs-acct-tag').textContent = p.account_name || 'Primary';
  document.getElementById('cs-mode-badge').innerHTML = p.is_demo
    ? '<span class="pos-demo-badge">DEMO</span>'
    : '<span class="pos-live-badge">LIVE</span>';
  document.getElementById('cs-entry').textContent = parseFloat(p.entry).toFixed(4);
  document.getElementById('cs-size').textContent  = p.size;
  const pnl = p.pnl || 0, pct = p.pct || 0;
  const pnlEl = document.getElementById('cs-pnl');
  pnlEl.textContent  = (pnl >= 0 ? '+' : '') + pnl.toFixed(2) + ' USDT';
  pnlEl.style.color  = pnl >= 0 ? 'var(--green)' : 'var(--red)';
  const pctEl = document.getElementById('cs-pct');
  pctEl.textContent  = (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%';
  pctEl.style.color  = pct >= 0 ? 'var(--green)' : 'var(--red)';
  document.getElementById('cs-demo-note').style.display = p.is_demo ? 'block' : 'none';
  const btn = document.getElementById('cs-confirm-btn');
  btn.disabled = false;
  btn.innerHTML = `<svg fill="none" viewBox="0 0 24 24" stroke="currentColor" style="width:16px;height:16px;stroke-width:2"><circle cx="12" cy="12" r="10" stroke-linecap="round"/><line x1="15" y1="9" x2="9" y2="15" stroke-linecap="round"/><line x1="9" y1="9" x2="15" y2="15" stroke-linecap="round"/></svg> Close Position`;
  document.getElementById('cs-overlay').classList.add('open');
}

function cancelClose() {
  document.getElementById('cs-overlay').classList.remove('open');
  _closePending = null;
}

async function confirmClose() {
  if (!_closePending) return;
  const p   = _closePending;
  const btn = document.getElementById('cs-confirm-btn');
  btn.disabled = true;
  btn.innerHTML = '⏳ Closing…';
  try {
    const body = {symbol: p.symbol, side: p.side};
    if (p.account_id && p.account_id !== 'primary') body.account_id = p.account_id;
    const r = await fetch('/api/close', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify(body)
    });
    const d = await r.json();
    cancelClose();
    if (d.success) {
      toast(`✅ Closed ${p.symbol}${p.is_demo ? ' (Demo)' : ''}`);
      // optimistically remove from local state
      _allPositions = _allPositions.filter(x =>
        !(x.symbol === p.symbol && x.account_id === p.account_id && x.side === p.side));
      renderPositionCards();
      countdown = 4;
    } else {
      toast('❌ ' + (d.error || 'Close failed'), false);
    }
  } catch(e) { cancelClose(); toast('❌ Network error', false); }
}

/* ── SL / TP ─────────────────────────────────────────── */
let _sltpPending = null;

function openSLTP(idx) {
  const p = _visiblePos()[idx];
  if (!p) return;
  _sltpPending = {...p, _idx: idx};
  document.getElementById('sltp-symbol').textContent = p.symbol;
  const sc = p.side === 'Buy' ? 'long' : 'short';
  const st = document.getElementById('sltp-side-tag');
  st.textContent  = p.side === 'Buy' ? 'Long ↑' : 'Short ↓';
  st.className    = `tag ${sc}`;
  // Show current values
  const curSl = p.stop_loss  ? parseFloat(p.stop_loss).toFixed(4)  : 'none';
  const curTp = p.take_profit ? parseFloat(p.take_profit).toFixed(4) : 'none';
  document.getElementById('sltp-cur-sl').textContent = `current: ${curSl}`;
  document.getElementById('sltp-cur-tp').textContent = `current: ${curTp}`;
  // Pre-fill inputs with existing values
  document.getElementById('sltp-sl-inp').value = p.stop_loss  ? parseFloat(p.stop_loss)  : '';
  document.getElementById('sltp-tp-inp').value = p.take_profit ? parseFloat(p.take_profit) : '';
  const demoNote = p.is_demo ? ' (Demo)' : '';
  document.getElementById('sltp-mode-lbl').textContent = `SL / TP · ${p.symbol}${demoNote}`;
  document.getElementById('sltp-overlay').classList.add('open');
}

function clearSLTP(type) {
  document.getElementById(`sltp-${type}-inp`).value = '0';
}

function cancelSLTP() {
  document.getElementById('sltp-overlay').classList.remove('open');
  _sltpPending = null;
}

async function confirmSLTP() {
  if (!_sltpPending) return;
  const p = _sltpPending;
  const slVal = document.getElementById('sltp-sl-inp').value.trim();
  const tpVal = document.getElementById('sltp-tp-inp').value.trim();
  if (!slVal && !tpVal) { toast('Enter at least one value', false); return; }
  const btn = document.getElementById('sltp-confirm-btn');
  btn.disabled = true;
  btn.innerHTML = '⏳ Setting…';
  try {
    const body = {symbol: p.symbol, side: p.side};
    if (slVal !== '') body.stop_loss  = parseFloat(slVal);
    if (tpVal !== '') body.take_profit = parseFloat(tpVal);
    if (p.account_id && p.account_id !== 'primary') body.account_id = p.account_id;
    const r = await fetch('/api/set-sl-tp', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify(body)
    });
    const d = await r.json();
    cancelSLTP();
    btn.disabled = false;
    btn.innerHTML = '<svg fill="none" viewBox="0 0 24 24" stroke="currentColor" style="width:14px;height:14px"><path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"/></svg> Set SL / TP';
    if (d.success) {
      toast(`✅ SL/TP updated for ${p.symbol}`);
      // patch local cache so card updates immediately
      const idx = _allPositions.findIndex(x =>
        x.symbol === p.symbol && x.account_id === p.account_id && x.side === p.side);
      if (idx !== -1) {
        if (slVal !== '') _allPositions[idx].stop_loss  = slVal === '0' ? '' : slVal;
        if (tpVal !== '') _allPositions[idx].take_profit = tpVal === '0' ? '' : tpVal;
      }
      renderPositionCards();
      setTimeout(fetchPositions, 3000);
    } else {
      toast('❌ ' + (d.error || 'Failed to set SL/TP'), false);
    }
  } catch(e) {
    cancelSLTP();
    btn.disabled = false;
    toast('❌ Network error', false);
  }
}

async function closeAllVisible() {
  const pos = _visiblePos();
  if (!pos.length) return;
  if (!confirm(`Close all ${pos.length} visible position(s)? This cannot be undone.`)) return;
  toast('Closing all…');
  let ok = 0, fail = 0;
  await Promise.all(pos.map(async p => {
    try {
      const body = {symbol: p.symbol, side: p.side};
      if (p.account_id && p.account_id !== 'primary') body.account_id = p.account_id;
      const r = await fetch('/api/close', {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify(body)
      });
      const d = await r.json();
      d.success ? ok++ : fail++;
    } catch { fail++; }
  }));
  toast(fail === 0 ? `✅ All ${ok} position(s) closed` : `⚠️ ${ok} closed, ${fail} failed`, fail === 0);
  await fetchPositions();
  countdown = 4;
}

/* ── History ─────────────────────────────────────────── */
function renderHistory(hist) {
  const hl = document.getElementById('hist-list');
  if (!hist.length) {
    hl.innerHTML = `<div class="empty"><svg width="40" height="40" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" d="M9 5H7a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2h-2M9 5a2 2 0 0 0 2 2h2a2 2 0 0 0 2-2M9 5a2 2 0 0 1 2-2h2a2 2 0 0 1 2 2"/></svg>No executed trades yet</div>`;
    return;
  }
  hl.innerHTML = hist.map(t => {
    const sc   = t.side === 'Buy' ? 'var(--green)' : 'var(--red)';
    const dir  = t.side === 'Buy' ? 'LONG ↑' : (t.side === 'Sell' ? 'SHORT ↓' : t.action?.toUpperCase() || '—');
    const a    = t.analysis;
    const score = a && a.enabled ? a.score : null;
    const scolor = score != null ? _scoreColor(score) : 'var(--text3)';
    const rec   = a && a.recommendation ? a.recommendation : null;
    const recEmoji = {take:'✅', caution:'⚠️', skip:'❌'}[rec] || '';
    const tp   = t.take_profit ? `<span class="tag" style="color:var(--green);background:var(--greenbg);border-color:var(--greenb)">TP ${t.take_profit}</span>` : '';
    const sl   = t.stop_loss   ? `<span class="tag" style="color:var(--red);background:var(--redbg);border-color:var(--redb)">SL ${t.stop_loss}</span>` : '';
    const lev  = t.leverage && t.leverage !== '—' ? `<span class="tag blue">${t.leverage}×</span>` : '';
    const srcTag = t.source === 'recovery' ? ' <span style="font-size:9px;color:var(--cyan)">⏪</span>' : '';
    return `<div class="row" style="flex-direction:column;align-items:stretch;gap:6px">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:8px">
        <div class="row-left">
          <div style="display:flex;align-items:center;gap:6px">
            <span class="row-sym">${t.symbol || '?'}</span>
            <span style="font-size:11px;font-weight:800;color:${sc}">${dir}</span>
            ${srcTag}
          </div>
          <div class="row-meta">${new Date(t.timestamp).toLocaleString()}</div>
        </div>
        ${score != null ? `<div style="text-align:right;flex-shrink:0">
          <div style="font-size:22px;font-weight:900;color:${scolor};line-height:1">${score}</div>
          <div style="font-size:9px;color:var(--text3);font-weight:700">AI SCORE</div>
        </div>` : '<span class="badge b-open">EXECUTED</span>'}
      </div>
      ${(tp||sl||lev) ? `<div class="tags">${tp}${sl}${lev}</div>` : ''}
      ${a && a.summary ? `<div style="font-size:10.5px;color:var(--text2);line-height:1.4;
        background:var(--card2);border:1px solid var(--border);border-radius:8px;padding:7px 9px">
        ${recEmoji} ${a.summary}
      </div>` : ''}
    </div>`;
  }).join('');
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

/* ── Collapsible panels ──────────────────────────────── */
function toggleMarketPanel() {
  const panel = document.getElementById('market-panel');
  const btn   = document.getElementById('mkt-toggle-btn');
  const shown = panel.style.display !== 'none';
  panel.style.display = shown ? 'none' : 'grid';
  btn.textContent     = shown ? 'Show ▾' : 'Hide ▴';
  localStorage.setItem('mkt_panel', shown ? '0' : '1');
}
function toggleScoreGuide() {
  const body = document.getElementById('score-guide-body');
  const btn  = document.getElementById('guide-toggle-btn');
  const shown = body.style.display !== 'none';
  body.style.display = shown ? 'none' : 'block';
  btn.textContent    = shown ? 'Show ▾' : 'Hide ▴';
  localStorage.setItem('score_guide', shown ? '0' : '1');
}
/* ── Balance visibility toggle ───────────────────────── */
let _balVisible = localStorage.getItem('balVis') !== '0';

function toggleBalVis() {
  _balVisible = !_balVisible;
  localStorage.setItem('balVis', _balVisible ? '1' : '0');
  _applyBalVis();
}

function _applyBalVis() {
  const ids  = ['b-equity','b-avail','b-margin','b-upnl','b-pertrade','b-lev'];
  const mask = '••••••';
  const lbl  = document.getElementById('bal-vis-lbl');
  const icon = document.getElementById('bal-vis-icon');
  if (!lbl) return;
  if (_balVisible) {
    lbl.textContent = 'Hide';
    icon.innerHTML  = `<path stroke-linecap="round" stroke-linejoin="round"
      d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/>
      <circle cx="12" cy="12" r="3" stroke-linecap="round" stroke-linejoin="round"/>`;
    ids.forEach(id => {
      const el = document.getElementById(id);
      if (el && el.dataset.real) { el.textContent = el.dataset.real; delete el.dataset.real; }
    });
  } else {
    lbl.textContent = 'Show';
    icon.innerHTML  = `<path stroke-linecap="round" stroke-linejoin="round"
      d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94"/>
      <path stroke-linecap="round" stroke-linejoin="round"
        d="M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19"/>
      <line x1="1" y1="1" x2="23" y2="23" stroke-linecap="round"/>`;
    ids.forEach(id => {
      const el = document.getElementById(id);
      if (el) { el.dataset.real = el.textContent; el.textContent = mask; }
    });
  }
}

function _restorePanels() {
  _applyBalVis();
  if (localStorage.getItem('mkt_panel') === '1') {
    document.getElementById('market-panel').style.display = 'grid';
    document.getElementById('mkt-toggle-btn').textContent = 'Hide ▴';
  }
  if (localStorage.getItem('score_guide') === '1') {
    document.getElementById('score-guide-body').style.display = 'block';
    document.getElementById('guide-toggle-btn').textContent = 'Hide ▴';
  }
}

/* ── Market Panel (Gainers / Losers) ─────────────────── */
function _fmtPrice(p) {
  if (!p) return '';
  if (p >= 1000)  return '$' + p.toLocaleString('en', {maximumFractionDigits: 0});
  if (p >= 1)     return '$' + p.toFixed(3);
  if (p >= 0.01)  return '$' + p.toFixed(4);
  return '$' + p.toFixed(6);
}
function _renderMktRows(coins, type) {
  if (!coins || !coins.length) return '<div class="mkt-row" style="color:var(--text3);font-size:11px">No data</div>';
  return coins.map(c => {
    const up  = c.change >= 0;
    const cls = up ? 'up' : 'dn';
    const arrow = up ? '▲' : '▼';
    return `<div class="mkt-row">
      <div class="mkt-left">
        <span class="mkt-sym">${c.symbol}</span>
        <span class="mkt-price">${_fmtPrice(c.price)}</span>
      </div>
      <span class="mkt-chg ${cls}">${arrow}${Math.abs(c.change).toFixed(2)}%</span>
    </div>`;
  }).join('');
}
async function loadTicker() {
  try {
    const r    = await fetch('/api/market-ticker');
    const data = await r.json();
    const gEl  = document.getElementById('mkt-gainers');
    const lEl  = document.getElementById('mkt-losers');
    if (gEl && data.gainers) gEl.innerHTML = _renderMktRows(data.gainers, 'gain');
    if (lEl && data.losers)  lEl.innerHTML = _renderMktRows(data.losers,  'lose');
  } catch(e) { console.error('[market]', e); }
}

/* ── AI Analysis render ──────────────────────────────── */
function _scoreColor(score) {
  if (score >= 75) return 'var(--green)';
  if (score >= 55) return 'var(--yellow)';
  return 'var(--red)';
}

/* ── Analysis store (uid → analysis data + signal context) ── */
const _sigAnalysisStore = {};

function _renderAnalysis(a, sigContext) {
  if (!a || !a.enabled) return '';
  const score   = a.score || 50;
  const verdict = (a.verdict || 'neutral').replace(/_/g, '-');
  const rec     = a.recommendation || 'caution';
  const color   = _scoreColor(score);
  const recEmoji = {take:'✅', caution:'⚠️', skip:'❌'}[rec] || '';
  const recLabel = {take:'Take Trade', caution:'Caution', skip:'Skip'}[rec] || rec;
  const C = 2 * Math.PI * 18;
  const offset = (C * (1 - score / 100)).toFixed(1);
  const uid = 'aip' + Math.random().toString(36).slice(2,7);
  _sigAnalysisStore[uid] = {analysis: a, signal: sigContext || {}};
  return `<div class="ai-panel" id="${uid}">
    <div class="ai-panel-head" onclick="openSigAnalysis('${uid}')" style="cursor:pointer">
      <div class="ai-score-ring">
        <svg width="52" height="52" viewBox="0 0 52 52">
          <circle cx="26" cy="26" r="18" fill="none" stroke="var(--card2)" stroke-width="4"/>
          <circle cx="26" cy="26" r="18" fill="none" stroke="${color}" stroke-width="4"
            stroke-linecap="round" stroke-dasharray="${C.toFixed(1)}" stroke-dashoffset="${offset}"
            transform="rotate(-90 26 26)"/>
        </svg>
        <div class="ai-score-num" style="color:${color}">${score}</div>
      </div>
      <div style="flex:1;min-width:0">
        <div style="display:flex;align-items:center;gap:5px;flex-wrap:wrap;margin-bottom:3px">
          <span class="ai-badge s-${verdict}">${verdict.replace(/-/g,' ').toUpperCase()}</span>
          <span class="ai-rec ${rec}">${recEmoji} ${recLabel}</span>
        </div>
        <div style="font-size:10.5px;color:var(--text2);line-height:1.4;overflow:hidden;
          text-overflow:ellipsis;white-space:nowrap">${a.summary || 'Tap to view full analysis'}</div>
      </div>
      <span style="flex-shrink:0;margin-left:8px;font-size:10px;font-weight:700;
        color:var(--accent);white-space:nowrap">View ▸</span>
    </div>
  </div>`;
}

function openSigAnalysis(uid) {
  const entry = _sigAnalysisStore[uid];
  if (!entry) return;
  const {analysis: a, signal: s} = entry;

  const score   = a.score || 50;
  const verdict = (a.verdict || 'neutral').replace(/_/g, '-');
  const rec     = a.recommendation || 'caution';
  const color   = _scoreColor(score);
  const recEmoji = {take:'✅', caution:'⚠️', skip:'❌'}[rec] || '';
  const recLabel = {take:'Take Trade', caution:'Caution', skip:'Skip'}[rec] || rec;

  // Score arc
  const C = 2 * Math.PI * 28;
  const offset = (C * (1 - score / 100)).toFixed(1);
  const arc = document.getElementById('sa-score-arc');
  arc.setAttribute('stroke-dasharray', C.toFixed(1));
  arc.setAttribute('stroke-dashoffset', offset);
  arc.setAttribute('stroke', color);
  document.getElementById('sa-score-num').textContent = score;
  document.getElementById('sa-score-num').style.color = color;

  // Symbol + side
  const sym  = s.symbol || a.symbol || '—';
  const side = s.side || s.action || '';
  document.getElementById('sa-symbol').textContent = sym;
  const sideTag = document.getElementById('sa-side-tag');
  sideTag.className = 'tag ' + (side === 'Buy' ? 'long' : side === 'Sell' ? 'short' : '');
  sideTag.textContent = side === 'Buy' ? 'LONG' : side === 'Sell' ? 'SHORT' : side || '';

  // Verdict + rec
  const vEl = document.getElementById('sa-verdict-badge');
  vEl.className = `ai-badge s-${verdict}`;
  vEl.textContent = verdict.replace(/-/g,' ').toUpperCase();
  const rEl = document.getElementById('sa-rec-badge');
  rEl.className = `ai-rec ${rec}`;
  rEl.textContent = recEmoji + ' ' + recLabel;

  // Meta grid
  document.getElementById('sa-rr').textContent       = a.risk_reward || 'N/A';
  document.getElementById('sa-trend').textContent    = a.trend_alignment || '—';
  document.getElementById('sa-lev-risk').textContent = a.leverage_risk || '—';
  document.getElementById('sa-conf').textContent     = a.confidence || '—';

  // Summary
  document.getElementById('sa-summary').textContent = a.summary || 'No summary available.';

  // Factors
  const factors = (a.factors || []);
  document.getElementById('sa-factors').innerHTML = factors.length
    ? factors.map(f => `<div class="sa-factor">
        <div class="sa-factor-icon">
          <svg fill="none" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" d="M9 12l2 2 4-4"/>
            <circle cx="12" cy="12" r="9" stroke-linecap="round"/>
          </svg>
        </div>
        <div class="sa-factor-txt">${f}</div>
      </div>`).join('')
    : '<div style="font-size:12px;color:var(--text3);padding:8px 0">No factors recorded.</div>';

  // Source
  document.getElementById('sa-source').textContent = a.ai ? '🤖 Powered by Claude AI' : '📐 Rule-based analysis';

  document.getElementById('sa-overlay').classList.add('open');
}

function closeSigAnalysis() {
  document.getElementById('sa-overlay').classList.remove('open');
}

/* ── Phase Tracker ───────────────────────────────────── */
function renderPhase(d, equity) {
  const p2    = d.phase_2_equity || 750;
  const p3    = d.phase_3_equity || 1500;
  const base  = d.risk_pct  || 0.02;
  const sl    = d.auto_sl_pct || 0.03;
  const score = d.min_ai_score || 60;

  let phase, riskMult, nextTarget, phaseColor;
  if (equity >= p3) {
    phase = 3; riskMult = 2.5; nextTarget = null; phaseColor = '#22c55e';
  } else if (equity >= p2) {
    phase = 2; riskMult = 1.5; nextTarget = p3; phaseColor = 'var(--accent)';
  } else {
    phase = 1; riskMult = 1.0; nextTarget = p2; phaseColor = 'var(--cyan)';
  }
  const effectiveRisk = (base * riskMult * 100).toFixed(1);
  const scoreGate     = phase === 1 ? score : phase === 2 ? Math.max(score, 65) : Math.max(score, 70);

  // Progress bar
  let pct = 0;
  if (phase === 1)      pct = Math.min(100, (equity / p2) * 100);
  else if (phase === 2) pct = Math.min(100, ((equity - p2) / (p3 - p2)) * 100);
  else                  pct = 100;

  const badge = document.getElementById('phase-badge');
  const bar   = document.getElementById('phase-bar');
  if (badge) {
    badge.textContent    = `Phase ${phase}`;
    badge.style.background = phase === 1 ? 'rgba(6,182,212,.15)' : phase === 2 ? 'var(--accentbg)' : 'rgba(34,197,94,.15)';
    badge.style.color      = phaseColor;
  }
  if (bar) { bar.style.width = pct.toFixed(1) + '%'; bar.style.background = phaseColor; }

  const eqLbl   = document.getElementById('phase-eq-lbl');
  const nextLbl  = document.getElementById('phase-next-lbl');
  if (eqLbl)  eqLbl.textContent  = `$${equity.toFixed(0)} equity`;
  if (nextLbl) nextLbl.textContent = nextTarget
    ? `→ $${nextTarget} unlocks Phase ${phase+1}`
    : '🏆 Max phase reached';

  const ph = (id, val) => { const el = document.getElementById(id); if(el) el.textContent = val; };
  ph('ph-risk',  effectiveRisk + '%');
  ph('ph-score', scoreGate + '+');
  ph('ph-sl',    (sl * 100).toFixed(0) + '%');
}

function renderHomeAI() {
  const el = document.getElementById('home-ai-section');
  if (!el) return;
  const all      = (DATA && DATA.signals) || [];
  const analysed = all.filter(s => s.analysis && s.analysis.enabled).slice(0, 3);
  if (!analysed.length) {
    el.innerHTML = `<div style="text-align:center;padding:20px;color:var(--text3);font-size:12px">
      Signals are analysed by Claude AI as they arrive
    </div>`;
    return;
  }
  el.innerHTML = analysed.map(s => {
    const a   = s.analysis;
    const sig = s.signal || {};
    const score   = a.score || 50;
    const color   = _scoreColor(score);
    const verdict = (a.verdict || 'neutral').replace(/_/g, '-');
    const rec     = a.recommendation || 'caution';
    const recEmoji = {take:'✅', caution:'⚠️', skip:'❌'}[rec] || '';
    return `<div class="home-ai-card" onclick="goTab('signals')">
      <div style="display:flex;align-items:center;gap:10px">
        <div style="font-size:26px;font-weight:900;color:${color};min-width:40px;text-align:center;
          line-height:1">${score}<div style="font-size:9px;font-weight:700;color:var(--text3);
          margin-top:1px">/100</div></div>
        <div style="flex:1;min-width:0">
          <div style="display:flex;align-items:center;gap:6px;margin-bottom:3px;flex-wrap:wrap">
            <span style="font-size:13px;font-weight:900">${sig.symbol || '?'}</span>
            <span style="font-size:10px;font-weight:700;color:${sig.side==='Buy'?'var(--green)':'var(--red)'}">
              ${sig.side==='Buy'?'LONG':'SHORT'}</span>
            <span class="ai-badge s-${verdict}" style="font-size:9px">${verdict.replace(/-/g,' ')}</span>
          </div>
          <div style="font-size:10.5px;color:var(--text2);overflow:hidden;text-overflow:ellipsis;
            white-space:nowrap">${a.summary || ''}</div>
        </div>
        <span class="ai-rec ${rec}" style="flex-shrink:0;font-size:10px;padding:3px 8px">
          ${recEmoji} ${rec}
        </span>
      </div>
    </div>`;
  }).join('');
}

function renderMomentumAlerts() {
  const el = document.getElementById('home-momentum-alerts');
  if (!el) return;
  if (!_momentumAlerts.length) {
    el.innerHTML = `<div style="text-align:center;padding:16px;color:var(--text3);font-size:12px">No alerts — positions momentum is stable</div>`;
    return;
  }
  const urgencyIcon = u => u === 'high' ? '🔴' : u === 'medium' ? '🟡' : '🟢';
  const actionLabel = a => ({close_now:'CLOSE NOW', close_soon:'CLOSE SOON', monitor:'Monitor'}[a] || a);
  el.innerHTML = _momentumAlerts.slice(0, 5).map(a => {
    const urg = a.urgency || 'medium';
    const act = a.action  || 'monitor';
    const ts  = a.ts ? new Date(a.ts).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'}) : '';
    return `<div class="mo-alert-card urgency-${urg} action-${act}">
      <div class="mo-alert-head">
        <span class="mo-alert-sym">${urgencyIcon(urg)} ${a.symbol}</span>
        <span style="font-size:10px;color:var(--text3)">${a.side==='Buy'?'LONG':'SHORT'} · ${a.account||'Primary'}</span>
        <span class="mo-alert-action action-${act}">${actionLabel(act)}</span>
        <span style="margin-left:auto;font-size:10px;color:var(--text3)">${ts}</span>
      </div>
      <div style="font-size:11px;color:var(--text2);line-height:1.5">${a.reason || ''}</div>
      ${(a.signals||[]).length ? `<div style="margin-top:6px;display:flex;gap:5px;flex-wrap:wrap">
        ${a.signals.map(s=>`<span class="pill pill-red" style="font-size:9px">${s.slice(0,50)}</span>`).join('')}
      </div>` : ''}
      ${a.action !== 'monitor' ? `<div style="margin-top:8px;display:flex;gap:6px">
        <button class="btn btn-red btn-sm" onclick="manualCloseAlert('${a.symbol}','${a.side}')" style="font-size:10px;padding:6px 12px">
          Close ${a.symbol}
        </button>
        <span style="font-size:9px;color:var(--text3);align-self:center">You must confirm — AI never auto-closes</span>
      </div>` : ''}
    </div>`;
  }).join('');
}
async function loadMomentumAlerts() {
  try {
    const r = await fetch('/api/momentum-alerts');
    _momentumAlerts = await r.json();
    renderMomentumAlerts();
  } catch(e) {}
}
async function manualCloseAlert(symbol, side) {
  if (!confirm(`Close ${side === 'Buy' ? 'LONG' : 'SHORT'} ${symbol}?`)) return;
  const r = await fetch('/api/trade', {method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({action:'close', symbol, side})});
  const d = await r.json();
  toast(d.success ? `✅ Closed ${symbol}` : `❌ ${d.error||'Failed'}`, d.success);
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

    const aiBlock = _renderAnalysis(s.analysis, {symbol: sym, side: s.side || s.action});
    return `<div class="row" style="flex-direction:column;align-items:stretch;gap:5px">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:8px">
        <div class="row-left">
          <div><span class="row-sym">${sym}</span>${side?` <span style="font-size:11px;font-weight:800;color:${sc}">${side}</span>`:''}</div>
          <div class="row-meta">${new Date(s.timestamp).toLocaleString()}${srcTag}</div>
        </div>${badge}
      </div>
      ${contentLine}${tpsl}${errLine}${reasonLine}${aiBlock}
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
  renderHomeAccounts();
  renderAcctControls();
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
    const autoEx  = a.auto_execute !== false;
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
        <span class="pill pill-gray">${a.testnet?'Demo':'LIVE'}</span>
        ${a.note?`<span class="pill pill-gray">${a.note.slice(0,22)}</span>`:''}
      </div>
      <div class="toggle-row" style="margin-top:10px;padding:8px 0;border-top:1px solid var(--border)">
        <div class="toggle-info">
          <strong style="font-size:12px">Auto Execute</strong>
          <span style="font-size:10px">${autoEx?'Signals execute on this account':'Signals paused — review AI score first'}</span>
        </div>
        <label class="switch"><input type="checkbox" id="ae-${a.id}" ${autoEx?'checked':''} onchange="toggleAutoExec('${a.id}',this)"><span class="sw-track"></span></label>
      </div>
      <button class="btn btn-ghost btn-sm" onclick="fetchAccBal('${a.id}')" style="margin-top:8px">
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
/* ── Home account cards ────────────────────────────────── */
let _homeAccBalCache = {};

function renderHomeAccounts() {
  const el = document.getElementById('home-accounts');
  if (!el) return;
  const cards = [];

  // Primary account card (from DATA)
  if (DATA && DATA.account) {
    const acc = DATA.account;
    const eq = (acc.equity || 0).toFixed(2);
    const pnl = acc.unrealised_pnl || acc.total_pnl || 0;
    const pnlCls = pnl >= 0 ? 'pos' : 'neg';
    const pnlTxt = (pnl >= 0 ? '+' : '') + pnl.toFixed(2);
    cards.push(`<div class="acct-mini" onclick="openAccountDetail('primary')">
      <div class="acct-mini-icon">
        <svg width="18" height="18" fill="none" viewBox="0 0 24 24" stroke="var(--accent)" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4" stroke-linecap="round" stroke-linejoin="round"/></svg>
      </div>
      <div class="acct-mini-body" style="flex:1">
        <div style="display:flex;align-items:center;gap:6px;margin-bottom:2px">
          <span class="acct-mini-name">Primary</span>
          <span class="badge-live">LIVE</span>
        </div>
        <div class="acct-mini-eq">${eq} <span style="font-size:10px;font-weight:600;color:var(--text3)">USDT</span></div>
      </div>
      <div style="text-align:right">
        <div style="font-size:11px;color:var(--text3);margin-bottom:1px">PnL</div>
        <div style="font-size:13px;font-weight:800" class="${pnlCls}">${pnlTxt}</div>
      </div>
      <svg width="16" height="16" fill="none" viewBox="0 0 24 24" stroke="var(--text3)" stroke-width="2.5" style="margin-left:6px;flex-shrink:0"><polyline stroke-linecap="round" stroke-linejoin="round" points="9 18 15 12 9 6"/></svg>
    </div>`);
  }

  // Extra accounts
  _accounts.forEach(a => {
    if (!a.enabled && a.enabled !== undefined) return;
    const cached = _homeAccBalCache[a.id] || {};
    const eq = cached.equity != null ? cached.equity.toFixed(2) : '—';
    const pnl = cached.unrealised_pnl != null ? cached.unrealised_pnl : null;
    const pnlTxt = pnl != null ? ((pnl >= 0 ? '+' : '') + pnl.toFixed(2)) : '—';
    const pnlCls = pnl != null ? (pnl >= 0 ? 'pos' : 'neg') : '';
    const isDemo = a.testnet || false;
    cards.push(`<div class="acct-mini ${isDemo?'demo':''}" onclick="openAccountDetail('${a.id}')">
      <div class="acct-mini-icon" style="${isDemo?'background:var(--cyanbg)':''}">
        <svg width="18" height="18" fill="none" viewBox="0 0 24 24" stroke="${isDemo?'var(--cyan)':'var(--accent)'}" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4" stroke-linecap="round" stroke-linejoin="round"/><path stroke-linecap="round" stroke-linejoin="round" d="M23 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75"/></svg>
      </div>
      <div class="acct-mini-body" style="flex:1">
        <div style="display:flex;align-items:center;gap:6px;margin-bottom:2px">
          <span class="acct-mini-name">${a.name}</span>
          ${isDemo ? '<span class="badge-demo">DEMO</span>' : '<span class="badge-live">LIVE</span>'}
        </div>
        <div class="acct-mini-eq">${eq} <span style="font-size:10px;font-weight:600;color:var(--text3)">USDT</span></div>
      </div>
      <div style="text-align:right">
        <div style="font-size:11px;color:var(--text3);margin-bottom:1px">PnL</div>
        <div style="font-size:13px;font-weight:800" class="${pnlCls}">${pnlTxt}</div>
      </div>
      <svg width="16" height="16" fill="none" viewBox="0 0 24 24" stroke="var(--text3)" stroke-width="2.5" style="margin-left:6px;flex-shrink:0"><polyline stroke-linecap="round" stroke-linejoin="round" points="9 18 15 12 9 6"/></svg>
    </div>`);
  });

  el.innerHTML = cards.length ? cards.join('') : '<div class="acct-mini" style="opacity:.5;cursor:default"><div class="acct-mini-body"><div class="acct-mini-name" style="color:var(--text3)">No accounts configured</div></div></div>';
}

/* ── Account Controls (Settings page) ─────────────── */
let _acctCtrlTab = {};

function renderAcctControls() {
  const list = document.getElementById('acct-ctrl-list');
  if (!list) return;
  const primary = {id:'primary', name:'Primary Account', testnet:false, leverage: document.getElementById('inp-lev')?.value || 5, equity_pct: 10};
  const all = [primary, ..._accounts];
  list.innerHTML = all.map(a => {
    const isDm = a.testnet || false;
    return `<div class="acct-ctrl-card" id="accc-${a.id}">
      <div class="acct-ctrl-head" onclick="toggleAcctCtrl('${a.id}')">
        <div style="display:flex;align-items:center;gap:8px">
          <div style="width:8px;height:8px;border-radius:50%;background:${isDm?'var(--cyan)':'var(--green)'}"></div>
          <span style="font-size:13px;font-weight:800">${a.name}</span>
          <span style="font-size:9px;font-weight:700;padding:2px 7px;border-radius:5px;
            background:${isDm?'var(--cyanbg)':'var(--greenbg)'};color:${isDm?'var(--cyan)':'var(--green)'}">${isDm?'DEMO':'LIVE'}</span>
        </div>
        <svg id="accc-arr-${a.id}" width="16" height="16" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2.5" style="transition:transform .2s;color:var(--text3)"><polyline points="6 9 12 15 18 9" stroke-linecap="round" stroke-linejoin="round"/></svg>
      </div>
      <div class="acct-ctrl-body" id="accc-body-${a.id}" style="display:none">
        <div style="display:flex;gap:4px;background:var(--card2);border-radius:9px;padding:3px;margin-bottom:12px">
          <button class="accc-tab active" id="accc-tab-risk-${a.id}" onclick="acccTab('${a.id}','risk',this)">Risk</button>
          <button class="accc-tab" id="accc-tab-trade-${a.id}" onclick="acccTab('${a.id}','trade',this)">Trade</button>
        </div>
        <div id="accc-risk-${a.id}">
          <div class="inp-grid mb">
            <div class="inp-wrap"><label class="inp-lbl">Risk % / trade</label>
              <input class="inp" type="number" id="accc-r-${a.id}" min="0.5" max="10" step="0.5" placeholder="2"></div>
            <div class="inp-wrap"><label class="inp-lbl">Auto-SL %</label>
              <input class="inp" type="number" id="accc-sl-${a.id}" min="1" max="10" step="0.5" placeholder="3"></div>
          </div>
          <div class="inp-wrap mb"><label class="inp-lbl">AI Score Gate</label>
            <input class="inp" type="number" id="accc-sc-${a.id}" min="0" max="100" placeholder="60"></div>
          <button class="btn btn-primary btn-sm" onclick="saveAcccRisk('${a.id}')">Save Risk Settings</button>
        </div>
        <div id="accc-trade-${a.id}" style="display:none">
          <div class="inp-grid mb">
            <div class="inp-wrap"><label class="inp-lbl">Symbol</label>
              <input class="inp" type="text" id="accc-sym-${a.id}" placeholder="BTC" autocapitalize="characters" autocomplete="off"></div>
            <div class="inp-wrap"><label class="inp-lbl">Direction</label>
              <select class="inp" id="accc-side-${a.id}"><option value="Buy">Long ↑</option><option value="Sell">Short ↓</option></select></div>
          </div>
          <div class="inp-grid mb">
            <div class="inp-wrap"><label class="inp-lbl">Size % of equity</label>
              <input class="inp" type="number" id="accc-sz-${a.id}" min="1" max="100" placeholder="10" value="10"></div>
            <div class="inp-wrap"><label class="inp-lbl">Leverage ×</label>
              <input class="inp" type="number" id="accc-lv-${a.id}" min="1" max="100" placeholder="5" value="5"></div>
          </div>
          <div class="inp-grid mb">
            <div class="inp-wrap"><label class="inp-lbl">Stop Loss</label>
              <input class="inp" type="number" id="accc-slp-${a.id}" step="any" placeholder="optional"></div>
            <div class="inp-wrap"><label class="inp-lbl">Take Profit</label>
              <input class="inp" type="number" id="accc-tp-${a.id}" step="any" placeholder="optional"></div>
          </div>
          <div class="btn-grid">
            <button class="btn btn-green btn-sm" onclick="acccOpenTrade('${a.id}')">Open</button>
            <button class="btn btn-red btn-sm" onclick="acccCloseTrade('${a.id}')">Close</button>
          </div>
          <div id="accc-msg-${a.id}" style="font-size:11px;text-align:center;margin-top:8px;display:none"></div>
        </div>
      </div>
    </div>`;
  }).join('');
}

function toggleAcctCtrl(id) {
  const body = document.getElementById('accc-body-' + id);
  const arr  = document.getElementById('accc-arr-' + id);
  const open = body.style.display === 'none';
  body.style.display = open ? 'block' : 'none';
  if (arr) arr.style.transform = open ? 'rotate(180deg)' : '';
}

function acccTab(id, name, el) {
  document.querySelectorAll('#accc-body-' + id + ' .accc-tab').forEach(b => b.classList.remove('active'));
  el.classList.add('active');
  document.getElementById('accc-risk-' + id).style.display  = name === 'risk'  ? 'block' : 'none';
  document.getElementById('accc-trade-' + id).style.display = name === 'trade' ? 'block' : 'none';
}

async function saveAcccRisk(id) {
  const r = document.getElementById('accc-r-' + id)?.value;
  const s = document.getElementById('accc-sl-' + id)?.value;
  const c = document.getElementById('accc-sc-' + id)?.value;
  if (id === 'primary') {
    if (r) document.getElementById('inp-risk').value = r;
    if (s) document.getElementById('inp-sl').value = s;
    if (c) document.getElementById('inp-score').value = c;
    applySettings();
  } else {
    toast('Risk settings applied for ' + id, true);
  }
}

async function acccOpenTrade(id) {
  const sym  = (document.getElementById('accc-sym-' + id)?.value || '').trim().toUpperCase();
  const side = document.getElementById('accc-side-' + id)?.value;
  const sz   = parseFloat(document.getElementById('accc-sz-' + id)?.value) || 10;
  const lv   = parseInt(document.getElementById('accc-lv-' + id)?.value)  || 5;
  const sl   = document.getElementById('accc-slp-' + id)?.value;
  const tp   = document.getElementById('accc-tp-' + id)?.value;
  if (!sym) { toast('Enter symbol', false); return; }
  const body = {symbol: sym.endsWith('USDT')?sym:sym+'USDT', side, leverage:lv, equity_pct:sz};
  if (id !== 'primary') body.account_id = id;
  if (sl) body.stop_loss   = parseFloat(sl);
  if (tp) body.take_profit = parseFloat(tp);
  const msgEl = document.getElementById('accc-msg-' + id);
  msgEl.style.display = 'block'; msgEl.style.color = 'var(--text3)'; msgEl.textContent = 'Sending…';
  try {
    const r = await fetch('/api/trade', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const d = await r.json();
    msgEl.style.color = d.success ? 'var(--green)' : 'var(--red)';
    msgEl.textContent = d.success ? body.symbol + ' opened' : (d.error || 'Failed');
    if (d.success) { toast(body.symbol+' opened', true); fetchPositions(); }
  } catch(e) { msgEl.style.color='var(--red)'; msgEl.textContent='Request failed'; }
}

async function acccCloseTrade(id) {
  const sym  = (document.getElementById('accc-sym-' + id)?.value || '').trim().toUpperCase();
  const side = document.getElementById('accc-side-' + id)?.value;
  if (!sym) { toast('Enter symbol to close', false); return; }
  const body = {symbol: sym.endsWith('USDT')?sym:sym+'USDT', side};
  if (id !== 'primary') body.account_id = id;
  try {
    const r = await fetch('/api/close', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const d = await r.json();
    toast(d.success ? body.symbol+' closed' : (d.error||'Failed'), d.success);
    if (d.success) fetchPositions();
  } catch(e) { toast('Request failed', false); }
}

/* ── Account Detail Panel state ───────────────────── */
let _adAccountId   = null;
let _adBalCache    = {};
let _adPropCfg     = {};   // keyed by account id: {prop: bool, dailyLimit, ddLimit, capital}

async function openAccountDetail(id) {
  _adAccountId = id;
  const ov = document.getElementById('ad-overlay');
  ov.classList.add('open');

  // Reset to Positions tab
  adTab('positions', document.querySelector('#ad-tab-bar .ad-tab'));

  // Load saved prop cfg for this account
  const cfg = _adPropCfg[id] || {prop: false, dailyLimit: 5, ddLimit: 10, capital: 0};
  document.getElementById('ad-prop-toggle').checked = cfg.prop;
  document.getElementById('ad-dl-inp').value   = cfg.dailyLimit;
  document.getElementById('ad-dd-inp').value   = cfg.ddLimit;
  document.getElementById('ad-cap-inp').value  = cfg.capital || '';
  document.getElementById('ad-prop-meters').style.display = cfg.prop ? 'block' : 'none';
  document.getElementById('ad-prop-badge').style.display  = cfg.prop ? 'inline-block' : 'none';

  let name, isDemo, balData;
  if (id === 'primary') {
    name = 'Primary';
    isDemo = false;
    balData = DATA ? {
      equity: DATA.account.equity,
      available: DATA.account.available,
      used_margin: DATA.account.used_margin,
      unrealised_pnl: DATA.account.unrealised_pnl || DATA.account.total_pnl || 0
    } : null;
  } else {
    const acc = _accounts.find(a => a.id === id);
    name = acc ? acc.name : id;
    isDemo = acc ? (acc.testnet || false) : false;
    balData = _homeAccBalCache[id] || null;
  }

  document.getElementById('ad-name').textContent = name;
  const typeEl = document.getElementById('ad-type-badge');
  typeEl.innerHTML = isDemo
    ? '<span class="badge-demo">DEMO</span>'
    : '<span class="badge-live">LIVE</span>';

  _renderAdPositions(id);

  if (balData) {
    _renderAdBal(balData, id);
  } else {
    ['ad-equity','ad-avail','ad-margin'].forEach(i => document.getElementById(i).textContent = '…');
    document.getElementById('ad-pnl').textContent = '…';
    try {
      const r = await fetch('/api/accounts/' + id + '/balance');
      const b = await r.json();
      _homeAccBalCache[id] = b;
      _adBalCache[id] = b;
      _renderAdBal(b, id);
    } catch(e) {
      document.getElementById('ad-equity').textContent = 'Error';
    }
  }
}

async function adRefresh() {
  if (!_adAccountId) return;
  ['ad-equity','ad-avail','ad-margin'].forEach(i => document.getElementById(i).textContent = '…');
  try {
    const r = await fetch('/api/accounts/' + _adAccountId + '/balance');
    const b = await r.json();
    _homeAccBalCache[_adAccountId] = b;
    _adBalCache[_adAccountId] = b;
    _renderAdBal(b, _adAccountId);
  } catch(e) { toast('Refresh failed','err'); }
  _renderAdPositions(_adAccountId);
}

function _renderAdBal(b, id) {
  const equity = b.equity || 0;
  document.getElementById('ad-equity').textContent = equity.toFixed(2);
  document.getElementById('ad-avail').textContent  = (b.available||0).toFixed(2);
  document.getElementById('ad-margin').textContent = (b.used_margin||0).toFixed(2);
  const pnl = b.unrealised_pnl || 0;
  const pnlEl = document.getElementById('ad-pnl');
  pnlEl.textContent    = (pnl>=0?'+':'')+pnl.toFixed(2);
  pnlEl.className      = 'ad-stat-val ' + (pnl >= 0 ? 'pos' : 'neg');

  // Update prop firm meters if enabled
  const cfg = _adPropCfg[id];
  if (cfg && cfg.prop && cfg.capital > 0) {
    const dailyPct  = Math.abs(pnl) / cfg.capital * 100;
    const ddPct     = Math.max(0, (cfg.capital - equity) / cfg.capital * 100);
    _updatePropBar('dl', dailyPct, cfg.dailyLimit);
    _updatePropBar('dd', ddPct,    cfg.ddLimit);
  }
}

function _updatePropBar(key, val, limit) {
  const pct    = Math.min(val / limit * 100, 100);
  const color  = pct >= 100 ? 'var(--red)' : pct >= 80 ? 'var(--yellow)' : 'var(--green)';
  const bar    = document.getElementById('ad-' + key + '-bar');
  const valEl  = document.getElementById('ad-' + key + '-val');
  const limEl  = document.getElementById('ad-' + key + '-limit');
  if (bar)   { bar.style.width = pct + '%'; bar.style.background = color; }
  if (valEl)   valEl.textContent = val.toFixed(2) + '%';
  if (limEl)   limEl.textContent = limit + '%';
}

function _renderAdPositions(id) {
  const adPos = document.getElementById('ad-positions');
  const myPos = _allPositions.filter(p =>
    id === 'primary' ? (p.account_id === 'primary' || !p.account_id) : p.account_id === id
  );
  if (!myPos.length) {
    adPos.innerHTML = '<div style="text-align:center;padding:20px;color:var(--text3);font-size:12px">No open positions</div>';
    return;
  }
  adPos.innerHTML = myPos.map((p, i) => {
    const pnl    = p.unrealised_pnl || 0;
    const pnlCls = pnl >= 0 ? 'pos' : 'neg';
    const sl     = p.stop_loss   || p.stopLoss   || '';
    const tp     = p.take_profit || p.takeProfit || '';
    const uid    = 'adp_' + i;
    return `<div class="adp-card">
      <div class="adp-head">
        <div style="display:flex;align-items:center;gap:7px">
          <span class="adp-sym">${p.symbol.replace('USDT','')}</span>
          <span class="tag ${p.side==='Buy'?'green':'red'}" style="font-size:9px">${p.side==='Buy'?'LONG':'SHORT'}</span>
          <span class="tag blue" style="font-size:9px">${p.leverage}×</span>
        </div>
        <span class="adp-pnl ${pnlCls}">${(pnl>=0?'+':'')}${pnl.toFixed(2)}</span>
      </div>
      <div class="adp-row">
        <span style="color:var(--text3)">Entry</span><span>${parseFloat(p.entry_price||0).toFixed(4)}</span>
        <span style="color:var(--text3)">Size</span><span>${p.size}</span>
        <span style="color:var(--text3)">Notional</span><span>$${((p.size||0)*(p.mark_price||p.entry_price||0)).toFixed(2)}</span>
      </div>
      <div class="adp-sltp">
        <div>
          <label>Stop Loss</label>
          <input id="${uid}-sl" type="number" step="any" value="${sl}" placeholder="—">
        </div>
        <div>
          <label>Take Profit</label>
          <input id="${uid}-tp" type="number" step="any" value="${tp}" placeholder="—">
        </div>
      </div>
      <div class="adp-actions">
        <button class="btn btn-primary btn-sm" style="flex:1;font-size:11px"
          onclick="adSetSlTp('${p.symbol}','${p.side}','${uid}')">Update SL/TP</button>
        <button class="btn btn-red btn-sm" style="flex:1;font-size:11px"
          onclick="adClose('${p.symbol}','${p.side}')">Close</button>
      </div>
    </div>`;
  }).join('');
}

function adTab(name, el) {
  document.querySelectorAll('.ad-tab').forEach(b => b.classList.remove('active'));
  if (el) el.classList.add('active');
  ['positions','trade','risk'].forEach(t => {
    document.getElementById('ad-tab-' + t).style.display = t === name ? 'block' : 'none';
  });
}

async function adSetSlTp(symbol, side, uid) {
  const sl = document.getElementById(uid + '-sl').value;
  const tp = document.getElementById(uid + '-tp').value;
  if (!sl && !tp) { toast('Enter SL or TP','err'); return; }
  const body = {symbol, side, account_id: _adAccountId};
  if (sl) body.stop_loss   = parseFloat(sl);
  if (tp) body.take_profit = parseFloat(tp);
  try {
    const r = await fetch('/api/set-sl-tp', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
    const d = await r.json();
    toast(d.success ? 'SL/TP updated' : (d.error || 'Failed'), d.success);
    if (d.success) adRefresh();
  } catch(e) { toast('Request failed','err'); }
}

async function adClose(symbol, side) {
  if (!confirm(`Close ${symbol} ${side} on this account?`)) return;
  try {
    const r = await fetch('/api/close', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({symbol, side, account_id: _adAccountId})
    });
    const d = await r.json();
    toast(d.success ? `${symbol} closed` : (d.error || 'Failed'), d.success);
    if (d.success) { await fetchPositions(); _renderAdPositions(_adAccountId); }
  } catch(e) { toast('Request failed','err'); }
}

async function adCloseAll() {
  if (!confirm('Close ALL positions on this account?')) return;
  try {
    const r = await fetch('/api/close-all', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({account_id: _adAccountId})
    });
    const d = await r.json();
    toast(d.message || (d.success ? 'All closed' : 'Failed'), d.success !== false);
    if (d.success !== false) { await fetchPositions(); _renderAdPositions(_adAccountId); }
  } catch(e) { toast('Request failed','err'); }
}

async function adOpenTrade() {
  const sym     = (document.getElementById('ad-sym').value || '').trim().toUpperCase();
  const side    = document.getElementById('ad-side').value;
  const sizePct = parseFloat(document.getElementById('ad-size-pct').value) || 10;
  const lev     = parseInt(document.getElementById('ad-lev').value) || 5;
  const sl      = document.getElementById('ad-sl').value;
  const tp      = document.getElementById('ad-tp').value;
  if (!sym) { toast('Enter a symbol','err'); return; }
  const body = {
    symbol: sym.endsWith('USDT') ? sym : sym + 'USDT',
    side, leverage: lev, equity_pct: sizePct, account_id: _adAccountId
  };
  if (sl) body.stop_loss   = parseFloat(sl);
  if (tp) body.take_profit = parseFloat(tp);
  const msgEl = document.getElementById('ad-trade-msg');
  msgEl.style.display = 'block';
  msgEl.style.color = 'var(--text3)';
  msgEl.textContent = 'Sending order…';
  try {
    const r = await fetch('/api/trade', {
      method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)
    });
    const d = await r.json();
    if (d.success) {
      msgEl.style.color = 'var(--green)';
      msgEl.textContent = `${body.symbol} ${side} opened`;
      toast(`${body.symbol} opened`, true);
      adClearTradeForm();
      setTimeout(() => { fetchPositions(); _renderAdPositions(_adAccountId); }, 1500);
    } else {
      msgEl.style.color = 'var(--red)';
      msgEl.textContent = d.error || 'Order failed';
    }
  } catch(e) {
    msgEl.style.color = 'var(--red)';
    msgEl.textContent = 'Request failed';
  }
}

function adClearTradeForm() {
  ['ad-sym','ad-sl','ad-tp'].forEach(i => document.getElementById(i).value = '');
  document.getElementById('ad-size-pct').value = '10';
  document.getElementById('ad-lev').value      = '5';
  document.getElementById('ad-trade-msg').style.display = 'none';
  document.getElementById('ad-trade-preview').style.display = 'none';
}

function adTogglePropMode(el) {
  const id  = _adAccountId;
  const cfg = _adPropCfg[id] || {dailyLimit:5, ddLimit:10, capital:0};
  cfg.prop  = el.checked;
  _adPropCfg[id] = cfg;
  document.getElementById('ad-prop-meters').style.display = el.checked ? 'block' : 'none';
  document.getElementById('ad-prop-badge').style.display  = el.checked ? 'inline-block' : 'none';
  try { localStorage.setItem('adPropCfg', JSON.stringify(_adPropCfg)); } catch(_) {}
}

/* ── Risk Strategy Detail Sheet ──────────────────────── */
function openRiskDetail() {
  const risk  = parseFloat(document.getElementById('inp-risk').value)  || 2;
  const sl    = parseFloat(document.getElementById('inp-sl').value)    || 3;
  const score = parseInt(document.getElementById('inp-score').value)   || 60;
  const p2    = DATA ? (DATA.phase_2_equity || 750)  : 750;
  const p3    = DATA ? (DATA.phase_3_equity || 1500) : 1500;

  // Top stats
  document.getElementById('rd-risk').textContent  = risk + '%';
  document.getElementById('rd-sl').textContent    = sl + '%';
  document.getElementById('rd-score').textContent = score || 'Off';

  // Prefill edit inputs
  document.getElementById('rd-inp-risk').value  = risk;
  document.getElementById('rd-inp-sl').value    = sl;
  document.getElementById('rd-inp-score').value = score;

  // Phase thresholds
  document.getElementById('rd-p2thresh').textContent  = '$' + p2;
  document.getElementById('rd-p2thresh2').textContent = '$' + p2;
  document.getElementById('rd-p3thresh').textContent  = '$' + p3;
  document.getElementById('rd-p3thresh2').textContent = '$' + p3;

  // Phase risk values
  document.getElementById('rd-p1risk').textContent = risk + '%';
  document.getElementById('rd-p2risk').textContent = (risk * 1.5).toFixed(1) + '%';
  document.getElementById('rd-p3risk').textContent = (risk * 2.5).toFixed(1) + '%';

  // Active phase
  const equity = DATA ? (DATA.account?.equity || 0) : 0;
  const phase  = equity >= p3 ? 3 : equity >= p2 ? 2 : 1;
  ['1','2','3'].forEach(p => {
    const dot = document.getElementById('rd-p' + p + '-active');
    if (dot) dot.style.display = p == phase ? 'block' : 'none';
  });
  const phaseColors = {1:'var(--cyan)', 2:'var(--accent)', 3:'var(--green)'};
  const badge = document.getElementById('risk-phase-badge');
  badge.textContent  = 'Phase ' + phase;
  badge.style.color  = phaseColors[phase];

  document.getElementById('risk-overlay').classList.add('open');
}

function closeRiskDetail() {
  document.getElementById('risk-overlay').classList.remove('open');
}

/* ── Phase Detail Sheet ───────────────────────────────── */
function openPhaseDetail(n) {
  const risk  = parseFloat(document.getElementById('inp-risk').value)  || 2;
  const sl    = parseFloat(document.getElementById('inp-sl').value)    || 3;
  const p2    = DATA ? (DATA.phase_2_equity || 750)  : 750;
  const p3    = DATA ? (DATA.phase_3_equity || 1500) : 1500;
  const eq    = DATA ? (DATA.account?.equity || 0) : 0;

  const phases = {
    1: {
      title:'Seed', subtitle:'Build your base — survive and grow.',
      color:'linear-gradient(135deg,#0A84FF,#0055CC)',
      multLabel:'1×', mult:1.0,
      rangeStart:0, rangeEnd:p2,
      color_hex:'#5AC8FA',
      rules:[
        {icon:'🎯', text:'Only take AI scores ≥ 60. Be selective — one bad trade is harder to recover from at small size.'},
        {icon:'📏', text:'Every position risks exactly ' + risk + '% of your account. Never override sizing.'},
        {icon:'🛡️', text:'If no SL is in the signal, the bot sets auto-SL at ' + sl + '% from entry. Never trade without a stop.'},
        {icon:'🔄', text:'Aim for 2:1 R:R minimum. Pass on anything below 1.5:1.'},
        {icon:'📵', text:'No revenge trading. If you hit 3 losses in a day, stop and review.'},
        {icon:'🏁', text:'Goal: reach $' + p2 + ' consistently. Once you cross it you unlock Phase 2 risk.'},
      ]
    },
    2: {
      title:'Growth', subtitle:'Compound aggressively but stay disciplined.',
      color:'linear-gradient(135deg,#007AFF,#5E5CE6)',
      multLabel:'1.5×', mult:1.5,
      rangeStart:p2, rangeEnd:p3,
      color_hex:'#007AFF',
      rules:[
        {icon:'📈', text:'Risk increases to ' + (risk*1.5).toFixed(1) + '% per trade — 1.5× base. Position sizes are now noticeably larger.'},
        {icon:'🎯', text:'Raise your score gate to 65+. More capital on the line means you can afford to be pickier.'},
        {icon:'⚡', text:'Start taking 2–3 signals per day instead of 1. The bot handles sizing automatically.'},
        {icon:'🧠', text:'Watch the momentum alerts. At this phase a reversal can cost real money, act on close_now alerts.'},
        {icon:'🔒', text:'Hard rule: never lose more than 6% in a single day. If you do, stop trading for 24 hours.'},
        {icon:'🏁', text:'Goal: reach $' + p3 + ' and stay above it for 5+ consecutive days before Phase 3.'},
      ]
    },
    3: {
      title:'Scale', subtitle:'Maximum compounding — protect what you\'ve built.',
      color:'linear-gradient(135deg,#34C759,#30A048)',
      multLabel:'2.5×', mult:2.5,
      rangeStart:p3, rangeEnd:null,
      color_hex:'#34C759',
      rules:[
        {icon:'💰', text:'Risk is ' + (risk*2.5).toFixed(1) + '% per trade — 2.5× base. Each trade now moves real money. Size is earned, not assumed.'},
        {icon:'🎯', text:'Minimum AI score gate: 70. You are trading at full power — only elite setups.'},
        {icon:'🛡️', text:'Drawdown protection: if equity drops 10% below its Phase 3 peak, cut risk back to Phase 2 levels until recovered.'},
        {icon:'🔁', text:'Diversify: spread signals across different assets. Avoid 3+ open positions in correlated coins.'},
        {icon:'📊', text:'Weekly review: check win rate, average R:R, and total PnL. Adjust score gate if win rate drops below 55%.'},
        {icon:'🏆', text:'This is the compounding phase. Let the bot work. Do not manually interfere with positions.'},
      ]
    }
  };

  const ph    = phases[n];
  const effRisk = risk * ph.mult;
  const riskAmt = eq * effRisk / 100;
  const slDist  = sl / 100;
  const notional= slDist > 0 ? riskAmt / slDist : 0;
  const margin  = notional / 5;
  const currPhase = eq >= p3 ? 3 : eq >= p2 ? 2 : 1;

  // Banner
  document.getElementById('pd-banner').style.background = ph.color;
  document.getElementById('pd-phase-num').textContent = 'Phase ' + n;
  document.getElementById('pd-title').textContent = ph.title;
  document.getElementById('pd-subtitle').textContent = ph.subtitle;
  document.getElementById('pd-active-pill').style.display = currPhase === n ? 'block' : 'none';

  // Progress bar
  const rangeStart = ph.rangeStart;
  const rangeEnd   = ph.rangeEnd;
  let pct = 0, posLabel = '';
  if (rangeEnd) {
    pct = Math.min(100, Math.max(0, (eq - rangeStart) / (rangeEnd - rangeStart) * 100));
    posLabel = eq > rangeStart
      ? `$${eq.toFixed(0)} — ${pct.toFixed(0)}% through Phase ${n}`
      : `Need $${(rangeStart - eq).toFixed(0)} more to enter Phase ${n}`;
  } else {
    pct = eq >= rangeStart ? 100 : 0;
    posLabel = eq >= rangeStart ? `✓ Phase ${n} unlocked` : `Need $${(rangeStart-eq).toFixed(0)} more`;
  }
  document.getElementById('pd-range-start').textContent = '$' + rangeStart;
  document.getElementById('pd-range-end').textContent   = rangeEnd ? '$' + rangeEnd : '∞';
  document.getElementById('pd-range-label').textContent = 'Phase ' + n + ' Range';
  document.getElementById('pd-progress-bar').style.width      = pct + '%';
  document.getElementById('pd-progress-bar').style.background = ph.color_hex;
  document.getElementById('pd-equity-pos').textContent = posLabel;

  // Stats
  document.getElementById('pd-risk').textContent   = effRisk.toFixed(1) + '%';
  document.getElementById('pd-risk').style.color   = ph.color_hex;
  document.getElementById('pd-mult').textContent   = ph.multLabel;
  document.getElementById('pd-dollar').textContent = eq > 0 ? '$' + riskAmt.toFixed(1) : '—';

  // Example
  document.getElementById('pd-ex-equity').textContent   = '$' + eq.toFixed(2);
  document.getElementById('pd-ex-risk-amt').textContent = '$' + riskAmt.toFixed(2);
  document.getElementById('pd-ex-sl').textContent       = sl + '%';
  document.getElementById('pd-ex-notional').textContent = '$' + notional.toFixed(2);
  document.getElementById('pd-ex-margin').textContent   = '$' + margin.toFixed(2);

  // Rules
  document.getElementById('pd-rules').innerHTML = ph.rules.map(r => `
    <div style="display:flex;align-items:flex-start;gap:10px;background:var(--card2);
      border:1px solid var(--border);border-radius:10px;padding:10px 12px">
      <span style="font-size:16px;flex-shrink:0;line-height:1.4">${r.icon}</span>
      <span style="font-size:12px;color:var(--text2);line-height:1.6">${r.text}</span>
    </div>`).join('');

  document.getElementById('phase-overlay').classList.add('open');
}

function closePhaseDetail() {
  document.getElementById('phase-overlay').classList.remove('open');
}

async function rdApply() {
  const risk  = document.getElementById('rd-inp-risk').value;
  const sl    = document.getElementById('rd-inp-sl').value;
  const score = document.getElementById('rd-inp-score').value;
  // Sync hidden inputs then call applySettings
  document.getElementById('inp-risk').value  = risk;
  document.getElementById('inp-sl').value    = sl;
  document.getElementById('inp-score').value = score;
  await applySettings();
  // Update preview label
  if (risk) document.getElementById('settings-risk-preview').textContent = risk + '% risk';
  closeRiskDetail();
  toast('Risk settings saved', true);
}

function adSavePropSettings() {
  const id = _adAccountId;
  _adPropCfg[id] = {
    prop:       document.getElementById('ad-prop-toggle').checked,
    dailyLimit: parseFloat(document.getElementById('ad-dl-inp').value) || 5,
    ddLimit:    parseFloat(document.getElementById('ad-dd-inp').value) || 10,
    capital:    parseFloat(document.getElementById('ad-cap-inp').value) || 0,
  };
  try { localStorage.setItem('adPropCfg', JSON.stringify(_adPropCfg)); } catch(_) {}
  toast('Prop settings saved', true);
  document.getElementById('ad-dl-limit').textContent = _adPropCfg[id].dailyLimit + '%';
  document.getElementById('ad-dd-limit').textContent = _adPropCfg[id].ddLimit + '%';
  // Rerender meters with latest balance
  const b = _adBalCache[id] || _homeAccBalCache[id];
  if (b) _renderAdBal(b, id);
}

function _adRestorePropCfg() {
  try {
    const s = localStorage.getItem('adPropCfg');
    if (s) _adPropCfg = JSON.parse(s);
  } catch(_) {}
}

function closeAccountDetail() {
  document.getElementById('ad-overlay').classList.remove('open');
  _adAccountId = null;
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
async function toggleAutoExec(id, el) {
  const r = await fetch('/api/accounts/' + id + '/auto-execute', {method:'POST'});
  const d = await r.json();
  toast(d.auto_execute ? '✅ Auto Execute ON for this account' : '⏸ Auto Execute OFF — signals paused', d.auto_execute);
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
  const lv    = parseFloat(document.getElementById('inp-lev').value);
  const risk  = parseFloat(document.getElementById('inp-risk').value) / 100;
  const sl    = parseFloat(document.getElementById('inp-sl').value) / 100;
  const score = parseInt(document.getElementById('inp-score').value);
  if (!lv || lv < 1) { toast('Invalid leverage', false); return; }
  const body = {default_leverage: lv};
  if (risk > 0 && risk <= 0.20)  body.risk_pct      = risk;
  if (sl > 0 && sl <= 0.20)      body.auto_sl_pct   = sl;
  if (!isNaN(score) && score >= 0 && score <= 100) body.min_ai_score = score;
  await fetch('/api/settings', {method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify(body)});
  toast(`✅ ${lv}× lev · ${((risk||0.02)*100).toFixed(1)}% risk · score≥${score||60} applied`);
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
  toast(`Closing ${sym}…`);
  const r = await fetch('/api/close', {method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({symbol: sym})});
  const d = await r.json();
  toast(d.success ? `✅ Closed ${sym}` : '❌ ' + (d.error||'Failed'), d.success);
  if (d.success) { countdown = 3; fetchPositions(); }
}
async function closeAll() {
  if (!confirm('Close ALL open positions on primary account?')) return;
  toast('Closing all…');
  const r = await fetch('/api/close-all', {method:'POST'});
  const d = await r.json();
  toast(d.success ? '✅ All positions closed' : '❌ Some failed', d.success);
  countdown = 3; fetchPositions();
}
function refreshNow() { countdown = 12; _sigOffset = 0; fetchData(); toast('Refreshed ✅'); }

/* ── Ticker ──────────────────────────────────────────── */
function tick() {
  countdown--;
  const el = document.getElementById('cd');
  if (el) el.textContent = `Auto-refresh in ${countdown}s · ${new Date().toLocaleTimeString()}`;
  if (countdown <= 0) { countdown = 12; fetchData(); }
}

/* ── Fear & Greed Index ───────────────────────────────── */
async function fngRefresh(btn) {
  const icon = document.getElementById('fng-refresh-icon');
  if (icon) icon.style.animation = 'fng-spin .7s linear infinite';
  if (btn)  btn.disabled = true;
  // bypass client cache — add ts param so server still caches on its side
  try {
    const r = await fetch('/api/fear-greed?r=' + Date.now());
    if (r.ok) {
      const d = await r.json();
      _applyFng(d);
    }
  } catch(e) {}
  if (icon) icon.style.animation = '';
  if (btn)  btn.disabled = false;
}

function _applyFng(d) {
  if (!d || d.value == null) return;
  const v   = d.value;
  const lbl = d.classification;
  const col = v <= 24 ? '#FF3B30'
            : v <= 49 ? '#FF9500'
            : v <= 54 ? '#FFD60A'
            : v <= 74 ? '#34C759'
            :            '#00C7BE';

  const arcLen = 113.1;
  const arc = document.getElementById('fng-arc');
  if (arc) { arc.style.strokeDashoffset = arcLen - (v/100)*arcLen; arc.style.stroke = col; }

  const needle = document.getElementById('fng-needle');
  if (needle) needle.style.transform = `rotate(${-90 + (v/100)*180}deg)`;

  const marker = document.getElementById('fng-marker');
  if (marker) marker.style.left = v + '%';

  const valEl = document.getElementById('fng-val');
  if (valEl) { valEl.textContent = v; valEl.style.color = col; }

  const lblEl = document.getElementById('fng-label');
  if (lblEl) { lblEl.textContent = lbl; lblEl.style.color = col; }

  const upEl = document.getElementById('fng-updated');
  if (upEl && d.updated) {
    const mins = Math.round((Date.now()/1000 - d.updated) / 60);
    upEl.textContent = mins < 2 ? 'Just updated' : `${mins}m ago`;
  }
}

async function fetchFearGreed() {
  try {
    const r = await fetch('/api/fear-greed');
    if (!r.ok) return;
    _applyFng(await r.json());
  } catch(e) {}
}

/* ── Boot ────────────────────────────────────────────── */
_restorePanels();
_adRestorePropCfg();
fetchData();
fetchPositions();
loadAccounts();
loadTicker();
loadMomentumAlerts();
fetchFearGreed();
connectSSE();
setInterval(tick, 1000);
setInterval(fetchPositions, 15000);
setInterval(loadTicker, 60000);
setInterval(loadMomentumAlerts, 300000);
setInterval(fetchFearGreed, 300000);
</script>

<!-- ── Risk Strategy Detail Sheet ── -->
<div class="sa-overlay" id="risk-overlay" onclick="if(event.target===this)closeRiskDetail()">
  <div class="sa-sheet" style="max-height:94dvh">
    <div class="cs-handle"></div>

    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:18px">
      <div>
        <div style="font-size:20px;font-weight:900">Risk Strategy</div>
        <div style="font-size:11px;color:var(--text3);margin-top:2px">Monte Carlo position sizing</div>
      </div>
      <div id="risk-phase-badge" style="padding:6px 14px;border-radius:20px;font-size:12px;font-weight:800;
        background:var(--accentbg);color:var(--accent);border:1px solid var(--accentbrd)">Phase 1</div>
    </div>

    <!-- Live settings display -->
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:16px">
      <div class="sa-meta-box" style="text-align:center">
        <div class="sa-meta-lbl">Risk / Trade</div>
        <div class="sa-meta-val" id="rd-risk" style="color:var(--accent)">2%</div>
      </div>
      <div class="sa-meta-box" style="text-align:center">
        <div class="sa-meta-lbl">Auto SL</div>
        <div class="sa-meta-val" id="rd-sl" style="color:var(--red)">3%</div>
      </div>
      <div class="sa-meta-box" style="text-align:center">
        <div class="sa-meta-lbl">Score Gate</div>
        <div class="sa-meta-val" id="rd-score" style="color:var(--green)">60</div>
      </div>
    </div>

    <!-- How it works -->
    <div style="background:var(--card2);border:1px solid var(--border);border-radius:14px;
      padding:14px;margin-bottom:14px">
      <div style="font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.8px;
        color:var(--text3);margin-bottom:10px">How Sizing Works</div>
      <div style="font-size:12.5px;color:var(--text2);line-height:1.7">
        Position size is back-calculated from your <strong style="color:var(--text)">risk %</strong>
        and the actual SL distance — not from a fixed equity fraction.<br><br>
        <span style="color:var(--accent);font-weight:700">Formula:</span><br>
        <code style="background:var(--card);border:1px solid var(--border);border-radius:6px;
          padding:4px 8px;font-size:11px;display:block;margin:6px 0;color:var(--text)">
          notional = (equity × risk%) ÷ SL_distance
        </code>
        If your SL is 3% from entry and risk is 2%, and equity is $500:<br>
        <span style="color:var(--green);font-weight:700">$500 × 2% ÷ 3% = $333 notional</span>
        — a SL hit costs exactly $10 (2% of $500).
      </div>
    </div>

    <!-- Phase thresholds -->
    <div style="font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.8px;
      color:var(--text3);margin-bottom:8px">Risk Phases</div>
    <div style="display:flex;flex-direction:column;gap:8px;margin-bottom:16px">
      <!-- Phase 1 -->
      <div id="rd-phase1" onclick="openPhaseDetail(1)"
        class="sa-meta-box" style="display:flex;align-items:center;gap:10px;padding:14px;
        cursor:pointer;transition:all .15s;border:1px solid rgba(90,200,250,.2)">
        <div style="width:38px;height:38px;border-radius:12px;
          background:linear-gradient(135deg,rgba(90,200,250,.25),rgba(90,200,250,.1));
          display:flex;align-items:center;justify-content:center;flex-shrink:0;
          font-size:18px;font-weight:900;color:#5AC8FA">1</div>
        <div style="flex:1">
          <div style="font-size:13px;font-weight:800">Phase 1 — Seed</div>
          <div style="font-size:10px;color:var(--text3);margin-top:3px">
            $0 → <span id="rd-p2thresh">$750</span>
            &nbsp;·&nbsp; Risk: <span id="rd-p1risk" style="color:#5AC8FA;font-weight:700">2%</span>
          </div>
        </div>
        <div style="display:flex;align-items:center;gap:6px">
          <div id="rd-p1-active" style="display:none;width:8px;height:8px;border-radius:50%;background:#5AC8FA"></div>
          <svg width="14" height="14" fill="none" viewBox="0 0 24 24" stroke="var(--text3)" stroke-width="2.5">
            <polyline points="9 18 15 12 9 6" stroke-linecap="round" stroke-linejoin="round"/>
          </svg>
        </div>
      </div>
      <!-- Phase 2 -->
      <div id="rd-phase2" onclick="openPhaseDetail(2)"
        class="sa-meta-box" style="display:flex;align-items:center;gap:10px;padding:14px;
        cursor:pointer;transition:all .15s;border:1px solid rgba(0,122,255,.2)">
        <div style="width:38px;height:38px;border-radius:12px;
          background:linear-gradient(135deg,rgba(0,122,255,.25),rgba(0,122,255,.1));
          display:flex;align-items:center;justify-content:center;flex-shrink:0;
          font-size:18px;font-weight:900;color:#007AFF">2</div>
        <div style="flex:1">
          <div style="font-size:13px;font-weight:800">Phase 2 — Growth</div>
          <div style="font-size:10px;color:var(--text3);margin-top:3px">
            <span id="rd-p2thresh2">$750</span> → <span id="rd-p3thresh">$1500</span>
            &nbsp;·&nbsp; Risk: <span id="rd-p2risk" style="color:#007AFF;font-weight:700">3%</span>
          </div>
        </div>
        <div style="display:flex;align-items:center;gap:6px">
          <div id="rd-p2-active" style="display:none;width:8px;height:8px;border-radius:50%;background:#007AFF"></div>
          <svg width="14" height="14" fill="none" viewBox="0 0 24 24" stroke="var(--text3)" stroke-width="2.5">
            <polyline points="9 18 15 12 9 6" stroke-linecap="round" stroke-linejoin="round"/>
          </svg>
        </div>
      </div>
      <!-- Phase 3 -->
      <div id="rd-phase3" onclick="openPhaseDetail(3)"
        class="sa-meta-box" style="display:flex;align-items:center;gap:10px;padding:14px;
        cursor:pointer;transition:all .15s;border:1px solid rgba(52,199,89,.2)">
        <div style="width:38px;height:38px;border-radius:12px;
          background:linear-gradient(135deg,rgba(52,199,89,.25),rgba(52,199,89,.1));
          display:flex;align-items:center;justify-content:center;flex-shrink:0;
          font-size:18px;font-weight:900;color:#34C759">3</div>
        <div style="flex:1">
          <div style="font-size:13px;font-weight:800">Phase 3 — Scale</div>
          <div style="font-size:10px;color:var(--text3);margin-top:3px">
            <span id="rd-p3thresh2">$1500</span>+
            &nbsp;·&nbsp; Risk: <span id="rd-p3risk" style="color:#34C759;font-weight:700">5%</span>
          </div>
        </div>
        <div style="display:flex;align-items:center;gap:6px">
          <div id="rd-p3-active" style="display:none;width:8px;height:8px;border-radius:50%;background:#34C759"></div>
          <svg width="14" height="14" fill="none" viewBox="0 0 24 24" stroke="var(--text3)" stroke-width="2.5">
            <polyline points="9 18 15 12 9 6" stroke-linecap="round" stroke-linejoin="round"/>
          </svg>
        </div>
      </div>
    </div>

    <!-- Edit settings inline -->
    <div style="font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.8px;
      color:var(--text3);margin-bottom:8px">Adjust Settings</div>
    <div class="inp-grid mb">
      <div class="inp-wrap">
        <label class="inp-lbl">Risk % / trade</label>
        <input class="inp" type="number" id="rd-inp-risk" min="0.5" max="10" step="0.5" placeholder="2">
      </div>
      <div class="inp-wrap">
        <label class="inp-lbl">Auto-SL %</label>
        <input class="inp" type="number" id="rd-inp-sl" min="1" max="10" step="0.5" placeholder="3">
      </div>
    </div>
    <div class="inp-wrap mb">
      <label class="inp-lbl">AI Score Gate (0 = off)</label>
      <input class="inp" type="number" id="rd-inp-score" min="0" max="100" placeholder="60">
    </div>
    <div class="btn-grid">
      <button class="btn btn-primary" onclick="rdApply()">
        <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" style="width:15px;height:15px;stroke-width:2.5"><path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"/></svg>
        Apply
      </button>
      <button class="btn btn-ghost" onclick="closeRiskDetail()">Cancel</button>
    </div>
  </div>
</div>

<!-- ── Phase Detail Sheet ── -->
<div class="sa-overlay" id="phase-overlay" onclick="if(event.target===this)closePhaseDetail()">
  <div class="sa-sheet" style="max-height:94dvh">
    <div class="cs-handle"></div>
    <!-- Coloured header banner -->
    <div id="pd-banner" style="border-radius:16px;padding:18px;margin-bottom:16px;position:relative;overflow:hidden">
      <div style="position:absolute;top:-30px;right:-30px;width:120px;height:120px;
        border-radius:50%;background:rgba(255,255,255,0.08)"></div>
      <div style="font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:1px;
        opacity:.75;margin-bottom:4px" id="pd-phase-num">Phase 1</div>
      <div style="font-size:26px;font-weight:900;color:#fff;letter-spacing:-.5px" id="pd-title">Seed</div>
      <div style="font-size:12px;color:rgba(255,255,255,.75);margin-top:4px" id="pd-subtitle">Building your base</div>
      <!-- Active indicator -->
      <div id="pd-active-pill" style="display:none;position:absolute;top:14px;right:14px;
        background:rgba(255,255,255,.2);border:1px solid rgba(255,255,255,.3);
        border-radius:20px;padding:4px 10px;font-size:9px;font-weight:800;color:#fff">
        CURRENT PHASE
      </div>
    </div>

    <!-- Equity range progress -->
    <div class="sa-meta-box" style="margin-bottom:12px;padding:14px">
      <div style="display:flex;justify-content:space-between;font-size:10px;font-weight:700;
        color:var(--text3);margin-bottom:8px">
        <span id="pd-range-start">$0</span>
        <span id="pd-range-label">Equity Range</span>
        <span id="pd-range-end">$750</span>
      </div>
      <div style="height:8px;background:var(--border);border-radius:4px;overflow:hidden">
        <div id="pd-progress-bar" style="height:100%;border-radius:4px;transition:width .6s cubic-bezier(.4,0,.2,1)"></div>
      </div>
      <div style="text-align:center;margin-top:6px;font-size:11px;color:var(--text3)">
        <span id="pd-equity-pos"></span>
      </div>
    </div>

    <!-- Stats grid -->
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:12px">
      <div class="sa-meta-box" style="text-align:center;padding:12px">
        <div class="sa-meta-lbl">Effective Risk</div>
        <div class="sa-meta-val" id="pd-risk" style="font-size:20px">—</div>
      </div>
      <div class="sa-meta-box" style="text-align:center;padding:12px">
        <div class="sa-meta-lbl">Risk Multiplier</div>
        <div class="sa-meta-val" id="pd-mult" style="font-size:20px">—</div>
      </div>
      <div class="sa-meta-box" style="text-align:center;padding:12px">
        <div class="sa-meta-lbl">$ at Risk / trade</div>
        <div class="sa-meta-val" id="pd-dollar" style="font-size:20px">—</div>
      </div>
    </div>

    <!-- Example trade -->
    <div id="pd-example" style="background:var(--card2);border:1px solid var(--border);
      border-radius:14px;padding:14px;margin-bottom:12px">
      <div style="font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:.8px;
        color:var(--text3);margin-bottom:10px">Example Trade (Your Equity)</div>
      <div style="display:flex;flex-direction:column;gap:7px;font-size:12px">
        <div style="display:flex;justify-content:space-between">
          <span style="color:var(--text3)">Account equity</span>
          <span id="pd-ex-equity" style="font-weight:800">$0</span>
        </div>
        <div style="display:flex;justify-content:space-between">
          <span style="color:var(--text3)">Risk amount</span>
          <span id="pd-ex-risk-amt" style="font-weight:800;color:var(--red)">$0</span>
        </div>
        <div style="display:flex;justify-content:space-between">
          <span style="color:var(--text3)">Auto-SL distance</span>
          <span id="pd-ex-sl" style="font-weight:800">3%</span>
        </div>
        <div style="border-top:1px solid var(--border);padding-top:7px;
          display:flex;justify-content:space-between">
          <span style="color:var(--text3)">Position notional</span>
          <span id="pd-ex-notional" style="font-weight:900;color:var(--accent);font-size:14px">$0</span>
        </div>
        <div style="display:flex;justify-content:space-between">
          <span style="color:var(--text3)">Margin used (5× lev)</span>
          <span id="pd-ex-margin" style="font-weight:800">$0</span>
        </div>
      </div>
    </div>

    <!-- Rules & strategy -->
    <div style="font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:.8px;
      color:var(--text3);margin-bottom:8px">Strategy & Rules</div>
    <div id="pd-rules" style="display:flex;flex-direction:column;gap:6px;margin-bottom:16px"></div>

    <button class="btn btn-ghost btn-sm" onclick="closePhaseDetail()" style="width:100%">Close</button>
  </div>
</div>

<!-- ── Signal Analysis Full-Screen Overlay ── -->
<div class="sa-overlay" id="sa-overlay" onclick="if(event.target===this)closeSigAnalysis()">
  <div class="sa-sheet">
    <div class="cs-handle"></div>

    <!-- Hero: score ring + symbol + verdict -->
    <div class="sa-hero">
      <div class="sa-score-ring">
        <svg width="72" height="72" viewBox="0 0 72 72">
          <circle cx="36" cy="36" r="28" fill="none" stroke="var(--card2)" stroke-width="6"/>
          <circle cx="36" cy="36" r="28" fill="none" id="sa-score-arc" stroke-width="6"
            stroke-linecap="round" stroke-dasharray="175.9" stroke-dashoffset="87.9"
            transform="rotate(-90 36 36)"/>
        </svg>
        <div class="sa-score-num" id="sa-score-num">—</div>
      </div>
      <div style="flex:1;min-width:0">
        <div class="sa-symbol" id="sa-symbol">—</div>
        <div style="display:flex;align-items:center;gap:6px;margin-top:5px;flex-wrap:wrap">
          <span id="sa-side-tag"></span>
          <span class="ai-badge" id="sa-verdict-badge">—</span>
        </div>
        <div style="margin-top:6px">
          <span class="ai-rec" id="sa-rec-badge" style="font-size:12px;padding:6px 14px">—</span>
        </div>
      </div>
    </div>

    <!-- Meta grid -->
    <div class="sa-meta-grid">
      <div class="sa-meta-box">
        <div class="sa-meta-lbl">Risk : Reward</div>
        <div class="sa-meta-val" id="sa-rr">—</div>
      </div>
      <div class="sa-meta-box">
        <div class="sa-meta-lbl">Trend Alignment</div>
        <div class="sa-meta-val" id="sa-trend">—</div>
      </div>
      <div class="sa-meta-box">
        <div class="sa-meta-lbl">Leverage Risk</div>
        <div class="sa-meta-val" id="sa-lev-risk">—</div>
      </div>
      <div class="sa-meta-box">
        <div class="sa-meta-lbl">Confidence</div>
        <div class="sa-meta-val" id="sa-conf">—</div>
      </div>
    </div>

    <!-- Summary -->
    <div class="sa-section-lbl">Summary</div>
    <div class="sa-summary-box" id="sa-summary">—</div>

    <!-- Key factors -->
    <div class="sa-section-lbl">Key Factors</div>
    <div id="sa-factors">—</div>

    <!-- Source badge -->
    <div style="margin-top:14px;text-align:center">
      <span id="sa-source" style="font-size:10px;color:var(--text3);font-weight:600"></span>
    </div>

    <button class="btn btn-ghost btn-sm" onclick="closeSigAnalysis()" style="margin-top:16px;width:100%">Close</button>
  </div>
</div>
</body>
</html>"""


if __name__ == "__main__":
    port = int(os.environ.get("PORT", os.environ.get("DASHBOARD_PORT", 8080)))
    print(f"\n  Prolific -> http://0.0.0.0:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
