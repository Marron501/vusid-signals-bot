"""
CopyBot re-entry watcher.

When a Discord signal is filtered out by the AI-score gate, it is registered
here and re-scored roughly every 5 minutes. If its AI score climbs back to the
gate within COPYBOT_WATCH_HOURS, an "entry window open" alert is POSTed to the
CopyBot Discord webhook so you can take the trade manually.

SAFETY: this module NEVER executes a trade — it only sends notifications.
The feature is fully disabled unless COPYBOT_WEBHOOK_URL is configured.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone

import config
from paths import DATA_DIR

logger = logging.getLogger(__name__)

WATCH_FILE      = DATA_DIR / "copybot_watch.json"
RECHECK_SECONDS = 300          # re-score cadence (~5 min)
_lock           = threading.RLock()


# ── persistence ───────────────────────────────────────────────────────────────
def _load() -> list[dict]:
    with _lock:
        if not WATCH_FILE.exists():
            return []
        try:
            return json.loads(WATCH_FILE.read_text())
        except Exception:
            return []


def _save(items: list[dict]) -> None:
    with _lock:
        try:
            WATCH_FILE.write_text(json.dumps(items, default=str))
        except Exception as e:
            logger.error(f"[copybot] save failed: {e}")


# ── public API (called from the signal listener at the filter point) ──────────
def add_filtered_signal(signal: dict, score: int, gate: int, phase_label: str) -> None:
    """Register an AI-score-filtered signal to be watched for a recovered entry."""
    if not config.COPYBOT_ALERTS:
        return  # feature disabled
    try:
        sym  = signal.get("symbol", "")
        side = signal.get("side", "")
        key  = f"{sym}_{side}"
        with _lock:
            items = [i for i in _load() if i.get("key") != key]  # de-dupe same symbol+side
            items.append({
                "key":        key,
                "signal":     signal,
                "symbol":     sym,
                "side":       side,
                "orig_score": int(score),
                "last_score": int(score),
                "gate":       int(gate),
                "phase":      phase_label,
                "added_ts":   time.time(),
                "added_iso":  datetime.now(timezone.utc).isoformat(),
            })
            _save(items)
        logger.info(
            f"[copybot] watching {key} (score {score} < gate {gate}) "
            f"for {config.COPYBOT_WATCH_HOURS}h"
        )
    except Exception as e:
        logger.error(f"[copybot] add_filtered_signal error: {e}")


# ── Discord webhook ───────────────────────────────────────────────────────────
def _post_webhook(title: str, description: str, color: int, fields: list[dict]) -> bool:
    url = config.COPYBOT_WEBHOOK_URL
    if not url:
        return False
    try:
        import requests
        payload = {
            "username": "Prolific CopyBot",
            "embeds": [{
                "title":       title,
                "description": description,
                "color":       color,
                "fields":      fields,
                "footer":      {"text": "Prolific Signals — notification only, not auto-traded"},
                "timestamp":   datetime.now(timezone.utc).isoformat(),
            }],
        }
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code in (200, 204):
            return True
        logger.warning(f"[copybot] webhook HTTP {r.status_code}: {r.text[:120]}")
        return False
    except Exception as e:
        logger.error(f"[copybot] webhook post failed: {e}")
        return False


# ── delivery ──────────────────────────────────────────────────────────────────
def _notify_entry_window(it: dict, score: int, res: dict) -> bool:
    """
    Deliver an 'entry window open' alert. Default channel is the owner DM
    (the CopyBot APP DM thread). If COPYBOT_WEBHOOK_URL is set, a channel
    webhook is used instead.
    """
    sig     = it["signal"]
    dir_lbl = "LONG" if it["side"] == "Buy" else "SHORT"

    if config.COPYBOT_WEBHOOK_URL:
        return _post_webhook(
            title=f"✅ Entry Window Open — {it['symbol']} {dir_lbl}",
            description=("A signal previously filtered by AI score has recovered to the "
                         "entry threshold. Consider taking the trade."),
            color=0x34D399,
            fields=[
                {"name": "Score",       "value": f"{it['orig_score']} → **{score}** (gate {int(it.get('gate',60))})", "inline": True},
                {"name": "Direction",   "value": dir_lbl, "inline": True},
                {"name": "Phase",       "value": str(it.get("phase", "—")), "inline": True},
                {"name": "Take Profit", "value": str(sig.get("take_profit") or "—"), "inline": True},
                {"name": "Stop Loss",   "value": str(sig.get("stop_loss") or "—"), "inline": True},
                {"name": "Verdict",     "value": str(res.get("verdict", "—")), "inline": True},
            ],
        )

    # Default: owner DM (thread-safe bridge into the running Discord client)
    try:
        from signal_listener import send_owner_dm
        msg = (
            f"✅ **Entry Window Open — CopyBot**\n"
            f"────────────────────\n"
            f"**{it['symbol']}** {dir_lbl} · {it.get('phase','—')}\n"
            f"AI Score: `{it['orig_score']} → {score}` (gate {int(it.get('gate',60))})\n"
            f"Take Profit: `{sig.get('take_profit') or '—'}`  ·  Stop Loss: `{sig.get('stop_loss') or '—'}`\n"
            f"Verdict: `{res.get('verdict','—')}`\n"
            f"_A signal earlier filtered by AI score has recovered — consider taking it. "
            f"Notification only; not auto-traded._"
        )
        return bool(send_owner_dm(msg))
    except Exception as e:
        logger.error(f"[copybot] DM delivery failed: {e}")
        return False


# ── monitor loop ──────────────────────────────────────────────────────────────
def _tick() -> None:
    items = _load()
    if not items:
        return

    from signal_analyzer import analyze_signal
    try:
        from signal_listener import get_win_rate
        win_rate = get_win_rate()[0]
    except Exception:
        win_rate = 0.0

    now        = time.time()
    watch_secs = config.COPYBOT_WATCH_HOURS * 3600
    keep: list[dict] = []
    changed = False

    for it in items:
        # 1) expire stale watches
        if now - it.get("added_ts", now) > watch_secs:
            logger.info(f"[copybot] {it['key']} expired after {config.COPYBOT_WATCH_HOURS}h")
            changed = True
            continue

        # 2) re-score against fresh market data
        try:
            res   = analyze_signal(dict(it["signal"]), win_rate)
            score = int(res.get("score", 0))
        except Exception as e:
            logger.debug(f"[copybot] rescore {it['key']} err: {e}")
            keep.append(it)          # keep for next tick
            continue

        gate = int(it.get("gate", 60))
        if score != it.get("last_score"):
            it["last_score"] = score
            changed = True

        if score >= gate:
            # 3) entry window open → notify, then drop from the watchlist
            ok = _notify_entry_window(it, score, res)
            logger.warning(
                f"[copybot] ENTRY WINDOW {it['key']} score {it['orig_score']}→{score} (sent={ok})"
            )
            changed = True
            # not appended to keep → removed from watchlist
        else:
            keep.append(it)

    if changed:
        _save(keep)


def _watch_loop() -> None:
    time.sleep(150)  # give the bot time to start
    while True:
        try:
            if config.COPYBOT_ALERTS:
                _tick()
        except Exception as e:
            logger.error(f"[copybot] loop error: {e}")
        time.sleep(RECHECK_SECONDS)


_started = False
def start_watcher() -> None:
    """Start the background watcher thread (idempotent)."""
    global _started
    if _started:
        return
    _started = True
    threading.Thread(target=_watch_loop, daemon=True, name="copybot-watch").start()
    if config.COPYBOT_ALERTS:
        dest = "channel webhook" if config.COPYBOT_WEBHOOK_URL else "owner DM"
        logger.info(f"[copybot] watcher started — window {config.COPYBOT_WATCH_HOURS}h, via {dest}")
    else:
        logger.info("[copybot] watcher started (disabled — COPYBOT_ALERTS=false)")
