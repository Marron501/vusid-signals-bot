"""
Trade Optimizer.
Tracks performance, learns from past trades, and suggests optimal
sizing and leverage to maximize profit per trade.
"""
import json
import logging
from datetime import datetime
from decimal import Decimal
from pathlib import Path

logger = logging.getLogger(__name__)

HISTORY_FILE = Path(__file__).parent / "trade_history.json"
STATS_FILE = Path(__file__).parent / "trade_stats.json"


class TradeOptimizer:
    def __init__(self, executor=None):
        self.executor = executor
        self.history: list[dict] = []
        self.stats: dict = {
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "total_pnl": "0",
            "best_trade": None,
            "worst_trade": None,
            "avg_win": "0",
            "avg_loss": "0",
            "best_leverage": {},
            "symbol_performance": {},
        }
        self._load_history()
        self._load_stats()

    def _load_history(self):
        if HISTORY_FILE.exists():
            try:
                with open(HISTORY_FILE) as f:
                    self.history = json.load(f)
            except Exception:
                self.history = []

    def _save_history(self):
        with open(HISTORY_FILE, "w") as f:
            json.dump(self.history, f, indent=2, default=str)

    def _load_stats(self):
        if STATS_FILE.exists():
            try:
                with open(STATS_FILE) as f:
                    self.stats = json.load(f)
            except Exception:
                pass

    def _save_stats(self):
        with open(STATS_FILE, "w") as f:
            json.dump(self.stats, f, indent=2, default=str)

    def record_trade(self, symbol: str, side: str, action: str, cost: Decimal, leverage: Decimal):
        """Record a trade for performance tracking."""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "symbol": symbol,
            "side": side,
            "action": action,
            "cost": str(cost),
            "leverage": str(leverage),
        }

        # If closing, try to calculate PnL from open positions
        if action == "close" and self.executor:
            try:
                positions = self.executor.get_my_positions()
                key = f"{symbol}_{side}"
                if key in positions:
                    pnl = positions[key].get("unrealisedPnl", Decimal("0"))
                    entry["pnl"] = str(pnl)
                    self._update_stats(symbol, side, leverage, pnl)
            except Exception:
                pass

        self.history.append(entry)
        self._save_history()

    def _update_stats(self, symbol: str, side: str, leverage: Decimal, pnl: Decimal):
        """Update running statistics after a closed trade."""
        pnl = Decimal(str(pnl))
        self.stats["total_trades"] = self.stats.get("total_trades", 0) + 1
        total_pnl = Decimal(self.stats.get("total_pnl", "0")) + pnl
        self.stats["total_pnl"] = str(total_pnl)

        if pnl > 0:
            self.stats["wins"] = self.stats.get("wins", 0) + 1
            wins = self.stats["wins"]
            avg_win = Decimal(self.stats.get("avg_win", "0"))
            self.stats["avg_win"] = str(((avg_win * (wins - 1)) + pnl) / wins)
        elif pnl < 0:
            self.stats["losses"] = self.stats.get("losses", 0) + 1
            losses = self.stats["losses"]
            avg_loss = Decimal(self.stats.get("avg_loss", "0"))
            self.stats["avg_loss"] = str(((avg_loss * (losses - 1)) + pnl) / losses)

        # Track best/worst
        best = self.stats.get("best_trade")
        if best is None or pnl > Decimal(str(best.get("pnl", "0"))):
            self.stats["best_trade"] = {"symbol": symbol, "side": side, "pnl": str(pnl)}
        worst = self.stats.get("worst_trade")
        if worst is None or pnl < Decimal(str(worst.get("pnl", "0"))):
            self.stats["worst_trade"] = {"symbol": symbol, "side": side, "pnl": str(pnl)}

        # Track per-symbol performance
        sym_perf = self.stats.get("symbol_performance", {})
        if symbol not in sym_perf:
            sym_perf[symbol] = {"trades": 0, "pnl": "0", "wins": 0, "losses": 0}
        sym_perf[symbol]["trades"] += 1
        sym_perf[symbol]["pnl"] = str(Decimal(sym_perf[symbol]["pnl"]) + pnl)
        if pnl > 0:
            sym_perf[symbol]["wins"] += 1
        elif pnl < 0:
            sym_perf[symbol]["losses"] += 1
        self.stats["symbol_performance"] = sym_perf

        # Track best leverage per symbol
        lev_perf = self.stats.get("best_leverage", {})
        lev_key = str(leverage)
        if lev_key not in lev_perf:
            lev_perf[lev_key] = {"trades": 0, "total_pnl": "0"}
        lev_perf[lev_key]["trades"] += 1
        lev_perf[lev_key]["total_pnl"] = str(Decimal(lev_perf[lev_key]["total_pnl"]) + pnl)
        self.stats["best_leverage"] = lev_perf

        self._save_stats()

    def optimize_entry(self, symbol: str, side: str, cost: Decimal, leverage: Decimal) -> dict:
        """
        Suggest optimized entry parameters based on past performance.
        Returns adjusted cost/leverage or None if no optimization needed.
        """
        sym_perf = self.stats.get("symbol_performance", {})
        lev_perf = self.stats.get("best_leverage", {})

        suggestion = {"cost": cost, "leverage": leverage, "reason": "No historical data — using requested params."}

        # If we have history for this symbol, check win rate
        if symbol in sym_perf:
            perf = sym_perf[symbol]
            total = perf["trades"]
            if total >= 3:
                win_rate = perf["wins"] / total if total > 0 else 0
                avg_pnl = Decimal(perf["pnl"]) / total

                if win_rate >= 0.7:
                    # High win rate — allow slightly more aggressive sizing
                    boost = Decimal("1.15")
                    suggestion["cost"] = (cost * boost).quantize(Decimal("0.01"))
                    suggestion["reason"] = f"{symbol} has {win_rate:.0%} win rate over {total} trades — boosted size 15%."
                elif win_rate <= 0.3:
                    # Low win rate — reduce exposure
                    reduction = Decimal("0.75")
                    suggestion["cost"] = (cost * reduction).quantize(Decimal("0.01"))
                    suggestion["reason"] = f"{symbol} has {win_rate:.0%} win rate over {total} trades — reduced size 25%."

        # Check if a different leverage has performed better historically
        if lev_perf and len(lev_perf) >= 2:
            best_lev = None
            best_avg = Decimal("-999999")
            for lev_str, data in lev_perf.items():
                if data["trades"] >= 2:
                    avg = Decimal(data["total_pnl"]) / data["trades"]
                    if avg > best_avg:
                        best_avg = avg
                        best_lev = lev_str
            if best_lev and best_lev != str(leverage) and best_avg > 0:
                suggestion["leverage"] = Decimal(best_lev)
                suggestion["reason"] += f" Best-performing leverage historically: {best_lev}x."

        return suggestion

    def get_performance_tips(self) -> list[str]:
        """Return actionable tips based on trading history."""
        tips = []
        sym_perf = self.stats.get("symbol_performance", {})

        if not sym_perf:
            return ["No trade history yet. Tips will appear as you trade."]

        # Find best and worst performing symbols
        sorted_syms = sorted(sym_perf.items(), key=lambda x: Decimal(x[1]["pnl"]), reverse=True)

        if sorted_syms:
            best = sorted_syms[0]
            if Decimal(best[1]["pnl"]) > 0:
                tips.append(f"Best symbol: {best[0]} with +{Decimal(best[1]['pnl']):.2f} USDT over {best[1]['trades']} trades")

            if len(sorted_syms) > 1:
                worst = sorted_syms[-1]
                if Decimal(worst[1]["pnl"]) < 0:
                    tips.append(f"Worst symbol: {worst[0]} with {Decimal(worst[1]['pnl']):.2f} USDT — consider avoiding")

        # Win rate
        total = self.stats.get("total_trades", 0)
        wins = self.stats.get("wins", 0)
        if total >= 5:
            wr = wins / total
            tips.append(f"Overall win rate: {wr:.0%} ({wins}/{total})")
            if wr < 0.5:
                tips.append("Win rate below 50% — consider reducing position sizes or being more selective")

        return tips

    def show_stats(self):
        """Print full performance statistics."""
        print(f"\n{'=' * 60}")
        print(f"  TRADING PERFORMANCE STATS")
        print(f"{'=' * 60}")

        total = self.stats.get("total_trades", 0)
        if total == 0:
            print("  No completed trades yet.")
            print()
            return

        wins = self.stats.get("wins", 0)
        losses = self.stats.get("losses", 0)
        total_pnl = Decimal(self.stats.get("total_pnl", "0"))
        avg_win = Decimal(self.stats.get("avg_win", "0"))
        avg_loss = Decimal(self.stats.get("avg_loss", "0"))

        print(f"\n  Total Trades:  {total}")
        print(f"  Wins:          {wins}")
        print(f"  Losses:        {losses}")
        print(f"  Win Rate:      {wins/total:.0%}" if total else "")
        print(f"  Total PnL:     {'+' if total_pnl >= 0 else ''}{total_pnl:.2f} USDT")
        print(f"  Avg Win:       +{avg_win:.2f} USDT")
        print(f"  Avg Loss:      {avg_loss:.2f} USDT")

        best = self.stats.get("best_trade")
        if best:
            print(f"  Best Trade:    {best['symbol']} {best['side']} ({'+' if Decimal(best['pnl']) >= 0 else ''}{Decimal(best['pnl']):.2f} USDT)")
        worst = self.stats.get("worst_trade")
        if worst:
            print(f"  Worst Trade:   {worst['symbol']} {worst['side']} ({Decimal(worst['pnl']):.2f} USDT)")

        # Per-symbol breakdown
        sym_perf = self.stats.get("symbol_performance", {})
        if sym_perf:
            print(f"\n  --- Per-Symbol Performance ---")
            print(f"  {'Symbol':<12} {'Trades':<8} {'Wins':<6} {'Losses':<8} {'PnL':>12}")
            print(f"  {'-'*12} {'-'*8} {'-'*6} {'-'*8} {'-'*12}")
            for sym, data in sorted(sym_perf.items(), key=lambda x: Decimal(x[1]["pnl"]), reverse=True):
                pnl = Decimal(data["pnl"])
                pnl_str = f"+{pnl:.2f}" if pnl >= 0 else f"{pnl:.2f}"
                print(f"  {sym:<12} {data['trades']:<8} {data['wins']:<6} {data['losses']:<8} {pnl_str:>12}")

        # Leverage analysis
        lev_perf = self.stats.get("best_leverage", {})
        if lev_perf:
            print(f"\n  --- Leverage Performance ---")
            print(f"  {'Leverage':<12} {'Trades':<8} {'Avg PnL':>12}")
            print(f"  {'-'*12} {'-'*8} {'-'*12}")
            for lev, data in sorted(lev_perf.items(), key=lambda x: Decimal(x[1]["total_pnl"]) / max(x[1]["trades"], 1), reverse=True):
                avg = Decimal(data["total_pnl"]) / max(data["trades"], 1)
                avg_str = f"+{avg:.2f}" if avg >= 0 else f"{avg:.2f}"
                print(f"  {lev}x{'':<10} {data['trades']:<8} {avg_str:>12}")

        print()
