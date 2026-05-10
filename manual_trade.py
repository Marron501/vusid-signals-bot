"""
Manual trade interface.
Allows placing, closing, and managing trades directly from the command line.
"""
import logging
from decimal import Decimal

from trade_executor import TradeExecutor
from trade_optimizer import TradeOptimizer

logger = logging.getLogger(__name__)


class ManualTrader:
    def __init__(self):
        self.executor = TradeExecutor()
        self.optimizer = TradeOptimizer(self.executor)

    def open_trade(self, symbol: str, side: str, amount_usdt: float, leverage: float, optimize: bool = True):
        """
        Open a manual trade.

        Args:
            symbol: e.g. "BTCUSDT"
            side: "Buy" or "Sell"
            amount_usdt: how much USDT to allocate
            leverage: leverage multiplier
            optimize: if True, use optimizer for better entry sizing
        """
        symbol = symbol.upper()
        if not symbol.endswith("USDT"):
            symbol += "USDT"

        side = side.capitalize()
        if side not in ("Buy", "Sell"):
            print(f"  ERROR: side must be 'Buy' or 'Sell', got '{side}'")
            return False

        cost = Decimal(str(amount_usdt))
        lev = Decimal(str(leverage))

        if optimize:
            suggestion = self.optimizer.optimize_entry(symbol, side, cost, lev)
            if suggestion:
                cost = suggestion["cost"]
                lev = suggestion["leverage"]
                print(f"  Optimizer adjusted: cost={cost:.2f} USDT, leverage={lev}x")
                print(f"  Reason: {suggestion['reason']}")

        print(f"\n  Opening {side} {symbol} | {cost:.2f} USDT | {lev}x leverage")

        success = self.executor.open_position(symbol, side, cost, lev)
        if success:
            print(f"  SUCCESS: {side} {symbol} opened!")
            self.optimizer.record_trade(symbol, side, "open", cost, lev)
        else:
            print(f"  FAILED: Could not open {side} {symbol}")
        return success

    def close_trade(self, symbol: str, side: str):
        """Close an existing position."""
        symbol = symbol.upper()
        if not symbol.endswith("USDT"):
            symbol += "USDT"
        side = side.capitalize()

        print(f"\n  Closing {side} {symbol}...")

        success = self.executor.close_position(symbol, side)
        if success:
            print(f"  SUCCESS: {side} {symbol} closed!")
            self.optimizer.record_trade(symbol, side, "close", Decimal("0"), Decimal("0"))
        else:
            print(f"  FAILED: Could not close {side} {symbol}")
        return success

    def list_positions(self):
        """Show all open positions with PnL."""
        positions = self.executor.get_my_positions()
        equity = self.executor.get_equity()

        print(f"\n{'=' * 70}")
        print(f"  Account Equity: {equity:.2f} USDT")
        print(f"{'=' * 70}")

        if not positions:
            print("  No open positions.")
            print()
            return

        total_pnl = Decimal("0")
        print(f"\n  {'Symbol':<12} {'Side':<6} {'Size':<14} {'Lev':<6} {'Entry':<14} {'PnL':>12}")
        print(f"  {'-'*12} {'-'*6} {'-'*14} {'-'*6} {'-'*14} {'-'*12}")

        for key, pos in positions.items():
            pnl = pos["unrealisedPnl"]
            total_pnl += pnl
            pnl_str = f"+{pnl:.2f}" if pnl >= 0 else f"{pnl:.2f}"
            icon = "+" if pos["side"] == "Buy" else "-"
            print(
                f"  {pos['symbol']:<12} {pos['side']:<6} {str(pos['size']):<14} "
                f"{str(pos['leverage'])}x{'':<4} {str(pos['avgPrice']):<14} {pnl_str:>12}"
            )

        print(f"\n  Total Unrealized PnL: {'+' if total_pnl >= 0 else ''}{total_pnl:.2f} USDT")

        # Show optimizer insights
        tips = self.optimizer.get_performance_tips()
        if tips:
            print(f"\n  --- Optimizer Insights ---")
            for tip in tips:
                print(f"  {tip}")
        print()

    def show_stats(self):
        """Show trading performance statistics."""
        self.optimizer.show_stats()
