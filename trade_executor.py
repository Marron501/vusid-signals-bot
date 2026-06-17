"""
Executes trades on your Bybit account via the V5 API.
Handles order placement, position closing, leverage management, and sizing.
"""
from __future__ import annotations

import logging
import os
from decimal import Decimal, ROUND_DOWN

from pybit.unified_trading import HTTP

import config

logger = logging.getLogger(__name__)

CATEGORY = "linear"


def _apply_proxy() -> str | None:
    """
    If BYBIT_PROXY_URL is configured, inject it into the process environment so
    that the underlying `requests` library (used by pybit) routes all HTTPS calls
    through the proxy.  Returns the proxy URL for logging, or None if not set.
    """
    proxy = config.BYBIT_PROXY_URL
    if not proxy:
        return None
    os.environ["HTTP_PROXY"]  = proxy
    os.environ["HTTPS_PROXY"] = proxy
    # Make sure requests doesn't accidentally bypass the proxy for bybit domains
    os.environ.pop("NO_PROXY",  None)
    os.environ.pop("no_proxy",  None)
    return proxy


class TradeExecutor:
    def __init__(self, api_key: str = None, api_secret: str = None, testnet: bool = None):
        """
        Initialise executor.  If api_key / api_secret are omitted the values
        from config (env vars) are used — preserving full backward compatibility.
        Pass them explicitly when managing multiple trading accounts.
        """
        key    = api_key    if api_key    else config.API_KEY
        secret = api_secret if api_secret else config.API_SECRET
        demo   = testnet    if testnet is not None else config.USE_TESTNET

        if not key or not secret:
            raise ValueError("BYBIT_API_KEY and BYBIT_API_SECRET must be set")

        proxy = _apply_proxy()
        if proxy:
            # Mask credentials for logging
            safe = proxy.split("@")[-1] if "@" in proxy else proxy[:40]
            logger.info(f"[proxy] Bybit requests routed via {safe}")
        else:
            logger.info("[proxy] Direct connection (no proxy)")

        self.client = HTTP(
            api_key=key,
            api_secret=secret,
            testnet=demo,   # testnet.bybit.com when True
            demo=False,
        )
        mode = "TESTNET" if demo else "LIVE"
        logger.info(f"Trade executor initialised in {mode} mode")
        self.instruments: dict[str, dict] = {}
        self._load_instruments()

    def _load_instruments(self):
        """Load all USDT perpetual instrument specs for proper qty/leverage rounding."""
        logger.info("Loading instrument specifications...")
        cursor = ""
        while True:
            resp = self.client.get_instruments_info(category=CATEGORY, cursor=cursor)
            for inst in resp["result"]["list"]:
                self.instruments[inst["symbol"]] = {
                    "min_qty": Decimal(inst["lotSizeFilter"]["minOrderQty"]),
                    "max_qty": Decimal(inst["lotSizeFilter"]["maxOrderQty"]),
                    "qty_step": Decimal(inst["lotSizeFilter"]["qtyStep"]),
                    "min_leverage": Decimal(inst["leverageFilter"]["minLeverage"]),
                    "max_leverage": Decimal(inst["leverageFilter"]["maxLeverage"]),
                }
            cursor = resp["result"].get("nextPageCursor", "")
            if not cursor:
                break
        logger.info(f"Loaded {len(self.instruments)} instruments")

    def get_equity(self) -> Decimal:
        """Get USDT equity from unified account."""
        wallet = self.client.get_wallet_balance(accountType="UNIFIED")
        for coin in wallet["result"]["list"][0]["coin"]:
            if coin["coin"] == "USDT":
                return Decimal(coin["equity"])
        return Decimal("0")

    def get_full_balance(self) -> dict:
        """Get full USDT wallet breakdown: equity, available, used margin, unrealised PnL."""
        try:
            wallet   = self.client.get_wallet_balance(accountType="UNIFIED")
            account  = wallet["result"]["list"][0]
            total_eq = Decimal(account.get("totalEquity") or "0")
            total_av = Decimal(account.get("totalAvailableBalance") or "0")
            total_mm = Decimal(account.get("totalInitialMargin") or "0")
            total_up = Decimal(account.get("totalPerpUPL") or "0")
            usdt_eq  = Decimal("0")
            usdt_av  = Decimal("0")
            for coin in account.get("coin", []):
                if coin["coin"] == "USDT":
                    usdt_eq = Decimal(coin.get("equity") or "0")
                    usdt_av = Decimal(coin.get("availableToWithdraw") or coin.get("availableToBorrow") or "0")
                    break
            return {
                "equity":           float(usdt_eq),
                "available":        float(usdt_av),
                "used_margin":      float(total_mm),
                "unrealised_pnl":   float(total_up),
                "total_equity_usd": float(total_eq),
                "error":            None,
            }
        except Exception as e:
            logger.error(f"get_full_balance failed: {e}")
            return {"equity": 0, "available": 0, "used_margin": 0,
                    "unrealised_pnl": 0, "total_equity_usd": 0, "error": str(e)}

    def get_mark_price(self, symbol: str) -> Decimal:
        """Get current mark price for a symbol."""
        ticker = self.client.get_tickers(category=CATEGORY, symbol=symbol)
        return Decimal(ticker["result"]["list"][0]["markPrice"])

    def calculate_qty(self, symbol: str, cost_usdt: Decimal, leverage: Decimal):
        """Calculate order quantity from USDT cost, respecting instrument limits."""
        if symbol not in self.instruments:
            logger.warning(f"Unknown instrument: {symbol}")
            return None

        inst = self.instruments[symbol]
        price = self.get_mark_price(symbol)
        raw_qty = (cost_usdt * leverage) / price

        # Round down to nearest qty_step
        step = inst["qty_step"]
        if step < 1:
            qty = raw_qty.quantize(step, rounding=ROUND_DOWN)
        else:
            qty = (raw_qty // step) * step

        if qty < inst["min_qty"]:
            logger.warning(f"{symbol}: calculated qty {qty} below minimum {inst['min_qty']}")
            return None
        if qty > inst["max_qty"]:
            qty = inst["max_qty"]

        return qty

    def set_margin_mode(self, symbol: str):
        """Set cross margin mode for a symbol."""
        try:
            self.client.switch_margin_mode(
                category=CATEGORY,
                symbol=symbol,
                tradeMode=0,  # 0 = cross margin
                buyLeverage=str(config.DEFAULT_LEVERAGE),
                sellLeverage=str(config.DEFAULT_LEVERAGE),
            )
            logger.info(f"{symbol}: margin mode set to CROSS")
        except Exception as e:
            if "110043" in str(e) or "already" in str(e).lower():
                pass
            else:
                logger.warning(f"{symbol}: failed to set margin mode: {e}")

    def set_leverage(self, symbol: str, leverage: Decimal):
        """Set leverage for a symbol (both buy and sell sides) with cross margin."""
        if symbol not in self.instruments:
            return

        # Always enforce cross margin
        self.set_margin_mode(symbol)

        inst = self.instruments[symbol]
        lev = max(inst["min_leverage"], min(leverage, inst["max_leverage"]))

        try:
            self.client.set_leverage(
                category=CATEGORY,
                symbol=symbol,
                buyLeverage=str(lev),
                sellLeverage=str(lev),
            )
            logger.info(f"{symbol}: leverage set to {lev}x cross")
        except Exception as e:
            # Error 110043 means leverage is already set to this value
            if "110043" in str(e):
                pass
            else:
                logger.warning(f"{symbol}: failed to set leverage: {e}")

    def get_my_positions(self) -> dict[str, dict]:
        """Get all open positions on our account."""
        positions = {}
        resp = self.client.get_positions(category=CATEGORY, settleCoin="USDT")
        for p in resp["result"]["list"]:
            size = Decimal(p["size"])
            if size > 0:
                key = f"{p['symbol']}_{p['side']}"
                positions[key] = {
                    "symbol": p["symbol"],
                    "side": p["side"],
                    "size": size,
                    "leverage": Decimal(p["leverage"]),
                    "avgPrice": Decimal(p["avgPrice"]),
                    "unrealisedPnl": Decimal(p["unrealisedPnl"]),
                    "positionIdx": p["positionIdx"],
                    "stopLoss":  p.get("stopLoss", ""),
                    "takeProfit": p.get("takeProfit", ""),
                }
        return positions

    def open_position(self, symbol: str, side: str, cost_usdt: Decimal, leverage: Decimal) -> bool:
        """Open a new position with market order."""
        import time
        self.last_open_error = ""
        self.set_leverage(symbol, leverage)
        qty = self.calculate_qty(symbol, cost_usdt, leverage)

        if not qty:
            self.last_open_error = f"Cannot calculate valid qty for {symbol} (cost={cost_usdt:.2f})"
            logger.error(self.last_open_error)
            return False

        last_err = ""
        for attempt in range(1, 4):
            try:
                result = self.client.place_order(
                    category=CATEGORY,
                    symbol=symbol,
                    side=side,
                    orderType="Market",
                    qty=str(qty),
                    positionIdx=config.POSITION_MODE,
                )
                logger.info(f"OPENED {side} {symbol}: qty={qty}, leverage={leverage}x, orderId={result['result']['orderId']}")
                return True
            except Exception as e:
                last_err = str(e)
                logger.error(f"Attempt {attempt}/3 failed to open {side} {symbol}: {e}")
                if attempt < 3:
                    time.sleep(attempt * 2)
        self.last_open_error = last_err
        return False

    def close_position(self, symbol: str, side: str) -> bool:
        """Close an existing position with market order."""
        my_positions = self.get_my_positions()
        key = f"{symbol}_{side}"

        if key not in my_positions:
            logger.warning(f"No open position found for {key}")
            return False

        pos = my_positions[key]
        close_side = "Sell" if side == "Buy" else "Buy"

        try:
            result = self.client.place_order(
                category=CATEGORY,
                symbol=symbol,
                side=close_side,
                orderType="Market",
                qty=str(pos["size"]),
                positionIdx=config.POSITION_MODE,
                reduceOnly=True,
            )
            logger.info(f"CLOSED {side} {symbol}: qty={pos['size']}, orderId={result['result']['orderId']}")
            return True
        except Exception as e:
            logger.error(f"Failed to close {side} {symbol}: {e}")
            return False

    def set_trading_stop(self, symbol: str, side: str,
                         stop_loss=None, take_profit=None) -> dict:
        """Set or clear SL/TP for an open position via Bybit trading-stop endpoint."""
        try:
            kwargs = dict(
                category=CATEGORY,
                symbol=symbol,
                positionIdx=config.POSITION_MODE,
            )
            if stop_loss is not None:
                kwargs["stopLoss"] = "" if str(stop_loss) == "0" else str(stop_loss)
            if take_profit is not None:
                kwargs["takeProfit"] = "" if str(take_profit) == "0" else str(take_profit)
            self.client.set_trading_stop(**kwargs)
            logger.info(f"set_trading_stop {symbol} SL={stop_loss} TP={take_profit}")
            return {"success": True}
        except Exception as e:
            logger.error(f"set_trading_stop failed {symbol}: {e}")
            return {"success": False, "error": str(e)}

    def adjust_position(self, symbol: str, side: str, new_cost_usdt: Decimal, leverage: Decimal) -> bool:
        """Adjust an existing position size (increase or partial close)."""
        my_positions = self.get_my_positions()
        key = f"{symbol}_{side}"

        if key not in my_positions:
            # Position doesn't exist, open new
            return self.open_position(symbol, side, new_cost_usdt, leverage)

        self.set_leverage(symbol, leverage)
        current_qty = my_positions[key]["size"]
        target_qty = self.calculate_qty(symbol, new_cost_usdt, leverage)

        if not target_qty:
            return False

        diff = target_qty - current_qty

        if abs(diff) < (self.instruments.get(symbol, {}).get("min_qty", Decimal("0.001"))):
            return True  # No significant change needed

        if diff > 0:
            # Need to increase position
            try:
                self.client.place_order(
                    category=CATEGORY,
                    symbol=symbol,
                    side=side,
                    orderType="Market",
                    qty=str(diff),
                    positionIdx=config.POSITION_MODE,
                )
                logger.info(f"INCREASED {side} {symbol} by {diff}")
                return True
            except Exception as e:
                logger.error(f"Failed to increase {side} {symbol}: {e}")
                return False
        else:
            # Need to decrease position
            close_side = "Sell" if side == "Buy" else "Buy"
            try:
                self.client.place_order(
                    category=CATEGORY,
                    symbol=symbol,
                    side=close_side,
                    orderType="Market",
                    qty=str(abs(diff)),
                    positionIdx=config.POSITION_MODE,
                    reduceOnly=True,
                )
                logger.info(f"DECREASED {side} {symbol} by {abs(diff)}")
                return True
            except Exception as e:
                logger.error(f"Failed to decrease {side} {symbol}: {e}")
                return False
