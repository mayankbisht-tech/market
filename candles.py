from datetime import datetime, timezone
class CandleBuilder:
    def __init__(self):
        self._open_candles: dict[str, dict] = {}
    def process_trade(self, symbol: str, price: float, qty: float, ts: datetime):
        bucket = ts.replace(microsecond=0)
        current = self._open_candles.get(symbol)
        if current is None:
            self._open_candles[symbol] = self._new_candle(symbol, bucket, price, qty)
            return None
        if current["opened_at"] == bucket:
            current["high"]        = max(current["high"], price)
            current["low"]         = min(current["low"],  price)
            current["close"]       = price
            current["volume"]     += qty
            current["trade_count"] += 1
            return None

        completed = current.copy()
        self._open_candles[symbol] = self._new_candle(symbol, bucket, price, qty)
        return completed

    def _new_candle(self, symbol, bucket, price, qty):
        return {
            "symbol":      symbol,
            "opened_at":   bucket,
            "open":        price,
            "high":        price,
            "low":         price,
            "close":       price,
            "volume":      qty,
            "trade_count": 1,
        }
