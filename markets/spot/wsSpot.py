from datetime import datetime, timezone
from core.wsClient import WSClient

class WSspot(WSClient):
     
    @staticmethod
    def parse_diff_ob(data: dict) -> tuple | None:
        """
        Parsea los eventos depthUpdate del WebSocket de Spot y devuelve tupla para Spot_DB.ob_diff:
        (timestamp, lastUpdateId, firstUpdateId, bids, asks)
        """
        payload = data.get("data", data)
        if payload.get("e") == "depthUpdate":
            ts_ms = int(payload.get("E", datetime.now(timezone.utc).timestamp() * 1000))
            return (
                ts_ms,
                int(payload.get("u")), # lastUpdateId
                int(payload.get("U")), # firstUpdateId
                [[float(price), float(qty)] for price, qty in payload.get("b", [])],
                [[float(price), float(qty)] for price, qty in payload.get("a", [])]
            )
        return None
        
    @classmethod
    async def stream_agg_trades(cls, underlying: str, handler_callback):
        clean_sym = underlying.lower().replace("usdt", "")
        url = f"wss://stream.binance.com:9443/ws/{clean_sym}usdt@aggTrade"
        await cls.fetch(url, cls.parse_trade, handler_callback)

    @classmethod
    async def stream_trades(cls, underlying: str, handler_callback):
        clean_sym = underlying.lower().replace("usdt", "")
        url = f"wss://stream.binance.com:9443/ws/{clean_sym}usdt@trade"
        await cls.fetch(url, cls.parse_trade, handler_callback)

    @classmethod
    async def stream_diff_ob(cls, underlying: str, handler_callback):
        clean_sym = underlying.lower().replace("usdt", "")
        url = f"wss://stream.binance.com:9443/ws/{clean_sym}usdt@depth"
        await cls.fetch(url, cls.parse_diff_ob, handler_callback)