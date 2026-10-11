from core.wsClient import WSClient 

class WSFutures(WSClient):
    
    @staticmethod
    def parse_diff_ob(data: dict) -> tuple | None:
        payload = data.get("data", data)
        
        if payload.get("e") == "depthUpdate":
            return (
                int(payload.get("T")),
                int(payload.get("pu")),
                int(payload.get("u")),
                [[float(price), float(qty)] for price, qty in payload.get("b", [])],
                [[float(price), float(qty)] for price, qty in payload.get("a", [])]
            )
        return None

    @staticmethod
    def parse_liquidations(data: dict, target_symbol: str = None) -> tuple | None:
        payload = data.get("data", data)
        order = payload.get("o", {})
        
        if order.get("S"):
            if target_symbol and order.get("s") != target_symbol:
                return None
            # SELL = 1 (Long liquidado) | BUY = -1 (Short liquidado)
            side = 1 if order.get("S") == "SELL" else -1
            return (
                int(side),
                order.get("o"),
                float(order.get("p", 0.0)),
                float(order.get("ap", 0.0)),
                float(order.get("q", 0.0)),
                float(order.get("z", 0.0)),
                int(order.get("T"))
            )
        return None

    @classmethod
    async def stream_liquidations(cls, underlying: str, handler_callback):
        clean_sym = underlying.upper().replace("USDT", "")
        target_pair = f"{clean_sym}USDT"
        url = f"wss://fstream.binance.com/ws/{target_pair.lower()}@forceOrder"
        await cls.fetch(url, lambda d: cls.parse_liquidations(d, target_pair), handler_callback)

    @classmethod
    async def stream_agg_trades(cls, underlying: str, handler_callback):
        clean_sym = underlying.lower().replace("usdt", "")
        url = f"wss://fstream.binance.com/ws/{clean_sym}usdt@aggTrade"
        await cls.fetch(url, cls.parse_trade, handler_callback)

    @classmethod
    async def stream_diff_ob(cls, underlying: str, handler_callback):
        clean_sym = underlying.lower().replace("usdt", "")
        url = f"wss://fstream.binance.com/ws/{clean_sym}usdt@depth"
        await cls.fetch(url, cls.parse_diff_ob, handler_callback)