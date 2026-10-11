import asyncio
from core.wsClient import WSClient

class WSOptions(WSClient):

    @staticmethod
    def parse_partial_book_depth(data: dict) -> tuple | None:
        payload = data.get("data", data) if isinstance(data, dict) else {}
            
        if "b" in payload and "a" in payload:
            update_id = int(payload.get("u", 0))
            ts_ms = int(payload.get("T") or payload.get("E") or 0)
            symbol = str(payload.get("s", ""))
            if not symbol or ts_ms == 0:
                return None

            bids_top5 = [[float(price), float(qty)] for price, qty in payload.get("b", [])[:5]]
            asks_top5 = [[float(price), float(qty)] for price, qty in payload.get("a", [])[:5]]
            return (
                symbol,
                ts_ms,
                update_id,
                bids_top5,
                asks_top5
            )
        return None
    
    @staticmethod
    def parse_option_trade(data: dict) -> tuple | None:
        payload = data.get("data", data) if isinstance(data, dict) else {}
        if "p" in payload and "q" in payload:
            price = float(payload["p"])
            qty = float(payload["q"])
            taker_side = -1 if payload.get("m", False) else 1
            
            return (
                int(payload.get("t", 0)),
                int(payload.get("T", 0)),
                str(payload.get("s")),
                price,
                qty,
                float(price * qty),
                int(taker_side),
                str(payload.get("X", ""))
            )
        return None

    @staticmethod
    def parse_griegas(data: dict | list) -> list[tuple] | None:
        resultados = []
        payload = data.get("data", data) if isinstance(data, dict) else data
            
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict) and "s" in item:
                    resultados.append((
                        str(item.get("s")),
                        float(item.get("mp", 0.0)),
                        float(item.get("b", 0.0)),
                        float(item.get("a", 0.0)),
                        float(item.get("vo", 0.0)),
                        float(item.get("d", 0.0)),
                        float(item.get("t", 0.0)),
                        float(item.get("g", 0.0)),
                        float(item.get("v", 0.0)),
                        0.0,
                        0.0,
                        float(item.get("rf", 0.0)),
                        int(item.get("E", 0))
                    ))
        return resultados if resultados else None

    @classmethod
    async def stream_contracts_book_depth(
        cls, symbols: list[str], handler_callback, updateSpeed: str = "100ms", chunk_size: int = 25
    ):
        """
        En la arquitectura enrutada de Binance (fstream.binance.com/public/stream):
        1. Todos los simbolos en la URL deben ir en MINUSCULAS (sym.lower()).
        2. Soporta directamente @depth5@100ms.
        """
        if not symbols:
            return
        tasks = []
        for i in range(0, len(symbols), chunk_size):
            batch = symbols[i:i + chunk_size]
            streams_str = "/".join([f"{sym.lower()}@depth5@{updateSpeed}" for sym in batch])
            url = f"wss://fstream.binance.com/public/stream?streams={streams_str}"
            tasks.append(asyncio.create_task(cls.fetch(url, cls.parse_partial_book_depth, handler_callback)))
            await asyncio.sleep(0.25)
            
        await asyncio.gather(*tasks)

    @classmethod
    async def stream_griegas(cls, underlying: str, handler_callback):
        clean_sym = underlying.lower().replace("usdt", "")
        # optionMarkPrice pertenece al router /market/
        url = f"wss://fstream.binance.com/market/ws/{clean_sym}usdt@optionMarkPrice"
        await cls.fetch(url, cls.parse_griegas, handler_callback)

    @classmethod
    async def stream_option_trades(cls, underlying: str, handler_callback):
        clean_sym = underlying.lower().replace("usdt", "")
        # optionTrade pertenece al router /public/
        url = f"wss://fstream.binance.com/public/ws/{clean_sym}usdt@optionTrade"
        await cls.fetch(url, cls.parse_option_trade, handler_callback)