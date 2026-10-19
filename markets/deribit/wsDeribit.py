class WSDeribit:
    WS_URL = "wss://www.deribit.com/ws/api/v2"

    @staticmethod
    def build_subscribe_msg(channels: list[str]) -> dict:
        return {
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "params": {"channels": channels}
        }

    @staticmethod
    def parse_trade(message: dict) -> list[tuple] | None:
        if "params" not in message or "data" not in message["params"]:
            return None
        
        channel = message["params"].get("channel", "")
        if not channel.startswith("trades."):
            return None

        data_array = message["params"]["data"]
        parsed = []
        for t in data_array:
            price = float(t["price"])
            qty = float(t["amount"])
            taker = 1 if t["direction"] == "buy" else -1
            parsed.append((price, qty, price * qty, int(t["timestamp"]), taker))
            
        return parsed

    @staticmethod
    def parse_ticker_to_binance_schema(message: dict) -> dict | None:
        if "params" not in message or "data" not in message["params"]:
            return None

        channel = message["params"].get("channel", "")
        if not channel.startswith("ticker."):
            return None

        d = message["params"]["data"]
        symbol = d["instrument_name"]
        ts = int(d["timestamp"])
        
        greeks = d.get("greeks", {})
        
        griegas_tuple = (
            symbol, 
            float(d.get("mark_price") or 0.0),
            float(d.get("bid_iv") or 0.0), 
            float(d.get("ask_iv") or 0.0), 
            float(d.get("mark_iv") or 0.0),
            float(greeks.get("delta") or 0.0), 
            float(greeks.get("theta") or 0.0), 
            float(greeks.get("gamma") or 0.0), 
            float(greeks.get("vega") or 0.0),
            0.0, 0.0, 0.0, ts
        )

        oi_tuple = (symbol, ts, float(d.get("open_interest") or 0.0), 0.0)

        bids = [[float(d.get("best_bid_price") or 0.0), float(d.get("best_bid_amount") or 0.0)]]
        asks = [[float(d.get("best_ask_price") or 0.0), float(d.get("best_ask_amount") or 0.0)]]
        
        ob_tuple = (
            symbol, ts, ts, bids, asks,
            float(d.get("bid_iv") or 0.0), float(d.get("ask_iv") or 0.0), float(d.get("mark_iv") or 0.0),
            float(d.get("mark_price") or 0.0), float(d.get("underlying_price") or 0.0),
            float(greeks.get("delta") or 0.0), float(greeks.get("gamma") or 0.0), float(greeks.get("vega") or 0.0),
            float(greeks.get("theta") or 0.0), float(d.get("open_interest") or 0.0),
            float(d.get("stats", {}).get("volume_usd") or 0.0)
        )

        return {
            "griegas": griegas_tuple,
            "oi": oi_tuple,
            "top_orderbook": ob_tuple
        }