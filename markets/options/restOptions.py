import datetime
from core.restClient import RestClient

class RestOptions(RestClient):
    BASE_URL = "https://eapi.binance.com/eapi/v1"

    @classmethod 
    async def fetch_griegas(cls, symbol: str = None) -> list[dict]:
        url = f"{cls.BASE_URL}/mark"
        params = {'symbol': symbol} if symbol is not None else None
        
        response = await cls.get(url, params=params)
        ts_ms = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
        
        result = []
        if isinstance(response, list):
            for data in response:
                if data.get('symbol'):
                    result.append({
                        "symbol": data["symbol"],
                        "markPrice": float(data.get("markPrice", 0.0)),
                        "bidIV": float(data.get("bidIV", 0.0)),
                        "askIV": float(data.get("askIV", 0.0)),
                        "markIV": float(data.get("markIV", 0.0)),
                        "delta": float(data.get("delta", 0.0)),
                        "theta": float(data.get("theta", 0.0)),
                        "gamma": float(data.get("gamma", 0.0)),
                        "vega": float(data.get("vega", 0.0)),
                        "highPriceLimit": float(data.get("highPriceLimit", 0.0)),
                        "lowPriceLimit": float(data.get("lowPriceLimit", 0.0)),
                        "riskFreeInterest": float(data.get("riskFreeInterest", 0.0)),
                        "timestamp": ts_ms
                    })
        return result

    @classmethod
    async def fetch_contracts(cls) -> list[dict] | None:
        url = f"{cls.BASE_URL}/exchangeInfo"
        data = await cls.get(url)
        if data and data.get('optionSymbols'):
            results = []
            for item in data['optionSymbols']:
                symbol = item.get("symbol")
                expiration = symbol.split('-')[1]
                side = 'C' if item.get("side") == "CALL" else 'P'
                results.append({
                    "symbol": symbol,
                    "expiration_ts": int(item.get("expiryDate")),
                    "expiration_date": expiration,
                    "strike_price": float(item.get("strikePrice")),
                    "option_type": side
                })
            return results
        return None

    @classmethod
    async def fetch_open_interest(cls, underlying: str, expiration: str) -> list[dict] | None:
        url = f"{cls.BASE_URL}/openInterest"
        params = {"underlyingAsset": underlying.upper(), "expiration": expiration}
        data = await cls.get(url, params=params)
        if isinstance(data, list):
            return [
                {
                    "symbol": item.get("symbol"),
                    "timestamp": int(item.get("timestamp", 0)),
                    "sumOpenInterest": float(item.get("sumOpenInterest", 0.0)),
                    "sumOpenInterestUsd": float(item.get("sumOpenInterestUsd", 0.0))
                }
                for item in data
            ]
        return None

    @classmethod
    async def fetch_tickers_24h(cls) -> list[dict] | None:
        """Obtiene el volumen 24h y el precio del subyacente (exercisePrice) por contrato."""
        url = f"{cls.BASE_URL}/ticker"
        data = await cls.get(url)
        if isinstance(data, list):
            return [
                {
                    "symbol": item.get("symbol"),
                    "volume_24h": float(item.get("volume", 0.0)),
                    "underlying_price": float(item.get("exercisePrice", 0.0))
                }
                for item in data if item.get("symbol")
            ]
        return None