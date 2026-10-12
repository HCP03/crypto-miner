from datetime import datetime, timezone
from pathlib import Path
from core.restClient import RestClient

class RestSpot(RestClient):
    BASE_URL = "https://api.binance.com/api/v3"
    TRADES_SPOT_URL = "https://data.binance.vision/data/spot/daily/trades/{}/{}-trades-{}.zip"

    @staticmethod
    def _format_pair(symbol: str) -> str:
        symbol = symbol.upper()
        return symbol if symbol.endswith("USDT") else f"{symbol}USDT"

    # ==========================================================
    #                 ENDPOINTS EN TIEMPO REAL (API)
    # ==========================================================
    @classmethod
    async def fetch_orderbook(cls, symbol: str, limit: int = 5000) -> dict:
        url = f"{cls.BASE_URL}/depth"
        params = {"symbol": cls._format_pair(symbol), "limit": limit}
        data = await cls.get(url, params=params)
        
        return {
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000), # Milisegundos
            "lastUpdateId": int(data.get("lastUpdateId")),
            "bids": [[float(price), float(qty)] for price, qty in data.get("bids", [])],
            "asks": [[float(price), float(qty)] for price, qty in data.get("asks", [])]
        }

    # ==========================================================
    #        DESCARGA DE HISTÓRICOS (BINANCE VISION SPOT)
    # ==========================================================
    @classmethod
    async def download_trades(cls, symbol: str, date_obj: datetime, target_dir: Path) -> Path | None:
        pair = cls._format_pair(symbol)
        date_str = date_obj.strftime("%Y-%m-%d")
        target_dir.mkdir(parents=True, exist_ok=True)
        
        local_zip_path = target_dir / f"{pair}-trades-{date_str}.zip"
        final_csv_path = target_dir / f"{date_str}.csv"

        if final_csv_path.exists():
            return final_csv_path

        url = cls.TRADES_SPOT_URL.format(pair, pair, date_str)
        if await cls.download_zip(url, local_zip_path):
            return cls.unzip_and_remove(local_zip_path, final_csv_path)
        return None
 
    @classmethod
    def get_trades_sql(cls, file_path: Path, date_obj: datetime) -> str:
        """
        En Binance Vision Spot, el CSV NO tiene cabecera y el timestamp viene en MICROSEGUNDOS (16 dígitos)
        o milisegundos (13 dígitos) en años antiguos. Normalizamos siempre a MILISEGUNDOS (13 dígitos).
        Columnas CSV Spot: id(0), price(1), qty(2), quoteQty(3), time(4), isBuyerMaker(5), isBestMatch(6)
        """
        start_ms, end_ms = cls._get_day_bounds_ms(date_obj)
        return f"""
            INSERT INTO trades (price, qty, quoteQty, timestamp, taker)
            SELECT 
                column01::DOUBLE AS price,
                SUM(column02::DOUBLE) AS qty,
                SUM(column03::DOUBLE) AS quoteQty,
                CASE 
                    WHEN column04::BIGINT > 100000000000000 THEN (column04::BIGINT // 1000)
                    ELSE column04::BIGINT 
                END AS ts,
                CASE WHEN column05::BOOLEAN THEN -1 ELSE 1 END AS taker
            FROM read_csv('{file_path}', header=False, auto_detect=True)
            WHERE ts >= {start_ms} AND ts <= {end_ms}
            GROUP BY ts, price, taker
            ON CONFLICT DO NOTHING;
        """
    @classmethod
    async def sync_day_vision(cls, symbol: str, date_obj: datetime, db, tmp_dir: Path) -> bool:
        """
        Descarga e ingesta un dia en Spot_DB.
        Devuelve True si el dia tenia datos, False si Binance Vision devolvio 404.
        """
        table_name = "trades"
        if db.is_day_ingested(table_name, date_obj):
            return True

        csv_path = await cls.download_trades(symbol, date_obj, tmp_dir / table_name)
        if csv_path:
            try:
                query = cls.get_trades_sql(csv_path, date_obj)
                db.execute_query(query, table_name=table_name)
                db.mark_day_ingested(table_name, date_obj)
                return True
            finally:
                cls.remove_file(csv_path)
        return False