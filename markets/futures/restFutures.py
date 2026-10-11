import csv
from datetime import datetime
from pathlib import Path
from core.restClient import RestClient

class RestFutures(RestClient):
    BASE_URL = "https://fapi.binance.com/fapi/v1"
    
    # URLs de Binance Data Collection (Vision) - Futures UM
    TRADES_PERP_URL = "https://data.binance.vision/data/futures/um/daily/trades/{}/{}-trades-{}.zip"
    OI_URL = "https://data.binance.vision/data/futures/um/daily/metrics/{}/{}-metrics-{}.zip"
    BOOK_DEPTH_URL = "https://data.binance.vision/data/futures/um/daily/bookDepth/{}/{}-bookDepth-{}.zip"
    FUNDING_MONTHLY_URL = "https://data.binance.vision/data/futures/um/monthly/fundingRate/{}/{}-fundingRate-{}.zip"
    FUNDING_DAILY_URL = "https://data.binance.vision/data/futures/um/daily/fundingRate/{}/{}-fundingRate-{}.zip"

    @staticmethod
    def _format_pair(symbol: str) -> str:
        symbol = symbol.upper()
        return symbol if symbol.endswith("USDT") else f"{symbol}USDT"

    @staticmethod
    def _get_day_bounds_ms(date_obj: datetime) -> tuple[int, int]:
        start_ms = int(date_obj.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        end_ms = int(date_obj.replace(hour=23, minute=59, second=59, microsecond=999999).timestamp() * 1000)
        return start_ms, end_ms

    # ==========================================================
    #                 ENDPOINTS EN TIEMPO REAL (API)
    # ==========================================================
    @classmethod
    async def fetch_orderbook(cls, symbol: str, limit: int = 1000) -> dict:
        url = f"{cls.BASE_URL}/depth"
        params = {"symbol": cls._format_pair(symbol), "limit": limit}
        data = await cls.get(url, params)

        return {
            "timestamp": data.get("T"),
            "lastUpdateId": data.get("lastUpdateId"),
            "bids": [[float(price), float(qty)] for price, qty in data.get("bids", [])],
            "asks": [[float(price), float(qty)] for price, qty in data.get("asks", [])]
        }
        
    @classmethod
    async def fetch_open_interest(cls, symbol: str) -> dict:
        url = f"{cls.BASE_URL}/openInterest"
        params = {"symbol": cls._format_pair(symbol)}
        data = await cls.get(url, params)
    
        return {
            "openInterest": float(data.get("openInterest", 0.0)),
            "timestamp": data.get("time")
        }

    @classmethod
    async def fetch_funding_rate(cls, symbol: str) -> dict:
        url = f"{cls.BASE_URL}/premiumIndex"
        params = {"symbol": cls._format_pair(symbol)}
        data = await cls.get(url, params)

        return {
            "lastFundingRate": float(data.get("lastFundingRate", 0.0)),
            "timestamp": int(data.get("time"))
        }

    # ==========================================================
    #        DESCARGA DE HISTÓRICOS (VISION + API)
    # ==========================================================
    @classmethod
    async def _download_and_extract_vision(cls, url: str, target_dir: Path, zip_name: str, date_str: str) -> Path | None:
        target_dir.mkdir(parents=True, exist_ok=True)
        local_zip_path = target_dir / zip_name
        final_csv_path = target_dir / f"{date_str}.csv"

        if final_csv_path.exists():
            return final_csv_path

        if await cls.download_zip(url, local_zip_path):
            return cls.unzip_and_remove(local_zip_path, final_csv_path)
        return None

    @classmethod
    async def download_trades(cls, symbol: str, date_obj: datetime, target_dir: Path) -> Path | None:
        pair = cls._format_pair(symbol)
        date_str = date_obj.strftime("%Y-%m-%d")
        url = cls.TRADES_PERP_URL.format(pair, pair, date_str)
        zip_name = f"{pair}-trades-{date_str}.zip"
        return await cls._download_and_extract_vision(url, target_dir, zip_name, date_str)

    @classmethod
    async def download_oi(cls, symbol: str, date_obj: datetime, target_dir: Path) -> Path | None:
        pair = cls._format_pair(symbol)
        date_str = date_obj.strftime("%Y-%m-%d")
        url = cls.OI_URL.format(pair, pair, date_str)
        zip_name = f"{pair}-metrics-{date_str}.zip"
        return await cls._download_and_extract_vision(url, target_dir, zip_name, date_str)

    @classmethod
    async def download_orderbooks(cls, symbol: str, date_obj: datetime, target_dir: Path) -> Path | None:
        pair = cls._format_pair(symbol)
        date_str = date_obj.strftime("%Y-%m-%d")
        url = cls.BOOK_DEPTH_URL.format(pair, pair, date_str)
        zip_name = f"{pair}-bookDepth-{date_str}.zip"
        return await cls._download_and_extract_vision(url, target_dir, zip_name, date_str)

    @classmethod
    async def download_funding(cls, symbol: str, date_obj: datetime, target_dir: Path) -> Path | None:
        """
        1. Intenta descargar desde Binance Data Collection (Vision): primero diario, luego mensual (YYYY-MM).
        2. Si el mes aún no ha cerrado en Vision (404), genera el CSV del día vía fapi para no dejar huecos.
        """
        pair = cls._format_pair(symbol)
        date_str = date_obj.strftime("%Y-%m-%d")
        month_str = date_obj.strftime("%Y-%m")
        target_dir.mkdir(parents=True, exist_ok=True)
        final_csv_path = target_dir / f"{date_str}.csv"

        if final_csv_path.exists():
            return final_csv_path

        # A) Intentar descarga desde Binance Vision (Daily o Monthly)
        daily_url = cls.FUNDING_DAILY_URL.format(pair, pair, date_str)
        daily_zip = target_dir / f"{pair}-fundingRate-{date_str}.zip"
        if await cls.download_zip(daily_url, daily_zip):
            return cls.unzip_and_remove(daily_zip, final_csv_path)

        monthly_url = cls.FUNDING_MONTHLY_URL.format(pair, pair, month_str)
        monthly_zip = target_dir / f"{pair}-fundingRate-{month_str}.zip"
        if await cls.download_zip(monthly_url, monthly_zip):
            return cls.unzip_and_remove(monthly_zip, final_csv_path)

        # B) Fallback para días del mes actual (aún no publicados en el ZIP mensual de Vision)
        url = f"{cls.BASE_URL}/fundingRate"
        start_ts, end_ts = cls._get_day_bounds_ms(date_obj)
        params = {"symbol": pair, "startTime": start_ts, "endTime": end_ts, "limit": 100}

        try:
            data = await cls.get(url, params=params)
            if not data:
                return None

            all_rows = [[item["fundingTime"], 8, item["fundingRate"]] for item in data]
            all_rows.sort(key=lambda x: int(x[0]))

            # Guardamos con el mismo esquema exacto que los CSVs oficiales de Binance Vision
            with open(final_csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["calc_time", "funding_interval_hours", "last_funding_rate"])
                writer.writerows(all_rows)

            return final_csv_path
        except Exception as e:
            print(f"Error descargando Funding ({date_str}): {e}")
            return None

    # ==========================================================
    #        GENERADORES SQL ADAPTADOS A FUTURES_DB
    # ==========================================================
    @classmethod
    def get_trades_sql(cls, file_path: Path, date_obj: datetime) -> str:
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
    def get_oi_sql(cls, file_path: Path, date_obj: datetime) -> str:
        start_ms, end_ms = cls._get_day_bounds_ms(date_obj)
        return f"""
            INSERT INTO open_interest (
                openInterest, timestamp, oi_value, ls_ratio_global, ls_ratio_top_acc, ls_ratio_top_pos
            )
            SELECT 
                sum_open_interest::DOUBLE,
                epoch_ms(CAST(create_time AS TIMESTAMP))::BIGINT,
                sum_open_interest_value::DOUBLE,
                count_long_short_ratio::DOUBLE,
                count_toptrader_long_short_ratio::DOUBLE,
                sum_toptrader_long_short_ratio::DOUBLE
            FROM read_csv('{file_path}', auto_detect=TRUE, header=TRUE)
            WHERE epoch_ms(CAST(create_time AS TIMESTAMP)) >= {start_ms} 
              AND epoch_ms(CAST(create_time AS TIMESTAMP)) <= {end_ms}
            ON CONFLICT DO NOTHING;
        """

    @classmethod
    def get_funding_sql(cls, file_path: Path, date_obj: datetime) -> str:
        """
        Lee el CSV con el formato oficial de Binance Vision:
        (calc_time, funding_interval_hours, last_funding_rate)
        e inserta en funding_rate (lastFundingRate, timestamp).
        """
        start_ms, end_ms = cls._get_day_bounds_ms(date_obj)
        return f"""
            INSERT INTO funding_rate (lastFundingRate, timestamp)
            SELECT 
                last_funding_rate::DOUBLE,
                calc_time::BIGINT
            FROM read_csv('{file_path}', auto_detect=TRUE, header=TRUE)
            WHERE calc_time::BIGINT >= {start_ms} AND calc_time::BIGINT <= {end_ms}
            ON CONFLICT DO NOTHING;
        """

    @classmethod
    def get_orderbooks_sql(cls, file_path: Path, date_obj: datetime) -> str:
        start_ms, end_ms = cls._get_day_bounds_ms(date_obj)
        return f"""
            INSERT INTO ob_depth (timestamp, percentage, depth, notional)
            SELECT 
                CAST(EXTRACT(epoch FROM CAST(timestamp AS TIMESTAMP)) * 1000 AS BIGINT) AS ts,
                percentage::DOUBLE,
                depth::DOUBLE,
                notional::DOUBLE
            FROM read_csv_auto('{file_path}', header=TRUE)
            WHERE ts >= {start_ms} AND ts <= {end_ms}
            ON CONFLICT DO NOTHING;
        """

    # ==========================================================
    #            ORQUESTADOR DIARIO DE BINANCE VISION
    # ==========================================================
    @classmethod
    async def sync_day_vision(cls, symbol: str, date_obj: datetime, db, tmp_dir: Path) -> bool:
        """
        Descarga e ingesta un dia en futures_DB.
        Devuelve True si el dia tenia datos (en DB o descargados), False si no habia datos en Binance Vision.
        """
        tasks = [
            ("trades", cls.download_trades, cls.get_trades_sql),
            ("open_interest", cls.download_oi, cls.get_oi_sql),
            ("funding_rate", cls.download_funding, cls.get_funding_sql),
            ("ob_depth", cls.download_orderbooks, cls.get_orderbooks_sql),
        ]

        day_had_data = False
        for table_name, download_fn, sql_fn in tasks:
            if db.is_day_ingested(table_name, date_obj):
                if table_name == "trades":
                    day_had_data = True
                continue

            csv_path = await download_fn(symbol, date_obj, tmp_dir / table_name)
            if csv_path:
                try:
                    query = sql_fn(csv_path, date_obj)
                    db.execute_query(query)
                    db.mark_day_ingested(table_name, date_obj)
                    if table_name == "trades":
                        day_had_data = True
                finally:
                    cls.remove_file(csv_path)

        return day_had_data