from datetime import datetime
import duckdb as db
import ctypes
import numpy


class TimeBarProcessor:
    def __init__(self, path:str= "../data", exchange:str="binance", symbol:str= "BTC", market:str="futures"):
        self.db_path = f"{path}/{exchange}/{symbol}/{market}_data.db"
        self.con = db.connect(self.db_path)
        
    def dinamicThreshold(self, target_bars_per_day):
        # Native DuckDB moving average computation
        self.con.execute("PRAGMA memory_limit='2GB'")

        # 1. Check if the table exists
        table_exists = self.con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = 'daily_thresholds'"
        ).fetchone()
        
        # 2. If it exists, check if the latest trade day is already calculated
        if table_exists:
            check_sync_query = """
                SELECT 
                    (SELECT COALESCE(MAX(day_start_ms), 0) FROM daily_thresholds) AS max_thresh,
                    (SELECT CAST(epoch(time_bucket(INTERVAL '1 day', epoch_ms(CAST(MAX(timestamp) AS BIGINT)))) * 1000 AS BIGINT) FROM trades) AS max_trade
            """
            res = self.con.execute(check_sync_query).fetchone()
            
            max_thresh = res[0]
            max_trade = res[1]
            
            # If the threshold table covers the latest trade day, skip recalculation
            if max_thresh >= max_trade:
                print("Dynamic thresholds already up to date")
                return
            
        # Changed to CREATE OR REPLACE to ensure it updates when new ticks exist
        query = f"""
            CREATE OR REPLACE TABLE daily_thresholds AS 
            WITH daily_vols AS (
                SELECT 
                    time_bucket(INTERVAL '1 day', epoch_ms(CAST(timestamp AS BIGINT))) AS day_start,
                    SUM(quoteQty) AS total_volume
                FROM trades
                GROUP BY day_start
            )
            SELECT 
                CAST(epoch(day_start) * 1000 AS BIGINT) AS day_start_ms,
                AVG(total_volume) OVER (
                    ORDER BY day_start ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
                ) / {target_bars_per_day} AS active_threshold
            FROM daily_vols
            ORDER BY day_start_ms ASC;
            """
        self.con.execute(query)
        print("Dinamic thresholds updated")

    def volume_bars(self, threshold: int = 0, start_date: str = None, end_date: str = None, target_bars_per_day:int = 50):

        req_start_ms = int(datetime.fromisoformat(start_date).timestamp() * 1000) if start_date else 0
        req_end_ms = int(datetime.fromisoformat(end_date).timestamp() * 1000) if end_date else 0

        if threshold == 0:
            self.dinamicThreshold(target_bars_per_day=target_bars_per_day)
            table_name = "candles_volume_dinamic"
        else:
            table_name = f"candles_volume_{threshold}"

        print(f"Triggering native C pipeline for volume bars")
        self.con.close()

        # 3. Call native C pipeline
        # compile .c file with gcc -O3 -shared -fPIC -o libvolumebars.so volume_bars.c -lduckdb (Linux)
        # Use './libvolumebars.so' for Mac/Linux or './libvolumebars.dll' for Windows
        c_lib = ctypes.CDLL('./libvolumebars.so')

        # void compute_volume_bars(const char *db_path, int threshold, long long start, long long end)
        c_lib.compute_volume_bars.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.c_longlong, ctypes.c_longlong]
        c_lib.compute_volume_bars.restype = None
        
        c_lib.compute_volume_bars(self.db_path.encode('utf-8'), threshold, req_start_ms, req_end_ms)
        
        print("Volume bars computed")

        return_conditions = []
        if req_start_ms > 0:
            return_conditions.append(f"start_timestamp >= {req_start_ms}")
        if req_end_ms > 0:
            return_conditions.append(f"start_timestamp < {req_end_ms}")
            
        return_interval = "WHERE " + " AND ".join(return_conditions) if return_conditions else ""
        self.con = db.connect(self.db_path, read_only=True)
        fetch_query = f"""
            SELECT * FROM {table_name}
            {return_interval}
            ORDER BY start_timestamp ASC;
        """
        
        df = self.con.execute(fetch_query).pl()
        self.con.close()
        print("Volume bars saved")
        return df

    def tf_bars(self, timeframe: str = "1 hour", start_date: str = None, end_date: str = None):

        # 1. Format dynamic table name (e.g., "1 hour" -> "candles_1_hour")
        table_name = f"candles_{timeframe.replace(' ', '_').lower()}"

        # 2. Parse requested dates into milliseconds
        req_start_ms = int(datetime.fromisoformat(start_date).timestamp() * 1000) if start_date else None
        req_end_ms = int(datetime.fromisoformat(end_date).timestamp() * 1000) if end_date else None

        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                candle TIMESTAMP PRIMARY KEY,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                contracts DOUBLE,
                taker_buy_vol DOUBLE,
                taker_sell_vol DOUBLE,
                taker_delta DOUBLE,
                taker_buy_contracts DOUBLE,
                taker_sell_contracts DOUBLE,
                taker_delta_contracts DOUBLE
            );
        """)

        # 4. Find the latest candle we already have in the database
        max_time_query = f"SELECT CAST(COALESCE(MAX(epoch(candle) * 1000), 0) AS BIGINT) FROM {table_name}"
        
        result = self.con.execute(max_time_query).pl()
        
        last_candle_ms = result.item()

        # 5. Determine what tick data actually needs to be processed
        trade_start_ms = last_candle_ms
        if req_start_ms:
            trade_start_ms = max(req_start_ms, last_candle_ms)
        
        conditions = []
        if trade_start_ms > 0:
            conditions.append(f"trades.timestamp >= {trade_start_ms}")
        if req_end_ms:
            conditions.append(f"trades.timestamp < {req_end_ms}")
            
        trade_interval = "WHERE " + " AND ".join(conditions) if conditions else ""

        # 6. Process missing trades and Upsert into the table
        if not req_end_ms or trade_start_ms < req_end_ms:
            upsert_query = f"""
                INSERT INTO {table_name}
                SELECT 
                    time_bucket(INTERVAL '{timeframe}', epoch_ms(CAST(timestamp AS BIGINT))) AS candle,
                    arg_min(price, timestamp) as open,
                    max(price) as high,
                    min(price) as low,
                    arg_max(price, timestamp) as close,
                    
                    sum(qty) as contracts,
                    sum(quoteQty) as volume,
                    
                    -- Taker metrics for Volume (quoteQty)
                    sum(CASE WHEN taker = 1 THEN quoteQty ELSE 0 END) AS taker_buy_vol,
                    sum(CASE WHEN taker = -1 THEN quoteQty ELSE 0 END) AS taker_sell_vol,
                    sum(CASE WHEN taker = 1 THEN quoteQty ELSE -quoteQty END) AS taker_delta,
                    
                    -- Taker metrics for Contracts (qty)
                    sum(CASE WHEN taker = 1 THEN qty ELSE 0 END) AS taker_buy_contracts,
                    sum(CASE WHEN taker = -1 THEN qty ELSE 0 END) AS taker_sell_contracts,
                    sum(CASE WHEN taker = 1 THEN qty ELSE -qty END) AS taker_delta_contracts
                    
                FROM trades
                {trade_interval}
                GROUP BY candle
                
                ON CONFLICT (candle) DO UPDATE SET
                    high = GREATEST(EXCLUDED.high, {table_name}.high),
                    low = LEAST(EXCLUDED.low, {table_name}.low),
                    close = EXCLUDED.close,
                    
                    volume = {table_name}.volume + EXCLUDED.volume,
                    contracts = {table_name}.contracts + EXCLUDED.contracts,
                    
                    taker_buy_vol = {table_name}.taker_buy_vol + EXCLUDED.taker_buy_vol,
                    taker_sell_vol = {table_name}.taker_sell_vol + EXCLUDED.taker_sell_vol,
                    taker_delta = {table_name}.taker_delta + EXCLUDED.taker_delta,
                    
                    taker_buy_contracts = {table_name}.taker_buy_contracts + EXCLUDED.taker_buy_contracts,
                    taker_sell_contracts = {table_name}.taker_sell_contracts + EXCLUDED.taker_sell_contracts,
                    taker_delta_contracts = {table_name}.taker_delta_contracts + EXCLUDED.taker_delta_contracts;
            """
            self.con.execute(upsert_query)

        # 7. Return the requested timeframe directly from the updated table
        return_conditions = []
        if req_start_ms:
            return_conditions.append(f"epoch(candle) * 1000 >= {req_start_ms}")
        if req_end_ms:
            return_conditions.append(f"epoch(candle) * 1000 < {req_end_ms}")
            
        return_interval = "WHERE " + " AND ".join(return_conditions) if return_conditions else ""

        fetch_query = f"""
            SELECT * FROM {table_name}
            {return_interval}
            ORDER BY candle ASC;
        """

        df = self.con.execute(fetch_query).pl()
        self.con.close()
        return df

if __name__ == "__main__":
    #TimeBarProcessor(market="spot").tf_bars(timeframe='1 hour')
    TimeBarProcessor(market="spot").tf_bars(timeframe='4 hour')
    TimeBarProcessor(market="spot").tf_bars(timeframe='1 day')
