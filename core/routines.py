import os
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path


class RoutineManager:
    @staticmethod
    def get_first_greeks_date(underlying: str, fallback_date: datetime) -> datetime:
        data_dir = os.getenv("DATA_DIR", "data")
        exchange = os.getenv("EXCHANGE", "binance").lower()
        options_db_path = Path(f"{data_dir}/{exchange}/{underlying.upper()}/options_data.db")
        if not options_db_path.exists():
            return fallback_date

        try:
            import duckdb
            con = duckdb.connect(str(options_db_path), read_only=True)
            try:
                tables = [r[0] for r in con.execute("SHOW TABLES;").fetchall()]
                target_table = "greeks" if "greeks" in tables else ("griegas" if "griegas" in tables else None)
                if target_table:
                    res = con.execute(f"SELECT MIN(timestamp) FROM {target_table};").fetchone()
                    if res and res[0]:
                        min_ts = int(res[0])
                        ts_sec = min_ts / 1_000_000 if min_ts > 1e14 else (min_ts / 1_000 if min_ts > 1e10 else min_ts)
                        dt = datetime.fromtimestamp(ts_sec, tz=timezone.utc)
                        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
            finally:
                con.close()
        except Exception:
            pass
        return fallback_date

    @staticmethod
    async def periodic_snapshot_loop(db, snapshot_coro, t_minute: int, *args, **kwargs):
        while True:
            try:
                await snapshot_coro(*args, **kwargs)
            except asyncio.CancelledError:
                break
            except Exception as e:
                db.log.error(f"Error en snapshot periodico: {e}")
            await asyncio.sleep(60 * t_minute)
