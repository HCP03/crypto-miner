import os
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import duckdb

class RoutineManager:
    @staticmethod
    def get_first_greeks_date(underlying: str, fallback_date: datetime) -> datetime:
        data_dir = os.getenv("DATA_DIR", "data")
        options_db_path = Path(f"{data_dir}/{underlying.upper()}/options_data.db")
        
        if not options_db_path.exists():
            return fallback_date

        try:
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
        except Exception as e:
            pass
        return fallback_date

    @staticmethod
    async def vision_daily_sync_loop(underlying: str, db, fallback_start_date: datetime, sync_func, tmp_dir: Path):
        """
        Bucle universal de Binance Vision. 
        'sync_func' debe ser la funcion Rest Spot o Futuros que descarga el dia.
        """
        sync_mode = os.getenv("SYNC_MODE", "greeks").lower()
        start_str = os.getenv("SYNC_START", "")
        end_str = os.getenv("SYNC_END", "")
        max_gap_days = int(os.getenv("MAX_GAP_DAYS", "3"))

        if sync_mode == "live_only":
            db.log.info("T0 leido correctamente | Modo: live_only (Vision desactivado)")
            return

        t0_date = RoutineManager.get_first_greeks_date(underlying, fallback_start_date).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) if sync_mode == "greeks" else fallback_start_date

        db.log.info(f"T0 leido correctamente | Modo: {sync_mode} (Base: {t0_date.strftime('%Y-%m-%d')})")
        initial_mode_done = False

        while True:
            try:
                now = datetime.now(timezone.utc)
                yesterday = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

                if not initial_mode_done:
                    if sync_mode == "range" and start_str and end_str:
                        s_date = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        e_date = min(datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc), yesterday)
                        db.log.info(f"Vision Sync [Rango]: {s_date.strftime('%Y-%m-%d')} -> {e_date.strftime('%Y-%m-%d')}")
                        curr = s_date
                        while curr <= e_date:
                            await sync_func(underlying, curr, db, tmp_dir)
                            curr += timedelta(days=1)
                        t0_date = s_date

                    elif sync_mode == "from_date" and start_str:
                        s_date = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        db.log.info(f"Vision Sync [Desde Fecha]: {s_date.strftime('%Y-%m-%d')} -> {yesterday.strftime('%Y-%m-%d')}")
                        curr = s_date
                        while curr <= yesterday:
                            await sync_func(underlying, curr, db, tmp_dir)
                            curr += timedelta(days=1)
                        t0_date = s_date

                    elif sync_mode == "backfill_max":
                        db.log.info(f"Vision Sync [Backfill]: Max gap={max_gap_days}d")
                        curr = yesterday
                        consecutive_missing = 0
                        while consecutive_missing <= max_gap_days:
                            had_data = await sync_func(underlying, curr, db, tmp_dir)
                            if had_data:
                                consecutive_missing = 0
                                t0_date = curr
                            else:
                                consecutive_missing += 1
                                db.log.warning(f"Vision Sync: Sin datos el {curr.strftime('%Y-%m-%d')} (Gap {consecutive_missing}/{max_gap_days})")
                            curr -= timedelta(days=1)
                        db.log.info(f"Vision Sync [Backfill]: Completado hasta {t0_date.strftime('%Y-%m-%d')}.")

                    else:
                        db.log.info(f"Vision Sync [Default T0]: {t0_date.strftime('%Y-%m-%d')} -> {yesterday.strftime('%Y-%m-%d')}")
                        curr = t0_date
                        while curr <= yesterday:
                            await sync_func(underlying, curr, db, tmp_dir)
                            curr += timedelta(days=1)

                    initial_mode_done = True
                else:
                    curr = t0_date
                    while curr <= yesterday:
                        await sync_func(underlying, curr, db, tmp_dir)
                        curr += timedelta(days=1)

                now = datetime.now(timezone.utc)
                next_run = now.replace(hour=4, minute=0, second=0, microsecond=0)
                if now >= next_run:
                    next_run += timedelta(days=1)

                sleep_seconds = (next_run - now).total_seconds()
                db.log.info(f"Vision al dia. Proxima revision en {sleep_seconds / 3600:.1f}h (04:00 UTC).")
                await asyncio.sleep(sleep_seconds)

            except asyncio.CancelledError:
                break
            except Exception as e:
                db.log.error(f"Error en bucle Vision ({e}). Reintentando en 5m...")
                await asyncio.sleep(300)

    @staticmethod
    async def periodic_snapshot_loop(db, snapshot_coro, t_minute: int, *args, **kwargs):
        """Ejecuta una corrutina de snapshot de forma infinita cada t_minute."""
        while True:
            try:
                await snapshot_coro(*args, **kwargs)
            except asyncio.CancelledError:
                break
            except Exception as e:
                db.log.error(f"Error en snapshot periodico: {e}")
            await asyncio.sleep(60 * t_minute)