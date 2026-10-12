import asyncio
import os
import shutil
import signal
from datetime import datetime, timedelta, timezone
from pathlib import Path
import duckdb

from markets.futures.futuresDB import futures_DB
from markets.futures.restFutures import RestFutures
from markets.futures.wsFutures import WSFutures

TMP_DIR = Path("./data/tmp_futures")


def get_first_greeks_date(underlying: str, fallback_date: datetime) -> datetime:
    """
    Obtiene el timestamp mas antiguo de la tabla griegas en options_data.db (T0).
    Si no existe, devuelve fallback_date.
    """
    options_db_path = Path(f"data/{underlying.upper()}/options_data.db")
    if not options_db_path.exists():
        return fallback_date

    try:
        con = duckdb.connect(str(options_db_path), read_only=True)
        try:
            tables = [r[0] for r in con.execute("SHOW TABLES;").fetchall()]
            if "griegas" in tables:
                res = con.execute("SELECT MIN(timestamp) FROM griegas;").fetchone()
                if res and res[0]:
                    min_ts = res[0]
                    ts_sec = min_ts / 1_000_000 if min_ts > 1e14 else (min_ts / 1_000 if min_ts > 1e10 else min_ts)
                    dt = datetime.fromtimestamp(ts_sec, tz=timezone.utc)
                    return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        finally:
            con.close()
    except Exception:
        pass

    return fallback_date


async def vision_daily_sync_loop(underlying: str, db: futures_DB, fallback_start_date: datetime):
    """
    Gestiona los 5 modos de sincronizacion historica con logs limpios.
    """
    sync_mode = os.getenv("SYNC_MODE", "greeks").lower()
    start_str = os.getenv("SYNC_START", "")
    end_str = os.getenv("SYNC_END", "")
    max_gap_days = int(os.getenv("MAX_GAP_DAYS", "3"))

    # MODO 5: Solo en vivo (omite Binance Vision)
    if sync_mode == "live_only":
        db.log.info("T0 leido correctamente | Modo: live_only (Vision desactivado)")
        return

    # Leemos T0 una sola vez al arrancar si estamos en modo 'greeks'
    if sync_mode == "greeks":
        t0_date = get_first_greeks_date(underlying, fallback_start_date).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    else:
        t0_date = fallback_start_date

    # IMPORTANTE: Mantener el texto "T0 leido correctamente" para el evento de main_worker
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
                        await RestFutures.sync_day_vision(underlying, curr, db, TMP_DIR / underlying)
                        curr += timedelta(days=1)
                    t0_date = s_date

                elif sync_mode == "from_date" and start_str:
                    s_date = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    db.log.info(f"Vision Sync [Desde Fecha]: {s_date.strftime('%Y-%m-%d')} -> {yesterday.strftime('%Y-%m-%d')}")
                    curr = s_date
                    while curr <= yesterday:
                        await RestFutures.sync_day_vision(underlying, curr, db, TMP_DIR / underlying)
                        curr += timedelta(days=1)
                    t0_date = s_date

                elif sync_mode == "backfill_max":
                    db.log.info(f"Vision Sync [Backfill]: Iniciando desde {yesterday.strftime('%Y-%m-%d')} hacia atras (max gap={max_gap_days}d)")
                    curr = yesterday
                    consecutive_missing = 0
                    while consecutive_missing <= max_gap_days:
                        had_data = await RestFutures.sync_day_vision(underlying, curr, db, TMP_DIR / underlying)
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
                        await RestFutures.sync_day_vision(underlying, curr, db, TMP_DIR / underlying)
                        curr += timedelta(days=1)

                initial_mode_done = True
            else:
                # En los dias siguientes a las 04:00 UTC, repasa desde t0_date hasta yesterday
                # (los dias ya descargados se saltan en microsegundos gracias a vision_sync_log)
                curr = t0_date
                while curr <= yesterday:
                    await RestFutures.sync_day_vision(underlying, curr, db, TMP_DIR / underlying)
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


async def run_futures_worker(underlying: str = "BTC", fallback_days_back: int = 3):
    underlying = underlying.upper().replace("USDT", "")
    shutil.rmtree(TMP_DIR / underlying, ignore_errors=True)

    db = futures_DB(underlying=underlying)
    db.log.info("Iniciando Futures Worker...")

    liq_handler = db.make_ws_handler(table_name="liquidations", batch_size=25)
    fallback_start = datetime.now(timezone.utc) - timedelta(days=fallback_days_back)

    tasks = [
        asyncio.create_task(WSFutures.stream_liquidations(underlying, liq_handler), name="ws_liquidations"),
        asyncio.create_task(vision_daily_sync_loop(underlying, db, fallback_start), name="vision_sync"),
        asyncio.create_task(db.auto_flush_loop(interval=5), name="db_auto_flush"),
    ]

    loop = asyncio.get_running_loop()
    closing = False
    
    def _signal_handler():
        nonlocal closing
        if closing:
            return
        closing = True
        db.log.info("Parada recibida. Vaciando buffers y eliminando .wal")
        for t in tasks:
            t.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        db.close()
        await RestFutures.close_session()
        db.log.info("Futures Worker detenido correctamente.")


if __name__ == "__main__":
    SYMBOL = os.getenv("UNDERLYING", "BTC")
    try:
        asyncio.run(run_futures_worker(underlying=SYMBOL, fallback_days_back=3))
    except KeyboardInterrupt:
        pass