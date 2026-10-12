import shutil
import asyncio
import signal
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
import duckdb

from markets.spot.spotDB import Spot_DB
from markets.spot.restSpot import RestSpot
from markets.spot.wsSpot import WSspot

TMP_DIR = Path("./data/tmp_spot")


def get_first_greeks_date(underlying: str, fallback_date: datetime) -> datetime:
    """Lee en modo READ_ONLY el primer timestamp de griegas en options_data.db."""
    options_db_path = f"data/{underlying}/options_data.db"
    if not os.path.exists(options_db_path):
        return fallback_date

    try:
        con = duckdb.connect(options_db_path, read_only=True)
        try:
            tables = [r[0] for r in con.execute("SHOW TABLES;").fetchall()]
            target_table = "greeks" if "greeks" in tables else ("griegas" if "griegas" in tables else None)
            if target_table:
                res = con.execute(f"SELECT MIN(timestamp) FROM {target_table};").fetchone()
                if res and res[0]:
                    min_ts = int(res[0])
                    ts_sec = min_ts / 1_000_000 if min_ts > 1e14 else min_ts / 1_000
                    dt = datetime.fromtimestamp(ts_sec, tz=timezone.utc)
                    return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        finally:
            con.close()
    except Exception as e:
        print(f"[Spot Worker] No se pudo leer T0 desde options_data.db ({e}). Usando fallback.")

    return fallback_date


async def periodic_snapshot_spot(db: Spot_DB, symbol: str, t_minute: int = 30):
    """Toma un snapshot REST de spot.orderbook (/api/v3/depth) cada t_minute minutos."""
    while True:
        try:
            ob_dict = await RestSpot.fetch_orderbook(symbol, limit=5000)
            if ob_dict:
                row_tuple = (
                    ob_dict["timestamp"],
                    ob_dict["lastUpdateId"],
                    ob_dict["bids"],
                    ob_dict["asks"]
                )
                db.insert_many("ob_snapshot", [row_tuple])
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Error durante el snapshot REST Spot: {e}")
            
        await asyncio.sleep(60 * t_minute)


async def vision_daily_sync_loop(underlying: str, db, fallback_start_date: datetime):
    """
    Soporta 4 modos de sincronizacion configurables por variables de entorno:
    - 'greeks' (Modo 4 - Default): Desde el primer snapshot de griegas (T0) hasta hoy.
    - 'range' (Modo 1): Desde SYNC_START hasta SYNC_END (y luego mantiene al dia).
    - 'from_date' (Modo 2): Desde SYNC_START hasta hoy.
    - 'backfill_max' (Modo 3): Desde ayer hacia atras hasta encontrar > 3 dias seguidos de gap.
    """
    sync_mode = os.getenv("SYNC_MODE", "greeks").lower()
    start_str = os.getenv("SYNC_START", "")
    end_str = os.getenv("SYNC_END", "")
    max_gap_days = int(os.getenv("MAX_GAP_DAYS", "3"))


    if sync_mode == "live_only":
        print(f"[Vision Sync] T0 leido correctamente (Modo: live_only - omitiendo descargas de Binance Vision)", flush=True)
        return

    if sync_mode == "greeks":
        t0_date = get_first_greeks_date(underlying, fallback_start_date).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    else:
        t0_date = fallback_start_date

    # Emitimos la linea de confirmacion para que main_worker sepa que ya no tocamos options_data.db
    print(f"[Vision Sync] T0 leido correctamente (Modo: {sync_mode})", flush=True)

    initial_run_done = False

    while True:
        try:
            now = datetime.now(timezone.utc)
            yesterday = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

            if not initial_run_done:
                # ---------------------------------------------------------
                # MODO 1: Rango fijo (start_date -> end_date)
                # ---------------------------------------------------------
                if sync_mode == "range" and start_str and end_str:
                    s_date = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    e_date = min(datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc), yesterday)
                    print(f"[Vision Sync] Modo 1 (Rango): {s_date.strftime('%Y-%m-%d')} -> {e_date.strftime('%Y-%m-%d')}", flush=True)
                    curr = t0_date
                    while curr <= yesterday:
                        await RestSpot.sync_day_vision(underlying, curr, db, TMP_DIR / underlying)
                        curr += timedelta(days=1)

                # ---------------------------------------------------------
                # MODO 2: Desde start_date hasta hoy (ayer UTC)
                # ---------------------------------------------------------
                elif sync_mode == "from_date" and start_str:
                    s_date = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    print(f"[Vision Sync] Modo 2 (Desde fecha): {s_date.strftime('%Y-%m-%d')} -> {yesterday.strftime('%Y-%m-%d')}", flush=True)
                    curr = s_date
                    while curr <= yesterday:
                        await RestSpot.sync_day_vision(underlying, curr, db, TMP_DIR / underlying)
                        curr += timedelta(days=1)

                # ---------------------------------------------------------
                # MODO 3: Backfill desde hoy hacia atras (max 3 dias de gap)
                # ---------------------------------------------------------
                elif sync_mode == "backfill_max":
                    print(f"[Vision Sync] Modo 3 (Backfill inverso desde {yesterday.strftime('%Y-%m-%d')}, max gap = {max_gap_days} dias)...", flush=True)
                    curr = yesterday
                    consecutive_missing = 0
                    while consecutive_missing <= max_gap_days:
                        had_data = await RestSpot.sync_day_vision(underlying, curr, db, TMP_DIR / underlying)
                        if had_data:
                            consecutive_missing = 0
                        else:
                            consecutive_missing += 1
                            print(
                                f"[Vision Sync] Sin datos en {curr.strftime('%Y-%m-%d')} "
                                f"(Gap consecutivo: {consecutive_missing}/{max_gap_days})",
                                flush=True
                            )
                        curr -= timedelta(days=1)
                    print(f"[Vision Sync] Backfill historico completado tras superar {max_gap_days} dias sin datos.", flush=True)

                # ---------------------------------------------------------
                # MODO 4 (Default): Desde el primer snapshot de griegas (T0)
                # ---------------------------------------------------------
                else:
                    print(f"[Vision Sync] Modo 4 (Default T0 Griegas): {t0_date.strftime('%Y-%m-%d')} -> {yesterday.strftime('%Y-%m-%d')}", flush=True)
                    curr = t0_date
                    while curr <= yesterday:
                        await RestSpot.sync_day_vision(underlying, curr, db, TMP_DIR / underlying)
                        curr += timedelta(days=1)

                initial_run_done = True
            else:
                # En los dias sucesivos (a las 04:00 UTC), sincroniza el dia anterior (yesterday)
                print(f"[Vision Sync] Sincronizando cierre diario de ayer ({yesterday.strftime('%Y-%m-%d')})...", flush=True)
                await RestSpot.sync_day_vision(underlying, yesterday, db, TMP_DIR / underlying)

            # Dormir hasta las 04:00 UTC del dia siguiente
            now = datetime.now(timezone.utc)
            next_run = now.replace(hour=4, minute=0, second=0, microsecond=0)
            if now >= next_run:
                next_run += timedelta(days=1)
                
            sleep_seconds = (next_run - now).total_seconds()
            print(f"[Vision Sync] {underlying} al dia. Proxima descarga en {sleep_seconds / 3600:.2f}h (04:00 UTC).", flush=True)
            await asyncio.sleep(sleep_seconds)

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[Vision Sync] Error en bucle diario ({e}). Reintentando en 5 min...", flush=True)
            await asyncio.sleep(300)

async def run_spot_worker(underlying: str = "BTC", snapshot_interval_min: int = 30, fallback_days_back: int = 3):
    underlying = underlying.upper().replace("USDT", "")
    print(f"Iniciando Spot Worker para {underlying}...")
    shutil.rmtree(TMP_DIR / underlying, ignore_errors=True)
    db = Spot_DB(symbol=underlying)
    ob_diff_handler = db.make_ws_handler(table_name="ob_diff", batch_size=50)

    fallback_date = (datetime.now(timezone.utc) - timedelta(days=fallback_days_back)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    tasks = [
        asyncio.create_task(WSspot.stream_diff_ob(underlying, ob_diff_handler), name="ws_spot_ob_diff"),
        asyncio.create_task(periodic_snapshot_spot(db, underlying, t_minute=snapshot_interval_min), name="rest_spot_snapshot"),
        asyncio.create_task(vision_daily_sync_loop(underlying, db, fallback_date), name="vision_spot_trades"),
        asyncio.create_task(db.auto_flush_loop(interval=5), name="db_auto_flush"),
    ]

    loop = asyncio.get_running_loop()
    def _signal_handler():
        print("\nSenal de parada recibida en Spot Worker. Volcando buffers...")
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
        print("Guardando datos pendientes de Spot en DuckDB...")
        db.close()
        await RestSpot.close_session()
        print(f"Spot Worker ({underlying}) detenido correctamente.")


if __name__ == "__main__":
    SYMBOL = os.getenv("UNDERLYING", "BTC")
    try:
        asyncio.run(run_spot_worker(underlying=SYMBOL))
    except KeyboardInterrupt:
        pass