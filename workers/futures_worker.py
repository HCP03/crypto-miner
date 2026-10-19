import asyncio
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from markets.futures.futuresDB import futures_DB
from markets.futures.restFutures import RestFutures
from markets.futures.wsFutures import WSFutures
from core.lifecycle import WorkerLifecycle
from core.routines import RoutineManager

DATA_DIR = os.getenv("DATA_DIR", "data")
TMP_DIR = Path(f"{DATA_DIR}/tmp_futures")
EXCHANGE = os.getenv("EXCHANGE", "binance").lower()

async def run_futures_worker(underlying: str = "BTC", fallback_days_back: int = 3):
    underlying = underlying.upper().replace("USDT", "")
    shutil.rmtree(TMP_DIR / underlying, ignore_errors=True)

    db = futures_DB(underlying=underlying)
    db.log.info("Iniciando Futures Worker...")

    liq_handler = db.make_ws_handler(table_name="liquidations", batch_size=25)
    fallback_start = datetime.now(timezone.utc) - timedelta(days=fallback_days_back)

    tasks = [
        asyncio.create_task(WSFutures.stream_liquidations(underlying, liq_handler), name="ws_liquidations"),
        asyncio.create_task(RoutineManager.vision_daily_sync_loop(
            underlying=underlying, db=db, fallback_start_date=fallback_start, 
            sync_func=RestFutures.sync_day_vision, tmp_dir=TMP_DIR / underlying
        ), name="vision_sync"),
        asyncio.create_task(db.auto_flush_loop(interval=5), name="db_auto_flush"),
    ]

    lifecycle = WorkerLifecycle(db, RestFutures, "Futures Worker", underlying)
    await lifecycle.run(tasks)


if __name__ == "__main__":
    SYMBOL = os.getenv("UNDERLYING", "BTC")
    asyncio.run(run_futures_worker(underlying=SYMBOL, fallback_days_back=3))