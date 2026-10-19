import asyncio
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from markets.spot.spotDB import Spot_DB
from markets.spot.restSpot import RestSpot
from markets.spot.wsSpot import WSspot
from core.lifecycle import WorkerLifecycle
from core.routines import RoutineManager

DATA_DIR = os.getenv("DATA_DIR", "data")
TMP_DIR = Path(f"{DATA_DIR}/tmp_spot")
EXCHANGE = os.getenv("EXCHANGE", "binance").lower()

async def _spot_snapshot_action(db: Spot_DB, symbol: str):
    """Adaptador de la logica exacta de un snapshot spot para pasarlo al bucle periodico"""
    ob_dict = await RestSpot.fetch_orderbook(symbol, limit=5000)
    if ob_dict:
        db.insert_many("ob_snapshot", [(
            ob_dict["timestamp"], ob_dict["lastUpdateId"],
            ob_dict["bids"], ob_dict["asks"]
        )])

async def _snapshot_action():
    ob_dict = await RestSpot.fetch_orderbook(underlying, limit=5000)
    if ob_dict:
        db.insert_many("ob_snapshot", [(
            ob_dict["timestamp"], ob_dict["lastUpdateId"],
            ob_dict["bids"], ob_dict["asks"]
        )])

    tasks = [
        asyncio.create_task(WSspot.stream_diff_ob(underlying, ob_diff_handler), name="ws_spot_ob_diff"),
        asyncio.create_task(RoutineManager.periodic_snapshot_loop(
            db, _snapshot_action, snapshot_interval_min
        ), name="rest_spot_snapshot"),
        asyncio.create_task(RoutineManager.vision_daily_sync_loop(
            underlying=underlying, db=db, fallback_start_date=fallback_start, 
            sync_func=RestSpot.sync_day_vision, tmp_dir=TMP_DIR / underlying
        ), name="vision_spot_trades"),
        asyncio.create_task(db.auto_flush_loop(interval=5), name="db_auto_flush"),
    ]

    lifecycle = WorkerLifecycle(db, RestSpot, "Spot Worker", underlying)
    await lifecycle.run(tasks)

async def run_spot_worker(underlying: str = "BTC", snapshot_interval_min: int = 30, fallback_days_back: int = 3):
    underlying = underlying.upper().replace("USDT", "")
    shutil.rmtree(TMP_DIR / underlying, ignore_errors=True)
    
    db = Spot_DB(symbol=underlying)
    db.log.info("Iniciando Spot Worker...")
    
    ob_diff_handler = db.make_ws_handler(table_name="ob_diff", batch_size=50)
    fallback_start = datetime.now(timezone.utc) - timedelta(days=fallback_days_back)

    async def _snapshot_action():
        ob_dict = await RestSpot.fetch_orderbook(underlying, limit=5000)
        if ob_dict:
            db.insert_many("ob_snapshot", [(
                ob_dict["timestamp"], ob_dict["lastUpdateId"],
                ob_dict["bids"], ob_dict["asks"]
            )])

    tasks = [
        asyncio.create_task(WSspot.stream_diff_ob(underlying, ob_diff_handler), name="ws_spot_ob_diff"),
        asyncio.create_task(RoutineManager.periodic_snapshot_loop(
            db, _snapshot_action, snapshot_interval_min
        ), name="rest_spot_snapshot"),
        asyncio.create_task(RoutineManager.vision_daily_sync_loop(
            underlying=underlying, db=db, fallback_start_date=fallback_start, 
            sync_func=RestSpot.sync_day_vision, tmp_dir=TMP_DIR / underlying
        ), name="vision_spot_trades"),
        asyncio.create_task(db.auto_flush_loop(interval=5), name="db_auto_flush"),
    ]

    lifecycle = WorkerLifecycle(db, RestSpot, "Spot Worker", underlying)
    await lifecycle.run(tasks)
    
if __name__ == "__main__":
    SYMBOL = os.getenv("UNDERLYING", "BTC")
    asyncio.run(run_spot_worker(underlying=SYMBOL))