import asyncio
import calendar
import os
from datetime import datetime, timezone, date

from markets.options.optionsDB import Options_DB
from markets.deribit.restDeribit import RestDeribit
from markets.deribit.wsDeribit import WSDeribit
from core.wsClient import WSClient
from core.lifecycle import WorkerLifecycle
from core.routines import RoutineManager


def _is_last_friday_of_month(d: date) -> bool:
    if d.weekday() != 4:
        return False
    _, last_day = calendar.monthrange(d.year, d.month)
    return (last_day - d.day) < 7

async def select_active_liquidity_surface(underlying: str) -> list[str]:
    summary = await RestDeribit.fetch_book_summary(currency=underlying)
    if not summary:
        return []

    today = datetime.now(timezone.utc).date()
    parsed_exps = {}

    for item in summary:
        inst = item["instrument_name"]
        try:
            exp_str = inst.split('-')[1]
            if exp_str not in parsed_exps:
                d = datetime.strptime(exp_str, "%d%b%y").date()
                if d >= today:
                    parsed_exps[exp_str] = d
        except Exception:
            continue

    sorted_dates = sorted(parsed_exps.items(), key=lambda x: x[1])
    if not sorted_dates:
        return []

    short_dte_exps = {s for s, d in sorted_dates[:2] if (d - today).days <= 2}
    if not short_dte_exps:
        short_dte_exps = {sorted_dates[0][0]}

    fridays = [s for s, d in sorted_dates if d.weekday() == 4]
    weeklies = set(fridays[:2])

    monthlies = [s for s, d in sorted_dates if _is_last_friday_of_month(d)]
    front_month = set(monthlies[:1])

    quarterlies = [s for s, d in sorted_dates if _is_last_friday_of_month(d) and d.month in (3, 6, 9, 12)]
    two_quarterlies = set(quarterlies[:2])
    
    structural_exps = front_month | two_quarterlies
    target_exps = short_dte_exps | weeklies | structural_exps
    
    selected_symbols = []
    
    for item in summary:
        parts = item["instrument_name"].split('-')
        if len(parts) < 4: continue
        
        exp_str = parts[1]
        if exp_str not in target_exps: continue

        strike = float(parts[2])
        opt_type = parts[3]
        # Deribit envia el precio base como underlying_price o estimated_delivery_price
        under_price = item.get("underlying_price") or item.get("estimated_delivery_price")
        
        if not under_price or under_price <= 0:
            continue

        # Moneyness = Relacion entre Strike y Precio actual
        moneyness = strike / under_price

        # Opciones OTM y ATM (Equivalente aproximado a Deltas 0.10 - 0.65)
        # Calls (1.0 a 1.25) y Puts (0.75 a 1.0)
        if 0.75 <= moneyness <= 1.25:
            selected_symbols.append(item["instrument_name"])
            
        # Puts OTM profundas para Tail Risk (Deltas 0.03 - 0.10)
        elif 0.50 <= moneyness < 0.75 and opt_type == 'P' and exp_str in structural_exps:
            selected_symbols.append(item["instrument_name"])
            
        # Calls OTM lejanas (Deltas 0.03 - 0.10)
        elif 1.25 < moneyness <= 1.50 and opt_type == 'C' and exp_str in structural_exps:
            selected_symbols.append(item["instrument_name"])

    print(f"[Deribit {underlying}] {len(target_exps)} vencimientos | {len(selected_symbols)} contratos en superficie")
    return selected_symbols

async def manage_deribit_tickers_ws(underlying: str, ticker_router_handler, rebalance_minutes: int = 30):
    while True:
        symbols = await select_active_liquidity_surface(underlying)
        if not symbols:
            await asyncio.sleep(15)
            continue

        channels = [f"ticker.{sym}.100ms" for sym in symbols]
        ws_task = asyncio.create_task(
            WSClient.fetch(
                url=WSDeribit.WS_URL, 
                parser_callback=WSDeribit.parse_ticker_to_binance_schema, 
                handler_callback=ticker_router_handler, 
                subscribe_msg=WSDeribit.build_subscribe_msg(channels)
            )
        )
        try:
            await asyncio.sleep(rebalance_minutes * 60)
        except asyncio.CancelledError:
            ws_task.cancel()
            break
        finally:
            if not ws_task.done(): ws_task.cancel()

async def _update_contracts(db: Options_DB, underlying: str):
    contracts = await RestDeribit.fetch_contracts(currency=underlying)
    if not contracts: return

    tuples = []
    for c in contracts:
        exp_ts = int(c["expiration_timestamp"])
        exp_date = datetime.fromtimestamp(exp_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        tuples.append((c["instrument_name"], exp_ts, exp_date, float(c["strike"]), c["option_type"]))
    db.insert_many("options", tuples)

async def run_deribit_worker(underlying: str = "BTC", snapshot_interval_min: int = 60):
    underlying = underlying.upper()
    print(f"Iniciando Deribit Worker Institucional para {underlying}", flush=True)

    db = Options_DB(underlying=underlying)
    await _update_contracts(db, underlying)
    
    trades_handler = db.make_ws_handler("trades", batch_size=100)
    griegas_handler = db.make_ws_handler("griegas", batch_size=50)
    oi_handler = db.make_ws_handler("oi", batch_size=50)
    ob_handler = db.make_ws_handler("top_orderbook", batch_size=50)

    def ticker_router(parsed_dict: dict):
        if not parsed_dict: return
        griegas_handler(parsed_dict["griegas"])
        oi_handler(parsed_dict["oi"])
        ob_handler(parsed_dict["top_orderbook"])

    async def _snapshot_action():
        await _update_contracts(db, underlying)

    tasks = [
        asyncio.create_task(
            WSClient.fetch(
                url=WSDeribit.WS_URL, 
                parser_callback=WSDeribit.parse_trade, 
                handler_callback=trades_handler, 
                subscribe_msg=WSDeribit.build_subscribe_msg([f"trades.option.{underlying}.raw"])
            ), name="deribit_ws_trades"
        ),
        asyncio.create_task(
            manage_deribit_tickers_ws(underlying, ticker_router, rebalance_minutes=30),
            name="deribit_ws_surface"
        ),
        asyncio.create_task(
            RoutineManager.periodic_snapshot_loop(db, _snapshot_action, snapshot_interval_min), 
            name="deribit_rest_contracts"
        ),
        asyncio.create_task(db.auto_flush_loop(interval=5), name="db_auto_flush")
    ]

    lifecycle = WorkerLifecycle(db, RestDeribit, "Deribit Worker", underlying)
    await lifecycle.run(tasks)

if __name__ == "__main__":
    SYMBOL = os.getenv("UNDERLYING", "BTC")
    asyncio.run(run_deribit_worker(underlying=SYMBOL))