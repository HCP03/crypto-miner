import asyncio
import calendar
import signal
import sys
import os
from datetime import datetime, timezone, date
from markets.options.optionsDB import Options_DB
from markets.options.restOptions import RestOptions
from markets.options.wsOptions import WSOptions


class SurfaceStateCache:
    def __init__(self):
        self.metrics: dict[str, dict] = {}
        self.fast_symbols: set[str] = set()
        self.last_saved_ts: dict[str, int] = {}
        self.last_tob: dict[str, tuple] = {}

    def update_from_rest(self, griegas_list: list[dict], oi_list: list[dict], tickers_list: list[dict]):
        for g in griegas_list:
            sym = g["symbol"]
            entry = self.metrics.setdefault(sym, {})
            entry.update({
                "mark_price": g["markPrice"],
                "bid_iv": g["bidIV"],
                "ask_iv": g["askIV"],
                "mark_iv": g["markIV"],
                "delta": g["delta"],
                "theta": g["theta"],
                "gamma": g["gamma"],
                "vega": g["vega"],
            })
        for o in oi_list:
            sym = o["symbol"]
            self.metrics.setdefault(sym, {})["open_interest"] = o["sumOpenInterest"]
        if tickers_list:
            for t in tickers_list:
                sym = t["symbol"]
                entry = self.metrics.setdefault(sym, {})
                entry["volume_24h"] = t["volume_24h"]
                if t["underlying_price"] > 0:
                    entry["underlying_price"] = t["underlying_price"]

    def update_from_ws_griegas(self, parsed_list: list[tuple]):
        if not parsed_list:
            return
        for row in parsed_list:
            sym = row[0]
            entry = self.metrics.setdefault(sym, {})
            entry.update({
                "mark_price": row[1],
                "bid_iv": row[2],
                "ask_iv": row[3],
                "mark_iv": row[4],
                "delta": row[5],
                "theta": row[6],
                "gamma": row[7],
                "vega": row[8],
            })

    def make_enriched_ob_handler(self, db_ob_handler):
        def _handler(parsed_ob: tuple):
            if not parsed_ob:
                return
            symbol, ts_ms, update_id, bids, asks = parsed_ob
            if not bids or not asks:
                return

            last_ts = self.last_saved_ts.get(symbol, 0)
            elapsed_ms = ts_ms - last_ts

            best_bid = (bids[0][0], bids[0][1])
            best_ask = (asks[0][0], asks[0][1])
            current_tob = (best_bid[0], best_bid[1], best_ask[0], best_ask[1])
            tob_changed = self.last_tob.get(symbol) != current_tob

            if symbol in self.fast_symbols:
                if elapsed_ms < 1000 and not (tob_changed and elapsed_ms >= 250):
                    return
            else:
                if elapsed_ms < 5000:
                    return

            self.last_saved_ts[symbol] = ts_ms
            self.last_tob[symbol] = current_tob

            m = self.metrics.get(symbol, {})
            enriched_row = (
                symbol,
                ts_ms,
                update_id,
                bids,
                asks,
                float(m.get("bid_iv", 0.0)),
                float(m.get("ask_iv", 0.0)),
                float(m.get("mark_iv", 0.0)),
                float(m.get("mark_price", 0.0)),
                float(m.get("underlying_price", 0.0)),
                float(m.get("delta", 0.0)),
                float(m.get("gamma", 0.0)),
                float(m.get("vega", 0.0)),
                float(m.get("theta", 0.0)),
                float(m.get("open_interest", 0.0)),
                float(m.get("volume_24h", 0.0)),
            )
            db_ob_handler(enriched_row)

        return _handler


def _is_last_friday_of_month(d: date) -> bool:
    if d.weekday() != 4:
        return False
    _, last_day = calendar.monthrange(d.year, d.month)
    return (last_day - d.day) < 7


def classify_target_expirations(exp_strings: list[str]) -> tuple[set[str], set[str], set[str]]:
    today = datetime.now(timezone.utc).date()
    parsed_dates: list[tuple[date, str]] = []

    for exp_str in set(exp_strings):
        try:
            d = datetime.strptime(exp_str, "%y%m%d").date()
            if d >= today:
                parsed_dates.append((d, exp_str))
        except ValueError:
            continue

    parsed_dates.sort(key=lambda x: x[0])
    if not parsed_dates:
        return set(), set(), set()

    short_dte_exps = {exp_str for d, exp_str in parsed_dates[:2] if (d - today).days <= 2}
    if not short_dte_exps:
        short_dte_exps = {parsed_dates[0][1]}

    fridays = [exp_str for d, exp_str in parsed_dates if d.weekday() == 4]
    weeklies = set(fridays[:2])

    monthlies = [exp_str for d, exp_str in parsed_dates if _is_last_friday_of_month(d)]
    front_month = set(monthlies[:1])

    quarterlies = [
        exp_str for d, exp_str in parsed_dates
        if _is_last_friday_of_month(d) and d.month in (3, 6, 9, 12)
    ]
    two_quarterlies = set(quarterlies[:2])

    structural_exps = front_month | two_quarterlies
    all_target_exps = short_dte_exps | weeklies | structural_exps
    return all_target_exps, short_dte_exps, structural_exps


def select_active_liquidity_surface(db: Options_DB, underlying: str) -> tuple[list[str], set[str]]:
    query = f"""
        SELECT 
            o.symbol,
            o.expiration_date,
            o.option_type,
            ABS(g.delta) AS abs_delta
        FROM options o
        JOIN griegas g ON o.symbol = g.symbol
        WHERE o.symbol LIKE '{underlying}-%'
          AND g.timestamp = (SELECT MAX(timestamp) FROM griegas WHERE symbol LIKE '{underlying}-%');
    """
    rows = db.con.execute(query).fetchall()
    if not rows:
        return [], set()

    all_exps = [r[1] for r in rows]
    target_exps, short_dte_exps, structural_exps = classify_target_expirations(all_exps)

    selected_symbols = []
    fast_symbols = set()

    for symbol, exp_date, opt_type, abs_delta in rows:
        if exp_date not in target_exps or abs_delta is None:
            continue
        if abs_delta > 0.65 or abs_delta < 0.03:
            continue

        if 0.35 <= abs_delta <= 0.65:
            selected_symbols.append(symbol)
            fast_symbols.add(symbol)
        elif 0.10 <= abs_delta < 0.35:
            selected_symbols.append(symbol)
            if exp_date in short_dte_exps:
                fast_symbols.add(symbol)
        elif 0.03 <= abs_delta < 0.10:
            if opt_type == 'P' and exp_date in structural_exps:
                selected_symbols.append(symbol)

    print(
        f"[Superficie Activa {underlying}] Vencimientos clave: {sorted(target_exps)} | "
        f"Contratos seleccionados: {len(selected_symbols)} (Rapidos 1s: {len(fast_symbols)}, Normales 5s: {len(selected_symbols) - len(fast_symbols)})"
    )
    return selected_symbols, fast_symbols


async def execute_single_options_snapshot(db: Options_DB, underlying: str, cache: SurfaceStateCache = None):
    """Realiza una descarga completa de Contratos, Griegas, OI y Tickers y los guarda en DB."""
    raw_contracts = await RestOptions.fetch_contracts()
    if not raw_contracts:
        return

    filtered_contracts = [c for c in raw_contracts if c["symbol"].startswith(f"{underlying}-")]
    if not filtered_contracts:
        return

    contract_tuples = [
        (c["symbol"], c["expiration_ts"], c["expiration_date"], c["strike_price"], c["option_type"])
        for c in filtered_contracts
    ]
    db.insert_many("options", contract_tuples)

    raw_griegas = await RestOptions.fetch_griegas()
    filtered_griegas = [g for g in (raw_griegas or []) if g["symbol"].startswith(f"{underlying}-")]
    if filtered_griegas:
        griegas_tuples = [
            (
                g["symbol"], g["markPrice"], g["bidIV"], g["askIV"], g["markIV"],
                g["delta"], g["theta"], g["gamma"], g["vega"],
                g["highPriceLimit"], g["lowPriceLimit"], g["riskFreeInterest"], g["timestamp"]
            )
            for g in filtered_griegas
        ]
        db.insert_many("griegas", griegas_tuples)

    expirations = list({c["expiration_date"] for c in filtered_contracts})
    tasks_oi = [RestOptions.fetch_open_interest(underlying, exp) for exp in expirations]
    respuestas_oi = await asyncio.gather(*tasks_oi, return_exceptions=True)
    
    filtered_oi = []
    oi_tuples = []
    for sublist in respuestas_oi:
        if isinstance(sublist, list):
            for item in sublist:
                filtered_oi.append(item)
                oi_tuples.append((
                    item["symbol"], item["timestamp"],
                    item["sumOpenInterest"], item["sumOpenInterestUsd"]
                ))
    if oi_tuples:
        db.insert_many("oi", oi_tuples)

    raw_tickers = await RestOptions.fetch_tickers_24h()
    filtered_tickers = [t for t in (raw_tickers or []) if t["symbol"].startswith(f"{underlying}-")]

    if cache is not None:
        cache.update_from_rest(filtered_griegas, filtered_oi, filtered_tickers)


async def run_initial_snapshot_and_close(underlying: str = "BTC"):
    """Modo --init-only: toma el primer snapshot de Griegas (T0) y cierra la DB para liberar el bloqueo."""
    underlying = underlying.upper().replace("USDT", "")
    print(f"[OPTIONS INIT] Guardando primer snapshot (T0) para {underlying}...")
    db = Options_DB(underlying=underlying)
    try:
        await execute_single_options_snapshot(db, underlying, cache=None)
        print(f"[OPTIONS INIT] Primer snapshot guardado. Cerrando options_data.db...")
    finally:
        db.close()
        await RestOptions.close_session()


async def snapshot_options_loop(
    db: Options_DB,
    cache: SurfaceStateCache,
    underlying: str = "BTC",
    t_minute: int = 15,
    active_contracts_event: asyncio.Event = None
):
    while True:
        try:
            print(f"[{underlying}] Actualizando snapshot REST de Opciones (Contratos, Griegas, OI, Ticker 24h)...")
            await execute_single_options_snapshot(db, underlying, cache=cache)
            if active_contracts_event and not active_contracts_event.is_set():
                active_contracts_event.set()
            print(f"[{underlying}] [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Snapshot REST y cache sincronizados.")
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Error en snapshot_options_loop: {e}")
            
        await asyncio.sleep(60 * t_minute)


async def manage_options_orderbook_ws(
    db: Options_DB,
    cache: SurfaceStateCache,
    underlying: str,
    enriched_ob_handler,
    contracts_ready: asyncio.Event,
    rebalance_minutes: int = 30
):
    await contracts_ready.wait()
    while True:
        symbols, fast_syms = select_active_liquidity_surface(db, underlying)
        cache.fast_symbols = fast_syms

        if not symbols:
            print(f"[Options OB] No se encontraron contratos en la superficie activa. Reintentando en 15s...")
            await asyncio.sleep(15)
            continue

        ws_task = asyncio.create_task(
            WSOptions.stream_contracts_book_depth(
                symbols=symbols,
                handler_callback=enriched_ob_handler,
                updateSpeed="100ms",
                chunk_size=25
            )
        )
        try:
            await asyncio.sleep(rebalance_minutes * 60)
        except asyncio.CancelledError:
            ws_task.cancel()
            break
        finally:
            if not ws_task.done():
                ws_task.cancel()


async def run_options_worker(underlying: str = "BTC", snapshot_interval_min: int = 15):
    underlying = underlying.upper().replace("USDT", "")
    print(f"Iniciando Options Worker Institucional para {underlying}...")

    db = Options_DB(underlying=underlying)
    cache = SurfaceStateCache()
    contracts_ready = asyncio.Event()

    trades_handler = db.make_ws_handler(table_name="trades", batch_size=100)
    raw_ob_db_handler = db.make_ws_handler(table_name="top_orderbook", batch_size=50)
    enriched_ob_handler = cache.make_enriched_ob_handler(raw_ob_db_handler)

    tasks = [
        asyncio.create_task(
            snapshot_options_loop(db, cache, underlying, t_minute=snapshot_interval_min, active_contracts_event=contracts_ready),
            name="rest_options_snapshot"
        ),
        asyncio.create_task(
            WSOptions.stream_griegas(underlying, cache.update_from_ws_griegas),
            name="ws_options_griegas_cache"
        ),
        asyncio.create_task(
            WSOptions.stream_option_trades(underlying, trades_handler),
            name="ws_options_trades"
        ),
        asyncio.create_task(
            manage_options_orderbook_ws(db, cache, underlying, enriched_ob_handler, contracts_ready, rebalance_minutes=30),
            name="ws_options_surface_ob"
        ),
        asyncio.create_task(db.auto_flush_loop(interval=5), name="db_auto_flush"),
    ]

    loop = asyncio.get_running_loop()
    def _signal_handler():
        print("\nSenal de parada recibida en Options Worker. Volcando buffers...")
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
        print("Guardando datos pendientes de Opciones en DuckDB...")
        db.close()
        await RestOptions.close_session()
        print(f"Options Worker ({underlying}) detenido correctamente.")


if __name__ == "__main__":
    SYMBOL = os.getenv("UNDERLYING", "BTC")
    try:
        if "--init-only" in sys.argv:
            asyncio.run(run_initial_snapshot_and_close(underlying=SYMBOL))
        else:
            asyncio.run(run_options_worker(underlying=SYMBOL, snapshot_interval_min=15))
    except KeyboardInterrupt:
        pass