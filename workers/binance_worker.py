import asyncio
import calendar
import os
from datetime import datetime, timezone, date

from core.writer import StreamWriter
from core.lifecycle import WorkerLifecycle
from core.logger import get_logger
from core.restClient import RestClient
from core.routines import RoutineManager

from markets.futures.wsFutures import WSFutures
from markets.spot.restSpot import RestSpot
from markets.spot.wsSpot import WSspot
from markets.options.restOptions import RestOptions
from markets.options.wsOptions import WSOptions


def _env_flag(name: str, default: bool = True) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


class _CompositeWriter:
    """Agrupa varios StreamWriter para gestionarlos juntos en WorkerLifecycle."""
    def __init__(self, writers: list[StreamWriter], underlying: str):
        self.writers = writers
        self.log = get_logger(f"BINANCE-{underlying}")

    def close(self):
        for w in self.writers:
            w.close()


# =====================================================================
#  OPTIONS: CACHÉ DE SUPERFICIE EN MEMORIA (SIN DUCKDB)
# =====================================================================
class SurfaceStateCache:
    def __init__(self):
        self.contracts: dict[str, dict] = {}
        self.metrics: dict[str, dict] = {}
        self.fast_symbols: set[str] = set()
        self.last_saved_ts: dict[str, int] = {}
        self.last_tob: dict[str, tuple] = {}

    def update_from_rest(
        self,
        contracts_list: list[dict],
        griegas_list: list[dict],
        oi_list: list[dict],
        tickers_list: list[dict],
    ):
        for c in contracts_list:
            self.contracts[c["symbol"]] = {
                "expiration_date": c["expiration_date"],
                "option_type": c["option_type"],
            }
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

    def make_enriched_ob_handler(self, writer_ob_handler):
        def _handler(parsed_ob: tuple):
            if not parsed_ob:
                return
            symbol, ts_ms, update_id, bids, asks = parsed_ob
            if not bids or not asks:
                return

            last_ts = self.last_saved_ts.get(symbol, 0)
            elapsed_ms = ts_ms - last_ts

            current_tob = (bids[0][0], bids[0][1], asks[0][0], asks[0][1])
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
            writer_ob_handler(enriched_row)

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


def select_active_liquidity_surface(
    cache: SurfaceStateCache, underlying: str
) -> tuple[list[str], set[str]]:
    prefix = f"{underlying}-"
    rows = []
    for symbol, meta in cache.contracts.items():
        if not symbol.startswith(prefix):
            continue
        delta = cache.metrics.get(symbol, {}).get("delta")
        if delta is None:
            continue
        rows.append((symbol, meta["expiration_date"], meta["option_type"], abs(float(delta))))

    if not rows:
        return [], set()

    all_exps = [r[1] for r in rows]
    target_exps, short_dte_exps, structural_exps = classify_target_expirations(all_exps)

    selected_symbols = []
    fast_symbols = set()

    for symbol, exp_date, opt_type, abs_delta in rows:
        if exp_date not in target_exps:
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
            if opt_type == "P" and exp_date in structural_exps:
                selected_symbols.append(symbol)

    print(
        f"[Superficie {underlying}] {len(target_exps)} vencimientos | "
        f"{len(selected_symbols)} contratos activos (Fast: {len(fast_symbols)})",
        flush=True,
    )
    return selected_symbols, fast_symbols


async def execute_options_snapshot(
    writer: StreamWriter, underlying: str, cache: SurfaceStateCache
):
    raw_contracts = await RestOptions.fetch_contracts()
    if not raw_contracts:
        return

    filtered_contracts = [c for c in raw_contracts if c["symbol"].startswith(f"{underlying}-")]
    if not filtered_contracts:
        return

    writer.insert_many(
        "options",
        [
            (c["symbol"], c["expiration_ts"], c["expiration_date"], c["strike_price"], c["option_type"])
            for c in filtered_contracts
        ],
    )

    raw_griegas = await RestOptions.fetch_griegas()
    filtered_griegas = [g for g in (raw_griegas or []) if g["symbol"].startswith(f"{underlying}-")]
    if filtered_griegas:
        writer.insert_many(
            "griegas",
            [
                (
                    g["symbol"], g["markPrice"], g["bidIV"], g["askIV"], g["markIV"],
                    g["delta"], g["theta"], g["gamma"], g["vega"],
                    g["highPriceLimit"], g["lowPriceLimit"], g["riskFreeInterest"], g["timestamp"],
                )
                for g in filtered_griegas
            ],
        )

    expirations = list({c["expiration_date"] for c in filtered_contracts})
    respuestas_oi = await asyncio.gather(
        *(RestOptions.fetch_open_interest(underlying, exp) for exp in expirations),
        return_exceptions=True,
    )

    filtered_oi = []
    oi_tuples = []
    for sublist in respuestas_oi:
        if isinstance(sublist, list):
            for item in sublist:
                filtered_oi.append(item)
                oi_tuples.append((
                    item["symbol"], item["timestamp"],
                    item["sumOpenInterest"], item["sumOpenInterestUsd"],
                ))
    if oi_tuples:
        writer.insert_many("oi", oi_tuples)

    raw_tickers = await RestOptions.fetch_tickers_24h()
    filtered_tickers = [t for t in (raw_tickers or []) if t["symbol"].startswith(f"{underlying}-")]

    cache.update_from_rest(filtered_contracts, filtered_griegas, filtered_oi, filtered_tickers)


async def manage_options_orderbook_ws(
    cache: SurfaceStateCache,
    underlying: str,
    enriched_ob_handler,
    contracts_ready: asyncio.Event,
    rebalance_minutes: int = 30,
):
    await contracts_ready.wait()
    while True:
        symbols, fast_syms = select_active_liquidity_surface(cache, underlying)
        cache.fast_symbols = fast_syms

        if not symbols:
            print(f"[Options OB {underlying}] Superficie vacia. Reintentando en 15s...", flush=True)
            await asyncio.sleep(15)
            continue

        ws_task = asyncio.create_task(
            WSOptions.stream_contracts_book_depth(
                symbols=symbols,
                handler_callback=enriched_ob_handler,
                updateSpeed="100ms",
                chunk_size=25,
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


# =====================================================================
#  WORKER UNIFICADO BINANCE (LIVE ONLY)
# =====================================================================
async def run_binance_worker(
    underlying: str = "BTC",
    enable_futures: bool = True,
    enable_spot: bool = True,
    enable_options: bool = True,
    spot_snapshot_min: int = 30,
    options_snapshot_min: int = 15,
    export_dir: str = "exports",
):
    os.environ["EXCHANGE"] = "binance"
    underlying = underlying.upper().replace("USDT", "")
    print(f"Iniciando Binance Live Worker para {underlying}...", flush=True)

    writers: list[StreamWriter] = []
    tasks: list[asyncio.Task] = []

    # 1. FUTURES (Liquidaciones en vivo)
    if enable_futures:
        fut_writer = StreamWriter(underlying=underlying, db_type="futures", export_dir=export_dir)
        writers.append(fut_writer)
        liq_handler = fut_writer.make_ws_handler("liquidations")
        tasks.extend([
            asyncio.create_task(
                WSFutures.stream_liquidations(underlying, liq_handler),
                name="binance_ws_fut_liquidations",
            ),
            asyncio.create_task(fut_writer.auto_flush_loop(interval=5), name="flush_futures"),
        ])

    # 2. SPOT (Diff OB + Snapshot OB periódico)
    if enable_spot:
        spot_writer = StreamWriter(underlying=underlying, db_type="spot", export_dir=export_dir)
        writers.append(spot_writer)
        ob_diff_handler = spot_writer.make_ws_handler("ob_diff")

        async def _spot_snapshot():
            ob_dict = await RestSpot.fetch_orderbook(underlying, limit=5000)
            if ob_dict:
                spot_writer.insert_many(
                    "ob_snapshot",
                    [(ob_dict["timestamp"], ob_dict["lastUpdateId"], ob_dict["bids"], ob_dict["asks"])],
                )

        tasks.extend([
            asyncio.create_task(
                WSspot.stream_diff_ob(underlying, ob_diff_handler),
                name="binance_ws_spot_ob_diff",
            ),
            asyncio.create_task(
                RoutineManager.periodic_snapshot_loop(spot_writer, _spot_snapshot, spot_snapshot_min),
                name="binance_rest_spot_snapshot",
            ),
            asyncio.create_task(spot_writer.auto_flush_loop(interval=5), name="flush_spot"),
        ])

    # 3. OPTIONS (Contratos/Griegas/OI REST + Griegas WS + Trades WS + Surface OB WS)
    if enable_options:
        opt_writer = StreamWriter(underlying=underlying, db_type="options", export_dir=export_dir)
        writers.append(opt_writer)
        cache = SurfaceStateCache()
        contracts_ready = asyncio.Event()

        async def _options_snapshot():
            await execute_options_snapshot(opt_writer, underlying, cache)
            if not contracts_ready.is_set():
                contracts_ready.set()

        trades_handler = opt_writer.make_ws_handler("trades")
        raw_ob_handler = opt_writer.make_ws_handler("top_orderbook")
        enriched_ob_handler = cache.make_enriched_ob_handler(raw_ob_handler)

        tasks.extend([
            asyncio.create_task(
                RoutineManager.periodic_snapshot_loop(opt_writer, _options_snapshot, options_snapshot_min),
                name="binance_rest_opt_snapshot",
            ),
            asyncio.create_task(
                WSOptions.stream_griegas(underlying, cache.update_from_ws_griegas),
                name="binance_ws_opt_griegas",
            ),
            asyncio.create_task(
                WSOptions.stream_option_trades(underlying, trades_handler),
                name="binance_ws_opt_trades",
            ),
            asyncio.create_task(
                manage_options_orderbook_ws(cache, underlying, enriched_ob_handler, contracts_ready, rebalance_minutes=30),
                name="binance_ws_opt_surface_ob",
            ),
            asyncio.create_task(opt_writer.auto_flush_loop(interval=5), name="flush_options"),
        ])

    if not tasks:
        print("[Binance Worker] No hay ningun mercado habilitado. Saliendo.", flush=True)
        return

    composite = _CompositeWriter(writers, underlying)
    lifecycle = WorkerLifecycle(composite, RestClient, "Binance Live Worker", underlying)
    await lifecycle.run(tasks)


if __name__ == "__main__":
    SYMBOL = os.getenv("UNDERLYING", "BTC")
    asyncio.run(
        run_binance_worker(
            underlying=SYMBOL,
            enable_futures=_env_flag("ENABLE_FUTURES", True),
            enable_spot=_env_flag("ENABLE_SPOT", True),
            enable_options=_env_flag("ENABLE_OPTIONS", True),
        )
    )