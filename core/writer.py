import asyncio
import atexit
import gzip
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from core.logger import get_logger

SCHEMAS = {
    "options": {
        "top_orderbook": [
            "symbol", "timestamp", "updateId", "bids", "asks",
            "bid_iv", "ask_iv", "mark_iv", "mark_price", "underlying_price",
            "delta", "gamma", "vega", "theta", "open_interest", "volume_24h",
        ],
        "options": [
            "symbol", "expiration_ts", "expiration_date", "strike_price", "option_type",
        ],
        "oi": ["symbol", "timestamp", "sumOpenInterest", "sumOpenInterestUsd"],
        "trades": ["id", "timestamp", "symbol", "price", "qty", "quoteQty", "side", "block"],
        "griegas": [
            "symbol", "markPrice", "bidIV", "askIV", "markIV",
            "delta", "theta", "gamma", "vega",
            "highPriceLimit", "lowPriceLimit", "riskFreeInterest", "timestamp",
        ],
    },
    "futures": {
        "liquidations": ["side", "order_type", "price", "average_price", "qty", "filled_qty", "timestamp"],
        "open_interest": ["openInterest", "timestamp", "oi_value", "ls_ratio_global", "ls_ratio_top_acc", "ls_ratio_top_pos"],
        "funding_rate": ["lastFundingRate", "timestamp"],
        "trades": ["price", "qty", "quoteQty", "timestamp", "taker"],
        "ob_diff": ["timestamp", "prevUpdateId", "finalUpdateId", "bids", "asks"],
        "ob_snapshot": ["timestamp", "lastUpdateId", "bids", "asks"],
        "ob_depth": ["timestamp", "percentage", "depth", "notional"],
    },
    "spot": {
        "ob_snapshot": ["timestamp", "lastUpdateId", "bids", "asks"],
        "ob_diff": ["timestamp", "lastUpdateId", "firstUpdateId", "bids", "asks"],
        "trades": ["price", "qty", "quoteQty", "timestamp", "taker"],
    },
}


class StreamWriter:
    def __init__(
        self,
        underlying: str,
        db_type: str,
        export_dir: str = "exports",
        flush_interval_sec: int = 900,
        max_rows_per_file: int = 25000,
    ):
        self.underlying = underlying.upper()
        self.db_type = db_type.lower()
        self.exchange = os.getenv("EXCHANGE", "binance").lower()
        self.export_dir = export_dir
        self.flush_interval_sec = flush_interval_sec
        self.max_rows_per_file = max_rows_per_file

        self.log = get_logger(f"{self.db_type.upper()}-{self.exchange.upper()}-{self.underlying}")
        os.makedirs(self.export_dir, exist_ok=True)

        self._buffers = defaultdict(list)
        self._minute_counts = defaultdict(int)
        self._last_flush_ts = time.monotonic()
        self._closed = False
        atexit.register(self.close)

    def insert_many(self, table_name: str, data_list: list[tuple]):
        if not data_list:
            return
        self._buffers[table_name].extend(data_list)
        self._minute_counts[table_name] += len(data_list)
        if len(self._buffers[table_name]) >= self.max_rows_per_file:
            self.flush_table(table_name)

    def make_ws_handler(self, table_name: str, batch_size: int = 50):
        def _handler(parsed_data):
            if parsed_data is None:
                return
            if isinstance(parsed_data, list):
                self._buffers[table_name].extend(parsed_data)
                self._minute_counts[table_name] += len(parsed_data)
            else:
                self._buffers[table_name].append(parsed_data)
                self._minute_counts[table_name] += 1

            if len(self._buffers[table_name]) >= self.max_rows_per_file:
                self.flush_table(table_name)
        return _handler

    def flush_table(self, table_name: str):
        rows = self._buffers.get(table_name)
        if not rows:
            return

        batch = rows[:]
        rows.clear()

        cols = SCHEMAS.get(self.db_type, {}).get(table_name)
        ts_tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        fname = f"{self.underlying}_{self.db_type}_{table_name}_{ts_tag}.jsonl.gz"
        final_path = os.path.join(self.export_dir, fname)
        tmp_path = final_path + ".tmp"

        try:
            with gzip.open(tmp_path, "wt", encoding="utf-8", compresslevel=6) as f:
                if cols:
                    for row in batch:
                        f.write(json.dumps(dict(zip(cols, row)), separators=(",", ":")) + "\n")
                else:
                    for row in batch:
                        f.write(json.dumps(row, separators=(",", ":")) + "\n")

            os.replace(tmp_path, final_path)
            self.log.info(f"[FLUSH] {self.db_type}.{table_name}: {len(batch):,} filas -> {fname}")
        except Exception as e:
            self.log.error(f"Error volcando {table_name}: {e}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def flush_all(self):
        for table_name in list(self._buffers.keys()):
            self.flush_table(table_name)
        self._last_flush_ts = time.monotonic()

    async def auto_flush_loop(self, interval: int = 5):
        cycles = 0
        while True:
            await asyncio.sleep(interval)
            cycles += 1

            if cycles % 12 == 0:
                if self._minute_counts:
                    stats = " | ".join(f"{t}: +{c:,}" for t, c in sorted(self._minute_counts.items()))
                    self.log.info(f"STATUS 1m -> {stats}")
                    self._minute_counts.clear()
                else:
                    self.log.info("STATUS 1m -> En espera de nuevos eventos (0 filas en el ultimo min)")

            if (time.monotonic() - self._last_flush_ts) >= self.flush_interval_sec:
                self.flush_all()

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self.flush_all()
            self.log.info("Buffers volcados a disco y writer cerrado correctamente.")
        except Exception as e:
            self.log.error(f"Error cerrando StreamWriter: {e}")
