import duckdb as db
import os
import asyncio
from collections import defaultdict
from datetime import datetime


class DB_Manager:
    def __init__(self, underlying: str, db_type: str):
        self.DB_path = f'data/{underlying}/{db_type}_data.db'
        os.makedirs(f'data/{underlying}', exist_ok=True)
        self.con = db.connect(self.DB_path)
        self._buffers = defaultdict(list)
        self._init_control_tables()
        self.create_tables()
    
    def _init_control_tables(self):
        """Tabla interna para registrar que dias de Binance Vision ya fueron procesados."""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS vision_sync_log (
                table_name VARCHAR,
                date_str VARCHAR,
                synced_at BIGINT,
                PRIMARY KEY (table_name, date_str)
            );
        """)

    def create_tables(self):
        pass

    def execute_query(self, query: str):
        """Ejecuta una consulta SQL directa (ideal para ingesta masiva con read_csv)."""
        self.con.execute(query)

    def insert_many(self, table_name: str, data_list: list[tuple]):
        if not data_list:
            return
        num_columns = len(data_list[0])
        placeholders = ", ".join(["?"] * num_columns)

        query = f"INSERT INTO {table_name} VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
        self.con.executemany(query, data_list)

    def get_last_ts(self, table_name: str):
        result = self.con.execute(f"SELECT MAX(timestamp) FROM {table_name};").fetchone()
        return result[0] if result else None

    # ==========================================================
    #        CONTROL DE INGESTA DIARIA (BINANCE VISION)
    # ==========================================================
    def is_day_ingested(self, table_name: str, date_obj: datetime) -> bool:
        date_str = date_obj.strftime("%Y-%m-%d")
        query = "SELECT 1 FROM vision_sync_log WHERE table_name = ? AND date_str = ? LIMIT 1;"
        return self.con.execute(query, [table_name, date_str]).fetchone() is not None

    def mark_day_ingested(self, table_name: str, date_obj: datetime):
        date_str = date_obj.strftime("%Y-%m-%d")
        now_ms = int(datetime.now().timestamp() * 1000)
        self.con.execute(
            "INSERT INTO vision_sync_log VALUES (?, ?, ?) ON CONFLICT DO NOTHING;",
            [table_name, date_str, now_ms]
        )

    # ==========================================================
    #        BUFFER PARA WEBSOCKETS (SOLO TUPLAS NATIVAS)
    # ==========================================================
    def make_ws_handler(self, table_name: str, batch_size: int = 50):
        """Genera un callback para WSClient.fetch que acumula tuplas y las inserta con insert_many."""
        def _handler(parsed_data):
            if parsed_data is not None:
                if isinstance(parsed_data, list):
                    self._buffers[table_name].extend(parsed_data)
                else:
                    self._buffers[table_name].append(parsed_data)
                if len(self._buffers[table_name]) >= batch_size:
                    self.flush_table(table_name)
        return _handler

    def flush_table(self, table_name: str):
        if self._buffers[table_name]:
            batch = self._buffers[table_name][:]
            self._buffers[table_name].clear()
            self.insert_many(table_name, batch)

    def flush_all(self):
        for table_name in list(self._buffers.keys()):
            self.flush_table(table_name)

    async def auto_flush_loop(self, interval: int = 5):
        cycles = 0
        while True:
            await asyncio.sleep(interval)
            self.flush_all()
            cycles += 1
            if cycles >= 180:  # Cada 15 min: CHECKPOINT
                try:
                    self.con.execute("CHECKPOINT;")
                except Exception:
                    pass
                cycles = 0

            # Si creas un archivo o carpeta llamada 'trigger_export', exporta en caliente sin parar los WS
            trigger_file = f"data/{self.underlying}/EXPORT_{self.db_type.upper()}"
            if os.path.exists(trigger_file):
                try:
                    os.remove(trigger_file)
                    self.export_incremental_parquet(export_dir=f"exports/{self.underlying}")
                except Exception as e:
                    print(f"[EXPORT ERROR] {e}", flush=True)

    def export_incremental_parquet(self, export_dir: str = "exports"):
        """
        Exporta en caliente (sin parar los WebSockets) todas las filas nuevas de cada tabla
        a archivos Parquet comprimidos con ZSTD, guardando el ultimo timestamp exportado.
        """
        os.makedirs(export_dir, exist_ok=True)
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS export_log (
                table_name VARCHAR PRIMARY KEY,
                last_exported_ts BIGINT
            );
        """)

        tables = [
            r[0] for r in self.con.execute("SHOW TABLES;").fetchall()
            if r[0] not in ("vision_sync_log", "export_log")
        ]

        now_tag = datetime.now().strftime("%Y%m%d_%H%M%S")

        for table in tables:
            cols = [r[0] for r in self.con.execute(f"DESCRIBE {table};").fetchall()]
            out_file = os.path.join(export_dir, f"{self.underlying}_{self.db_type}_{table}_{now_tag}.parquet")

            # Si la tabla no tiene columna timestamp (ej. options.options), la exporta entera (es muy pequena)
            if "timestamp" not in cols:
                self.con.execute(f"COPY {table} TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD);")
                continue

            res = self.con.execute("SELECT last_exported_ts FROM export_log WHERE table_name = ?;", [table]).fetchone()
            last_ts = res[0] if res else 0
            max_ts = self.get_last_ts(table)

            if max_ts and max_ts > last_ts:
                self.con.execute(f"""
                    COPY (
                        SELECT * FROM {table} 
                        WHERE timestamp > {last_ts} AND timestamp <= {max_ts}
                    ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD);
                """)
                self.con.execute(
                    "INSERT OR REPLACE INTO export_log VALUES (?, ?);",
                    [table, max_ts]
                )
                print(f"[EXPORT] {self.db_type}.{table} exportado ({last_ts} -> {max_ts}) -> {out_file}", flush=True)

    def close(self):
        self.flush_all()
        self.con.close()