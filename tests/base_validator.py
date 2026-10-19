import os
import duckdb


class DBValidator:
    def __init__(self, underlying: str, db_type: str):
        self.underlying = underlying.upper()
        self.db_type = db_type
        
        self.exchange = os.getenv("EXCHANGE", "binance").lower()
        self.data_dir = os.getenv("DATA_DIR", "data")
        
        self.db_path = f"{self.data_dir}/{self.exchange}/{self.underlying}/{self.db_type}_data.db"
        
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"No existe la base de datos: {self.db_path}")
            
        self.con = duckdb.connect(self.db_path, read_only=True)
        self.failures = 0
        self.warnings = 0

    def log_pass(self, msg: str):
        print(f"  [PASS] {msg}")

    def log_fail(self, msg: str):
        print(f"  [FAIL] {msg}")
        self.failures += 1

    def log_warn(self, msg: str):
        print(f"  [WARN] {msg}")
        self.warnings += 1

    def check_table_not_empty(self, table_name: str, required: bool = True) -> int:
        try:
            count = self.con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
        except Exception as e:
            self.log_fail(f"{table_name}: Error al consultar la tabla ({e})")
            return 0

        if count > 0:
            self.log_pass(f"{table_name}: Contiene {count:,} filas.")
        else:
            if required:
                self.log_fail(f"{table_name}: La tabla esta VACIA (0 filas).")
            else:
                self.log_warn(f"{table_name}: Tabla vacia (0 filas) - omitiendo resto de checks en esta tabla.")
        return count

    def check_no_nulls_and_valid_arrays(self, table_name: str):
        cols_info = self.con.execute(f"PRAGMA table_info('{table_name}');").fetchall()
        if not cols_info:
            self.log_fail(f"{table_name}: No se pudo obtener el esquema de columnas.")
            return

        null_conditions = []
        array_cols = []
        for col in cols_info:
            col_name = col[1]
            col_type = col[2].upper()
            null_conditions.append(f"SUM(CASE WHEN \"{col_name}\" IS NULL THEN 1 ELSE 0 END) AS \"{col_name}\"")
            if "[]" in col_type:
                array_cols.append(col_name)

        query = f"SELECT {', '.join(null_conditions)} FROM {table_name};"
        res = self.con.execute(query).fetchone()

        null_found = False
        for idx, col in enumerate(cols_info):
            col_name = col[1]
            null_count = res[idx] or 0
            if null_count > 0:
                self.log_fail(f"{table_name}.{col_name}: Contiene {null_count:,} valores NULL.")
                null_found = True

        if not null_found:
            self.log_pass(f"{table_name}: 0 valores NULL en sus {len(cols_info)} columnas.")

        for arr_col in array_cols:
            empty_arr_q = f"SELECT COUNT(*) FROM {table_name} WHERE len(\"{arr_col}\") = 0;"
            empty_count = self.con.execute(empty_arr_q).fetchone()[0]
            if empty_count > 0:
                self.log_warn(f"{table_name}.{arr_col}: Contiene {empty_count:,} filas con listas vacias [].")
            else:
                self.log_pass(f"{table_name}.{arr_col}: Todas las listas anidadas contienen datos.")

    def check_side_distribution(self, table_name: str, column_name: str, strict: bool = True):
        query = f"""
            SELECT "{column_name}", COUNT(*) 
            FROM {table_name} 
            GROUP BY "{column_name}" 
            ORDER BY "{column_name}";
        """
        rows = self.con.execute(query).fetchall()
        dist = {r[0]: r[1] for r in rows}
        keys = set(dist.keys())

        invalid_keys = keys - {-1, 1}
        if invalid_keys:
            self.log_fail(f"{table_name}.{column_name}: Valores invalidos detectados: {invalid_keys}. Distribucion: {dist}")
            return

        if -1 in keys and 1 in keys:
            self.log_pass(f"{table_name}.{column_name}: Ambos lados presentes (-1: {dist[-1]:,}, 1: {dist[1]:,}).")
        else:
            msg = f"{table_name}.{column_name}: Solo se encontro un lado en la muestra: {dist}."
            if strict:
                self.log_fail(msg)
            else:
                self.log_warn(msg + " (Posible falta de eventos de ambos lados en ventana de 15 min).")

    def check_time_gaps(self, table_name: str, max_gap_ms: int, partition_col: str = None):
        if partition_col:
            query = f"""
                WITH diffs AS (
                    SELECT 
                        "{partition_col}",
                        timestamp,
                        timestamp - LAG(timestamp) OVER (PARTITION BY "{partition_col}" ORDER BY timestamp) AS gap_ms
                    FROM {table_name}
                )
                SELECT 
                    COUNT(*) FILTER (WHERE gap_ms > {max_gap_ms}),
                    MAX(gap_ms),
                    MIN(timestamp),
                    MAX(timestamp)
                FROM diffs;
            """
        else:
            query = f"""
                WITH diffs AS (
                    SELECT 
                        timestamp,
                        timestamp - LAG(timestamp) OVER (ORDER BY timestamp) AS gap_ms
                    FROM {table_name}
                )
                SELECT 
                    COUNT(*) FILTER (WHERE gap_ms > {max_gap_ms}),
                    MAX(gap_ms),
                    MIN(timestamp),
                    MAX(timestamp)
                FROM diffs;
            """

        res = self.con.execute(query).fetchone()
        num_gaps, max_observed_gap, min_ts, max_ts = res

        if max_observed_gap is None:
            self.log_warn(f"{table_name}: Insuficientes puntos temporales para calcular gaps (se requieren >= 2 timestamps).")
            return

        span_sec = (max_ts - min_ts) / 1000.0
        max_gap_sec = max_observed_gap / 1000.0
        threshold_sec = max_gap_ms / 1000.0

        if num_gaps == 0:
            self.log_pass(
                f"{table_name}: Sin gaps mayores a {threshold_sec:.1f}s "
                f"(Max gap observado: {max_gap_sec:.2f}s | Rango cubierto: {span_sec:.1f}s)."
            )
        else:
            self.log_fail(
                f"{table_name}: Se detectaron {num_gaps:,} gaps mayores al limite de {threshold_sec:.1f}s "
                f"(Max gap observado: {max_gap_sec:.2f}s)."
            )

    def summary(self) -> int:
        print("-" * 70)
        print(f"RESUMEN [{self.db_type.upper()} - {self.underlying}]: {self.failures} Fallos | {self.warnings} Advertencias")
        print("-" * 70)
        self.con.close()
        return self.failures