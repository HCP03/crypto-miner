import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
import duckdb

RAW_DIR = Path("./imports/raw")
PROCESSED_DIR = Path("./imports/processed")
LOCAL_DB_PATH = Path("./data/local_warehouse.duckdb")


def infer_table_name(parquet_path: Path) -> str:
    """
    Intenta inferir un nombre limpio de tabla a partir del nombre del archivo.
    Ejemplos:
      spot_BTC_trades_20261120.parquet -> spot_trades
      BTC_liquidations_20261120.parquet -> liquidations
      deribit_ETH_mark_price_20261120.parquet -> deribit_mark_price
    Si no encaja con un patrón conocido, usa el stem limpio sin fecha.
    """
    stem = parquet_path.stem
    parts = stem.split("_")

    # Quita fecha final si tiene 8 dígitos (YYYYMMDD)
    if parts and parts[-1].isdigit() and len(parts[-1]) == 8:
        parts = parts[:-1]

    # Quita símbolos típicos si aparecen sueltos
    cleaned = [p for p in parts if p.upper() not in {"BTC", "ETH", "SOL", "USDT", "USD"}]
    return "_".join(cleaned).lower() if cleaned else stem.lower()


def ensure_table_from_parquet(con: duckdb.DuckDBPyConnection, table_name: str, parquet_file: Path):
    """Crea la tabla en DuckDB con el mismo esquema del Parquet si aún no existe."""
    safe_path = str(parquet_file.resolve()).replace("'", "''")
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" AS
        SELECT * FROM read_parquet('{safe_path}') WHERE 1=0;
        """
    )


def insert_parquet_idempotent(con: duckdb.DuckDBPyConnection, table_name: str, parquet_file: Path):
    """
    Inserta filas del Parquet evitando duplicados exactos si la tabla ya tenía datos.
    """
    safe_path = str(parquet_file.resolve()).replace("'", "''")
    count_before = con.execute(f'SELECT COUNT(*) FROM "{table_name}";').fetchone()[0]

    if count_before == 0:
        con.execute(
            f"""
            INSERT INTO "{table_name}"
            SELECT * FROM read_parquet('{safe_path}');
            """
        )
    else:
        # Evita reinsertar filas idénticas si se re-procesa un archivo por error
        con.execute(
            f"""
            INSERT INTO "{table_name}"
            SELECT src.*
            FROM read_parquet('{safe_path}') AS src
            WHERE NOT EXISTS (
                SELECT 1
                FROM "{table_name}" AS dst
                WHERE dst IS NOT DISTINCT FROM src
            );
            """
        )

    count_after = con.execute(f'SELECT COUNT(*) FROM "{table_name}";').fetchone()[0]
    return count_after - count_before


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    LOCAL_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    parquet_files = sorted(RAW_DIR.rglob("*.parquet"))
    if not parquet_files:
        print("No hay archivos .parquet pendientes en ./imports/raw.")
        return

    run_day = datetime.now(timezone.utc).strftime("%Y%m%d")
    con = duckdb.connect(str(LOCAL_DB_PATH))

    try:
        for pq_file in parquet_files:
            table_name = infer_table_name(pq_file)
            print(f"Procesando {pq_file} -> tabla '{table_name}'...")

            ensure_table_from_parquet(con, table_name, pq_file)
            inserted = insert_parquet_idempotent(con, table_name, pq_file)
            print(f"  ✔ Filas nuevas insertadas: {inserted}")

            # Mover a processed conservando subcarpeta (asia/usa)
            rel_path = pq_file.relative_to(RAW_DIR)
            dest_file = PROCESSED_DIR / run_day / rel_path
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(pq_file), str(dest_file))

        # Limpiar carpetas vacías dentro de raw
        for root, dirs, _ in os.walk(RAW_DIR, topdown=False):
            for d in dirs:
                p = Path(root) / d
                if not any(p.iterdir()):
                    p.rmdir()

    finally:
        con.close()

    print("\nIngesta finalizada en:", LOCAL_DB_PATH)


if __name__ == "__main__":
    main()