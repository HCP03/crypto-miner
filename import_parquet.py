import os
import re
from pathlib import Path
import duckdb

from markets.futures.futuresDB import futures_DB
from markets.spot.spotDB import Spot_DB
from markets.options.optionsDB import Options_DB

DERIBIT_OPTIONS_TABLES = {"top_orderbook", "options", "oi", "trades", "griegas"}
os.environ["DATA_DIR"] = "data"
os.environ["EXCHANGE"] = "binance"

TABLE_TO_DB = {
    "liquidations": "futures",
    "open_interest": "futures",
    "funding_rate": "futures",
    "ob_depth": "futures",
    "top_orderbook": "options",
    "options": "options",
    "oi": "options",
    "griegas": "options",
}


def get_parquet_columns(con: duckdb.DuckDBPyConnection, fpath: Path) -> set[str]:
    safe_path = str(fpath.resolve()).replace("'", "''")
    rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{safe_path}');").fetchall()
    return {r[0] for r in rows}


def resolve_target(
    fname: str,
    pq_cols: set[str],
) -> tuple[str | None, str | None]:
    """
    Devuelve (db_type, table_name) a partir del nombre del archivo y sus columnas.
    Soporta:
      - BTC_futures_trades_20261120_120000.parquet
      - BTC_trades_20261116.parquet
    """
    stem = Path(fname).stem

    # 1) Formato explícito: <UNDERLYING>_<db_type>_<table>_<YYYYMMDD>[_<HHMMSS>]
    m = re.match(
        r"^[A-Za-z0-9]+_(futures|options|spot)_(.+?)_\d{8}(?:_\d{6})?$",
        stem,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).lower(), m.group(2).lower()

    # 2) Formato corto: <UNDERLYING>_<table>_<YYYYMMDD>[_<HHMMSS>]
    m2 = re.match(
        r"^[A-Za-z0-9]+_(.+?)_\d{8}(?:_\d{6})?$",
        stem,
        re.IGNORECASE,
    )
    if not m2:
        return None, None

    table = m2.group(1).lower()

    if table in TABLE_TO_DB:
        return TABLE_TO_DB[table], table

    # Desambiguación por columnas para tablas que existen en varias DBs
    if table == "trades":
        if {"symbol", "side"}.issubset(pq_cols):
            return "options", "trades"
        if "taker" in pq_cols:
            return "futures", "trades"

    if table == "ob_diff":
        if "prevUpdateId" in pq_cols:
            return "futures", "ob_diff"
        if "firstUpdateId" in pq_cols:
            return "spot", "ob_diff"

    if table == "ob_snapshot":
        return "futures", "ob_snapshot"

    return None, None


def import_parquets(underlying: str = "BTC", import_dir: str = "imports/asia"):
    folder = Path(import_dir)
    files = sorted(folder.glob(f"{underlying}_*.parquet"))

    if not files:
        print(f"No hay archivos parquet pendientes para {underlying} en {folder}")
        return

    dbs = {
        "options": Options_DB(underlying=underlying),
        "futures": futures_DB(underlying=underlying),
        "spot": Spot_DB(symbol=underlying),
    }

    inspect_con = duckdb.connect()

    try:
        for fpath in files:
            fname = fpath.name

            if fpath.stat().st_size == 0:
                print(f"[SKIP] Archivo vacío (0 bytes): {fname}")
                continue

            try:
                pq_cols = get_parquet_columns(inspect_con, fpath)
            except duckdb.InvalidInputException as e:
                print(f"[SKIP] Parquet inválido/corrupto ({fname}): {e}")
                continue

            db_type, table_name = resolve_target(fname, pq_cols)
            db_inst = dbs.get(db_type) if db_type else None

            if not db_inst or not table_name:
                print(f"[AVISO] No se pudo mapear {fname} (cols={sorted(pq_cols)})")
                continue

            safe_path = str(fpath.resolve()).replace("'", "''")

            try:
                target_cols = [
                    r[0]
                    for r in db_inst.con.execute(f"DESCRIBE {table_name};").fetchall()
                ]
                common_cols = [c for c in target_cols if c in pq_cols]
                cols_sql = ", ".join(f'"{c}"' for c in common_cols)

                before = db_inst.con.execute(
                    f"SELECT COUNT(*) FROM {table_name};"
                ).fetchone()[0]

                db_inst.con.execute(
                    f"""
                    INSERT INTO {table_name} ({cols_sql})
                    SELECT {cols_sql} FROM read_parquet('{safe_path}')
                    ON CONFLICT DO NOTHING;
                    """
                )

                after = db_inst.con.execute(
                    f"SELECT COUNT(*) FROM {table_name};"
                ).fetchone()[0]

                print(
                    f"[IMPORT] {db_type}.{table_name}: +{after - before:,} filas nuevas desde {fname}"
                )
            except Exception as e:
                print(f"[ERROR] Fallo al ingestar {fname} en {db_type}.{table_name}: {e}")

    finally:
        inspect_con.close()
        for d in dbs.values():
            d.close()

def import_deribit_parquets(underlying: str = "BTC", import_dir: str = "imports/raw/usa"):
    # Fuerza el destino en data/deribit/<UNDERLYING>/options_data.db
    os.environ["DATA_DIR"] = "data"
    os.environ["EXCHANGE"] = "deribit"

    folder = Path(import_dir)
    files = sorted(folder.glob(f"{underlying}_*.parquet"))

    if not files:
        print(f"No hay archivos parquet pendientes de Deribit para {underlying} en {folder}")
        return

    db_inst = Options_DB(underlying=underlying)
    inspect_con = duckdb.connect()

    try:
        for fpath in files:
            fname = fpath.name

            if fpath.stat().st_size == 0:
                print(f"[SKIP] Archivo vacío (0 bytes): {fname}")
                continue

            try:
                pq_cols = get_parquet_columns(inspect_con, fpath)
            except duckdb.InvalidInputException as e:
                print(f"[SKIP] Parquet inválido/corrupto ({fname}): {e}")
                continue

            _, table_name = resolve_target(fname, pq_cols)
            if table_name not in DERIBIT_OPTIONS_TABLES:
                print(f"[AVISO] Tabla '{table_name}' no pertenece a Options_DB ({fname})")
                continue

            safe_path = str(fpath.resolve()).replace("'", "''")

            try:
                target_cols = [
                    r[0]
                    for r in db_inst.con.execute(f"DESCRIBE {table_name};").fetchall()
                ]
                common_cols = [c for c in target_cols if c in pq_cols]
                if not common_cols:
                    print(f"[AVISO] Sin columnas compatibles en {fname} para {table_name}")
                    continue

                cols_sql = ", ".join(f'"{c}"' for c in common_cols)

                before = db_inst.con.execute(
                    f"SELECT COUNT(*) FROM {table_name};"
                ).fetchone()[0]

                db_inst.con.execute(
                    f"""
                    INSERT INTO {table_name} ({cols_sql})
                    SELECT {cols_sql} FROM read_parquet('{safe_path}')
                    ON CONFLICT DO NOTHING;
                    """
                )

                after = db_inst.con.execute(
                    f"SELECT COUNT(*) FROM {table_name};"
                ).fetchone()[0]

                print(
                    f"[IMPORT DERIBIT] options.{table_name}: +{after - before:,} filas nuevas desde {fname}"
                )
            except Exception as e:
                print(f"[ERROR] Fallo al ingestar {fname} en deribit.options.{table_name}: {e}")

    finally:
        inspect_con.close()
        db_inst.close()

if __name__ == "__main__":
    #import_parquets(underlying="BTC", import_dir="imports/asia")
    import_deribit_parquets(underlying="BTC", import_dir="imports/raw/usa")