import os
import re
from pathlib import Path
import duckdb

from markets.futures.futuresDB import futures_DB
from markets.spot.spotDB import Spot_DB
from markets.options.optionsDB import Options_DB

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
DERIBIT_OPTIONS_TABLES = {"top_orderbook", "options", "oi", "trades", "griegas"}


def _read_expr(fpath: Path) -> str:
    safe_path = str(fpath.resolve()).replace("'", "''")
    if fpath.name.endswith(".parquet"):
        return f"read_parquet('{safe_path}')"
    return f"read_json_auto('{safe_path}')"


def _strip_ext(fname: str) -> str:
    for ext in (".jsonl.gz", ".parquet", ".jsonl"):
        if fname.endswith(ext):
            return fname[: -len(ext)]
    return Path(fname).stem


def get_file_columns(con: duckdb.DuckDBPyConnection, fpath: Path) -> set[str]:
    rows = con.execute(f"DESCRIBE SELECT * FROM {_read_expr(fpath)};").fetchall()
    return {r[0] for r in rows}


def resolve_target(fname: str, cols: set[str]) -> tuple[str | None, str | None]:
    stem = _strip_ext(fname)

    m = re.match(
        r"^[A-Za-z0-9]+_(futures|options|spot)_(.+?)_\d{8}(?:_\d{6})?$",
        stem,
        re.IGNORECASE,
    )
    if m:
        return m.group(1).lower(), m.group(2).lower()

    m2 = re.match(r"^[A-Za-z0-9]+_(.+?)_\d{8}(?:_\d{6})?$", stem, re.IGNORECASE)
    if not m2:
        return None, None

    table = m2.group(1).lower()
    if table in TABLE_TO_DB:
        return TABLE_TO_DB[table], table

    if table == "trades":
        if {"symbol", "side"}.issubset(cols):
            return "options", "trades"
        if "taker" in cols:
            return "futures", "trades"

    if table == "ob_diff":
        if "prevUpdateId" in cols:
            return "futures", "ob_diff"
        if "firstUpdateId" in cols:
            return "spot", "ob_diff"

    if table == "ob_snapshot":
        return "futures", "ob_snapshot"

    return None, None


def _collect_files(folder: Path, underlying: str) -> list[Path]:
    found = []
    for pattern in (f"{underlying}_*.parquet", f"{underlying}_*.jsonl.gz"):
        found.extend(folder.glob(pattern))
    return sorted(found)


def import_exchange(exchange: str, underlying: str = "BTC", import_dir: str = "imports/asia"):
    os.environ["DATA_DIR"] = "data"
    os.environ["EXCHANGE"] = exchange.lower()

    folder = Path(import_dir)
    files = _collect_files(folder, underlying)
    if not files:
        print(f"No hay archivos pendientes para {exchange}/{underlying} en {folder}")
        return

    if exchange.lower() == "deribit":
        dbs = {"options": Options_DB(underlying=underlying)}
    else:
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
                print(f"[SKIP] Archivo vacio (0 bytes): {fname}")
                continue

            try:
                src_cols = get_file_columns(inspect_con, fpath)
            except Exception as e:
                print(f"[SKIP] Archivo invalido/corrupto ({fname}): {e}")
                continue

            db_type, table_name = resolve_target(fname, src_cols)
            if exchange.lower() == "deribit":
                db_type = "options" if table_name in DERIBIT_OPTIONS_TABLES else None

            db_inst = dbs.get(db_type) if db_type else None
            if not db_inst or not table_name:
                print(f"[AVISO] No se pudo mapear {fname} (cols={sorted(src_cols)})")
                continue

            try:
                target_cols = [
                    r[0] for r in db_inst.con.execute(f"DESCRIBE {table_name};").fetchall()
                ]
                common_cols = [c for c in target_cols if c in src_cols]
                if not common_cols:
                    print(f"[AVISO] Sin columnas compatibles en {fname} para {table_name}")
                    continue

                cols_sql = ", ".join(f'"{c}"' for c in common_cols)
                before = db_inst.con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
                db_inst.con.execute(
                    f"""
                    INSERT INTO {table_name} ({cols_sql})
                    SELECT {cols_sql} FROM {_read_expr(fpath)}
                    ON CONFLICT DO NOTHING;
                    """
                )
                after = db_inst.con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
                print(f"[IMPORT {exchange.upper()}] {db_type}.{table_name}: +{after - before:,} filas desde {fname}")
            except Exception as e:
                print(f"[ERROR] Fallo al ingestar {fname} en {exchange}.{db_type}.{table_name}: {e}")
    finally:
        inspect_con.close()
        for d in dbs.values():
            d.close()


if __name__ == "__main__":
    import_exchange("binance", underlying="BTC", import_dir="imports/asia")
    import_exchange("deribit", underlying="BTC", import_dir="imports/usa")
