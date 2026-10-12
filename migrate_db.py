import os
from datetime import datetime, timezone
import duckdb

from markets.futures.futuresDB import futures_DB
from markets.spot.spotDB import Spot_DB
from markets.options.optionsDB import Options_DB


def table_has_rows(con: duckdb.DuckDBPyConnection, alias: str, table_name: str) -> int:
    query = f"""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_catalog = '{alias}' AND table_name = '{table_name}';
    """
    if con.execute(query).fetchone()[0] == 0:
        return 0
    return con.execute(f"SELECT COUNT(*) FROM {alias}.{table_name};").fetchone()[0]


def migrate_options(underlying: str, old_dir: str):
    """
    Migra los datos de saveOptions_2.py:
    - options (contratos)
    - griegas (convirtiendo timestamp de segundos a milisegundos)
    - oi
    - trades (convirtiendo id de INTEGER a BIGINT)
    """
    old_path = os.path.join(old_dir, underlying, "options_data.db")
    if not os.path.exists(old_path):
        print(f"[OPTIONS] No existe {old_path}, omitiendo.")
        return

    print(f"\n--- Migrando OPTIONS ({underlying}) desde {old_path} ---")
    db_new = Options_DB(underlying=underlying)
    con = db_new.con
    con.execute(f"ATTACH '{old_path}' AS old_opt (READ_ONLY);")

    try:
        # 1. Contratos (options)
        if table_has_rows(con, "old_opt", "options") > 0:
            before = con.execute("SELECT COUNT(*) FROM options;").fetchone()[0]
            con.execute("INSERT INTO options SELECT * FROM old_opt.options ON CONFLICT DO NOTHING;")
            after = con.execute("SELECT COUNT(*) FROM options;").fetchone()[0]
            print(f"  [OK] options.options: +{after - before:,} filas (Total: {after:,})")

        # 2. Griegas (Normalizando timestamp de segundos a milisegundos)
        if table_has_rows(con, "old_opt", "griegas") > 0:
            before = con.execute("SELECT COUNT(*) FROM griegas;").fetchone()[0]
            con.execute("""
                INSERT INTO griegas
                SELECT 
                    symbol, markPrice, bidIV, askIV, markIV,
                    delta, theta, gamma, vega,
                    highPriceLimit, lowPriceLimit, riskFreeInterest,
                    CASE WHEN timestamp < 10000000000 THEN timestamp * 1000 ELSE timestamp END AS timestamp
                FROM old_opt.griegas
                ON CONFLICT DO NOTHING;
            """)
            after = con.execute("SELECT COUNT(*) FROM griegas;").fetchone()[0]
            min_ts = con.execute("SELECT MIN(timestamp) FROM griegas;").fetchone()[0]
            t0_str = datetime.fromtimestamp(min_ts / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            print(f"  [OK] options.griegas: +{after - before:,} filas (Total: {after:,} | Nuevo T0: {t0_str})")

        # 3. Open Interest (oi)
        if table_has_rows(con, "old_opt", "oi") > 0:
            before = con.execute("SELECT COUNT(*) FROM oi;").fetchone()[0]
            con.execute("""
                INSERT INTO oi
                SELECT 
                    symbol,
                    CASE WHEN timestamp < 10000000000 THEN timestamp * 1000 ELSE timestamp END AS timestamp,
                    sumOpenInterest,
                    sumOpenInterestUsd
                FROM old_opt.oi
                ON CONFLICT DO NOTHING;
            """)
            after = con.execute("SELECT COUNT(*) FROM oi;").fetchone()[0]
            print(f"  [OK] options.oi: +{after - before:,} filas (Total: {after:,})")

        # 4. Trades de opciones (id INTEGER -> BIGINT)
        if table_has_rows(con, "old_opt", "trades") > 0:
            before = con.execute("SELECT COUNT(*) FROM trades;").fetchone()[0]
            con.execute("""
                INSERT INTO trades
                SELECT 
                    id::BIGINT,
                    CASE WHEN timestamp < 10000000000 THEN timestamp * 1000 ELSE timestamp END AS timestamp,
                    symbol, price, qty, quoteQty, side, block
                FROM old_opt.trades
                ON CONFLICT DO NOTHING;
            """)
            after = con.execute("SELECT COUNT(*) FROM trades;").fetchone()[0]
            print(f"  [OK] options.trades: +{after - before:,} filas (Total: {after:,})")

        con.execute("CHECKPOINT;")
    finally:
        con.execute("DETACH old_opt;")
        db_new.close()


def migrate_futures(underlying: str, old_dir: str):
    """
    Migra los datos de saveFutures.py:
    - liquidations (el stream principal que tenias activo)
    - Si existieran datos en trades o funding_rate de otro minero en ese .db, tambien los copia.
    """
    old_path = os.path.join(old_dir, underlying, "futures_data.db")
    if not os.path.exists(old_path):
        print(f"[FUTURES] No existe {old_path}, omitiendo.")
        return

    print(f"\n--- Migrando FUTURES ({underlying}) desde {old_path} ---")
    db_new = futures_DB(underlying=underlying)
    con = db_new.con
    con.execute(f"ATTACH '{old_path}' AS old_fut (READ_ONLY);")

    try:
        for table in ("liquidations", "trades", "funding_rate", "ob_diff", "ob_snapshot"):
            if table_has_rows(con, "old_fut", table) > 0:
                before = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
                con.execute(f"INSERT INTO {table} SELECT * FROM old_fut.{table} ON CONFLICT DO NOTHING;")
                after = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
                print(f"  [OK] futures.{table}: +{after - before:,} filas (Total: {after:,})")

        con.execute("CHECKPOINT;")
    finally:
        con.execute("DETACH old_fut;")
        db_new.close()


def migrate_spot(underlying: str, old_dir: str):
    """
    Migra los datos de saveSpot.py:
    - ob_snapshot (REST cada 30m)
    - ob_diff (WebSocket @depth)
    - trades (si hubiera alguno)
    """
    old_path = os.path.join(old_dir, underlying, "spot_data.db")
    if not os.path.exists(old_path):
        print(f"[SPOT] No existe {old_path}, omitiendo.")
        return

    print(f"\n--- Migrando SPOT ({underlying}) desde {old_path} ---")
    db_new = Spot_DB(symbol=underlying)
    con = db_new.con
    con.execute(f"ATTACH '{old_path}' AS old_spot (READ_ONLY);")

    try:
        for table in ("ob_snapshot", "ob_diff", "trades"):
            if table_has_rows(con, "old_spot", table) > 0:
                before = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
                con.execute(f"INSERT INTO {table} SELECT * FROM old_spot.{table} ON CONFLICT DO NOTHING;")
                after = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
                print(f"  [OK] spot.{table}: +{after - before:,} filas (Total: {after:,})")

        con.execute("CHECKPOINT;")
    finally:
        con.execute("DETACH old_spot;")
        db_new.close()


if __name__ == "__main__":
    OLD_DATA_DIR = "../options/data"
    UNDERLYING = "BTC"

    migrate_options(UNDERLYING, OLD_DATA_DIR)
    migrate_futures(UNDERLYING, OLD_DATA_DIR)
    migrate_spot(UNDERLYING, OLD_DATA_DIR)
    print("\nMigracion finalizada. Ahora puedes arrancar main_worker.py.")