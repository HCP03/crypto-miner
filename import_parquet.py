import os
import glob
from markets.futures.futuresDB import futures_DB
from markets.spot.spotDB import Spot_DB
from markets.options.optionsDB import Options_DB

def import_parquets(underlying: str = "BTC", import_dir: str = "exports"):
    folder = os.path.join(import_dir, underlying)
    files = sorted(glob.glob(os.path.join(folder, "*.parquet")))
    if not files:
        print(f"No hay archivos parquet pendientes en {folder}")
        return

    dbs = {
        "options": Options_DB(underlying=underlying),
        "futures": futures_DB(underlying=underlying),
        "spot": Spot_DB(symbol=underlying),
    }

    try:
        for fpath in files:
            fname = os.path.basename(fpath)
            # Formato: BTC_{db_type}_{table}_{timestamp}.parquet
            parts = fname.replace(".parquet", "").split("_")
            db_type = parts[1]
            table_name = "_".join(parts[2:-2])

            db_inst = dbs.get(db_type)
            if db_inst:
                before = db_inst.con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
                db_inst.con.execute(f"""
                    INSERT INTO {table_name}
                    SELECT * FROM read_parquet('{fpath}')
                    ON CONFLICT DO NOTHING;
                """)
                after = db_inst.con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
                print(f"[IMPORT] {db_type}.{table_name}: +{after - before:,} filas desde {fname}")
                os.remove(fpath)
    finally:
        for d in dbs.values():
            d.close()

if __name__ == "__main__":
    import_parquets("BTC")