import duckdb
import os
import glob
from datetime import datetime

# Define the paths as they appear inside the Docker container's mapped volume
DB_PATHS = [
    "data/binance/BTC/spot_data.db",
    "data/binance/BTC/futures_data.db",
    "data/binance/ETH/spot_data.db",
    "data/binance/ETH/futures_data.db"
]

os.makedirs("exports", exist_ok=True)
now_tag = datetime.now().strftime("%Y%m%d_%H%M")

for db_path in DB_PATHS:
    if not os.path.exists(db_path):
        continue
        
    print(f"Purging {db_path}...")
    con = duckdb.connect(db_path)
    
    try:
        # Get all data tables (ignoring internal logs)
        tables = [r[0] for r in con.execute("SHOW TABLES;").fetchall() if 'log' not in r[0]]
        
        for t in tables:
            out_file = f"exports/{os.path.basename(db_path).replace('.db', '')}_{t}_{now_tag}.parquet"
            
            # Export to Parquet with ZSTD compression (extremely efficient)
            con.execute(f"COPY {t} TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD);")
            
            # Delete data to clear RAM/Disk
            con.execute(f"DELETE FROM {t};")
            
        # VACUUM reclaims the physical disk space from the OS after a DELETE
        con.execute("VACUUM;")
        print(f"  -> Exported to {now_tag} and vacuumed.")
    finally:
        con.close()