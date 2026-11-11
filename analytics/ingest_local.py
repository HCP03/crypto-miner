import duckdb
import glob
import os

# Target local database
LOCAL_DB = "data/binance/BTC/spot_data.db"
con = duckdb.connect(LOCAL_DB)

# Find all locally synced Parquet files for this specific table
parquet_files = glob.glob("imports/spot_data_trades_*.parquet")

if parquet_files:
    print(f"Found {len(parquet_files)} trade files. Ingesting...")
    
    files_str = ", ".join([f"'{f}'" for f in parquet_files])
    
    # Ingest using ON CONFLICT DO NOTHING to prevent duplicates if a sync overlaps
    con.execute(f"""
        INSERT INTO trades
        SELECT * FROM read_parquet([{files_str}])
        ON CONFLICT DO NOTHING;
    """)
    
    print("Ingestion complete. Removing local Parquet files...")
    for f in parquet_files:
        os.remove(f)

con.close()