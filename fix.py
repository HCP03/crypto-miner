import duckdb
import os

for db_file in ["futures_data.db", "spot_data.db", "options_data.db"]:
    path = f"data/binance/ETH/{db_file}"
    if not os.path.exists(path):
        continue
        
    print(f"Reparando vision_sync_log en {db_file}...")
    con = duckdb.connect(path)
    try:
        # Renombramos la tabla sin clave primaria
        con.execute("ALTER TABLE vision_sync_log RENAME TO vision_sync_log_backup;")
        
        # La creamos con su PRIMARY KEY oficial
        con.execute("""
            CREATE TABLE vision_sync_log (
                table_name VARCHAR,
                date_str VARCHAR,
                synced_at BIGINT,
                PRIMARY KEY (table_name, date_str)
            );
        """)
        
        # Volcamos el historial de descargas y borramos el backup
        con.execute("INSERT INTO vision_sync_log SELECT * FROM vision_sync_log_backup;")
        con.execute("DROP TABLE vision_sync_log_backup;")
        con.execute("FORCE CHECKPOINT;")
        print(f"  [OK] Reparada con éxito.")
    except Exception as e:
        print(f"  [SKIP] No se pudo reparar (quizá ya estaba bien): {e}")
    finally:
        con.close()