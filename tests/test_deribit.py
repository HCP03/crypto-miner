import os
import sys
from tests.base_validator import DBValidator


def run_deribit_tests(underlying: str = "BTC") -> int:
    print("=" * 70)
    print(f"INICIANDO TESTS DE BASE DE DATOS: DERIBIT OPTIONS ({underlying})")
    print("=" * 70)

    # Inyectamos "deribit" como exchange para que DBValidator busque en la ruta correcta
    os.environ["EXCHANGE"] = "deribit"
    
    try:
        v = DBValidator(underlying, "options")
    except FileNotFoundError as e:
        print(f"\n[ERROR] {e}")
        print("Asegurate de haber dejado corriendo el worker de Deribit para generar la DB.")
        return 1

    # 1. OPTIONS (Contratos de Deribit)
    print("\n[1/5] Verificando tabla: options")
    if v.check_table_not_empty("options", required=True) > 0:
        v.check_no_nulls_and_valid_arrays("options")

    # 2. GRIEGAS (Ingresadas desde el WS Ticker 100ms L1)
    print("\n[2/5] Verificando tabla: griegas")
    count_g = v.check_table_not_empty("griegas", required=True)
    if count_g > 0:
        v.check_no_nulls_and_valid_arrays("griegas")
        # En Deribit el WS inyecta griegas constantemente. Permitimos hasta 60s sin trades/cambios
        v.check_time_gaps("griegas", max_gap_ms=60_000, partition_col="symbol")
        
    # 3. OI (Ingresadas desde el WS Ticker 100ms L1)
    print("\n[3/5] Verificando tabla: oi")
    if v.check_table_not_empty("oi", required=True) > 0:
        v.check_no_nulls_and_valid_arrays("oi")

    # 4. TOP_ORDERBOOK (Superficie activa desde el WS Ticker 100ms L1)
    print("\n[4/5] Verificando tabla: top_orderbook (Deribit L1)")
    if v.check_table_not_empty("top_orderbook", required=True) > 0:
        v.check_no_nulls_and_valid_arrays("top_orderbook")
        v.check_time_gaps("top_orderbook", max_gap_ms=60_000, partition_col="symbol")

    # 5. TRADES (Canal WS trades.option)
    print("\n[5/5] Verificando tabla: trades")
    if v.check_table_not_empty("trades", required=False) > 0:
        v.check_no_nulls_and_valid_arrays("trades")
        # En Deribit mapeamos 'taker' a 1 (buy) y -1 (sell) en wsDeribit.py
        v.check_side_distribution("trades", "taker", strict=False)
        v.check_time_gaps("trades", max_gap_ms=300_000)

    return v.summary()


if __name__ == "__main__":
    symbol = os.getenv("UNDERLYING", "BTC")
    sys.exit(run_deribit_tests(symbol))