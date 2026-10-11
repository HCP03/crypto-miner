import os
import sys
from tests.base_validator import DBValidator


def run_options_tests(underlying: str = "BTC") -> int:
    print("=" * 70)
    print(f"INICIANDO TESTS DE BASE DE DATOS: OPTIONS ({underlying})")
    print("=" * 70)

    v = DBValidator(underlying, "options")

    # 1. OPTIONS (Contratos activos)
    print("\n[1/5] Verificando tabla: options")
    if v.check_table_not_empty("options", required=True) > 0:
        v.check_no_nulls_and_valid_arrays("options")

    # 2. GRIEGAS (Snapshots REST cada 15 min = 900,000 ms -> tolerancia 20 min = 1,200,000 ms)
    print("\n[2/5] Verificando tabla: griegas")
    count_g = v.check_table_not_empty("griegas", required=True)
    if count_g > 0:
        v.check_no_nulls_and_valid_arrays("griegas")
        v.check_time_gaps("griegas", max_gap_ms=1_200_000, partition_col="symbol")
        
    # 3. OI (Open Interest por contrato)
    print("\n[3/5] Verificando tabla: oi")
    if v.check_table_not_empty("oi", required=True) > 0:
        v.check_no_nulls_and_valid_arrays("oi")

    # 4. TOP_ORDERBOOK (WebSocket Superficie Activa L2 + Griegas enriquecidas)
    print("\n[4/5] Verificando tabla: top_orderbook (WebSocket L2)")
    if v.check_table_not_empty("top_orderbook", required=True) > 0:
        v.check_no_nulls_and_valid_arrays("top_orderbook")
        # Evaluamos gaps tanto a nivel global del stream como por contrato individual (max 120s de silencio por contrato liquido)
        v.check_time_gaps("top_orderbook", max_gap_ms=30_000)

    # 5. TRADES (WebSocket @optionTrade)
    print("\n[5/5] Verificando tabla: trades (WebSocket)")
    if v.check_table_not_empty("trades", required=False) > 0:
        v.check_no_nulls_and_valid_arrays("trades")
        v.check_side_distribution("trades", "side", strict=False)
        v.check_time_gaps("trades", max_gap_ms=300_000)

    return v.summary()


if __name__ == "__main__":
    symbol = os.getenv("UNDERLYING", "BTC")
    sys.exit(run_options_tests(symbol))