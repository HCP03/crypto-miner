import os
import sys
from tests.base_validator import DBValidator


def run_futures_tests(underlying: str = "BTC") -> int:
    print("=" * 70)
    print(f"INICIANDO TESTS DE BASE DE DATOS: FUTURES ({underlying})")
    print("=" * 70)

    v = DBValidator(underlying, "futures")

    # 1. TRADES (Binance Vision)
    print("\n[1/5] Verificando tabla: trades")
    if v.check_table_not_empty("trades", required=True) > 0:
        v.check_no_nulls_and_valid_arrays("trades")
        v.check_side_distribution("trades", "taker", strict=True)
        v.check_time_gaps("trades", max_gap_ms=60_000)

    # 2. OPEN INTEREST (Binance Vision Metrics - granularidad 5 min)
    print("\n[2/5] Verificando tabla: open_interest")
    if v.check_table_not_empty("open_interest", required=True) > 0:
        v.check_no_nulls_and_valid_arrays("open_interest")
        v.check_time_gaps("open_interest", max_gap_ms=600_000)

    # 3. FUNDING RATE (Cada 8 horas = 28,800,000 ms)
    print("\n[3/5] Verificando tabla: funding_rate")
    if v.check_table_not_empty("funding_rate", required=True) > 0:
        v.check_no_nulls_and_valid_arrays("funding_rate")
        v.check_time_gaps("funding_rate", max_gap_ms=29_700_000)

    # 4. OB_DEPTH (Binance Vision BookDepth)
    print("\n[4/5] Verificando tabla: ob_depth")
    if v.check_table_not_empty("ob_depth", required=True) > 0:
        v.check_no_nulls_and_valid_arrays("ob_depth")
        v.check_time_gaps("ob_depth", max_gap_ms=300_000)

    # 5. LIQUIDATIONS (WebSocket en tiempo real durante 15 min)
    print("\n[5/5] Verificando tabla: liquidations (WebSocket)")
    # En 15 minutos puede haber pocas liquidaciones; required=False evita falso fallo si el mercado no liquido a nadie
    if v.check_table_not_empty("liquidations", required=False) > 0:
        v.check_no_nulls_and_valid_arrays("liquidations")
        v.check_side_distribution("liquidations", "side", strict=False)
        v.check_time_gaps("liquidations", max_gap_ms=900_000)

    return v.summary()


if __name__ == "__main__":
    symbol = os.getenv("UNDERLYING", "BTC")
    sys.exit(run_futures_tests(symbol))