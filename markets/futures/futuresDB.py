from core.db import DB_Manager

class futures_DB(DB_Manager):
    def __init__(self, underlying: str):
        super().__init__(underlying, db_type="futures")

    def create_tables(self):
        # 1. Liquidaciones (Soporta WS en tiempo real y CSV de Vision)
        # side: 1 (BUY), -1 (SELL)
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS liquidations(
                side INTEGER,
                order_type VARCHAR,
                price DOUBLE,
                average_price DOUBLE, 
                qty DOUBLE,
                filled_qty DOUBLE, 
                timestamp BIGINT,
                PRIMARY KEY(timestamp, side, price)
            );"""
        )

        # 2. Open Interest (Soporta fapi/v1/openInterest y el CSV metrics de Vision)
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS open_interest (
                openInterest DOUBLE,
                timestamp BIGINT,
                oi_value DOUBLE,
                ls_ratio_global DOUBLE,
                ls_ratio_top_acc DOUBLE,
                ls_ratio_top_pos DOUBLE,
                PRIMARY KEY (timestamp)
            );"""
        )
        
        # 3. Funding Rate
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS funding_rate (
                lastFundingRate DOUBLE,
                timestamp BIGINT,
                PRIMARY KEY (timestamp)
            );"""
        )
        
        # 4. Trades (taker: 1 si el comprador es taker, -1 si el vendedor es taker)
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                price DOUBLE,
                qty DOUBLE,
                quoteQty DOUBLE,
                timestamp BIGINT,
                taker INTEGER,
                PRIMARY KEY (timestamp, price, qty, taker)
            );"""
        )

        # 5. Orderbook Snapshots (para fapi/v1/depth) y Diffs (para WS)
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS ob_diff (
                timestamp BIGINT,
                prevUpdateId BIGINT,
                finalUpdateId BIGINT,
                bids DOUBLE[][],
                asks DOUBLE[][],
                PRIMARY KEY(timestamp, prevUpdateId, finalUpdateId)
            );"""
        )

        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS ob_snapshot (
                timestamp BIGINT,
                lastUpdateId BIGINT,
                bids DOUBLE[][],
                asks DOUBLE[][],
                PRIMARY KEY(timestamp, lastUpdateId)
            );"""
        )

        # 6. Orderbook BookDepth (Para los ZIPs históricos de Binance Vision)
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS ob_depth (
                timestamp BIGINT,
                percentage DOUBLE,
                depth DOUBLE,
                notional DOUBLE,
                PRIMARY KEY(timestamp, percentage)
            );"""
        )