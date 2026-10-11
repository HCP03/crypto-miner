from core.db import DB_Manager

class Options_DB(DB_Manager):
    def __init__(self, underlying: str):
        super().__init__(underlying, db_type="options")
    
    def create_tables(self):
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS top_orderbook (
                symbol VARCHAR,
                timestamp BIGINT,
                updateId BIGINT,
                bids DOUBLE[][],
                asks DOUBLE[][],
                bid_iv DOUBLE,
                ask_iv DOUBLE,
                mark_iv DOUBLE,
                mark_price DOUBLE,
                underlying_price DOUBLE,
                delta DOUBLE,
                gamma DOUBLE,
                vega DOUBLE,
                theta DOUBLE,
                open_interest DOUBLE,
                volume_24h DOUBLE,
                PRIMARY KEY (symbol, timestamp)
            );"""
        )
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS options (
                symbol VARCHAR PRIMARY KEY,
                expiration_ts BIGINT,
                expiration_date VARCHAR,
                strike_price DOUBLE,
                option_type VARCHAR
            );"""
        )
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS oi (
                symbol VARCHAR,
                timestamp BIGINT,
                sumOpenInterest DOUBLE,
                sumOpenInterestUsd DOUBLE,
                PRIMARY KEY (symbol, timestamp)
            );"""
        )
        # SIDE desde el TAKER: SIDE = 1 is a market buy | SIDE = -1 is a market sell
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id BIGINT,
                timestamp BIGINT,
                symbol VARCHAR,
                price DOUBLE,
                qty DOUBLE,
                quoteQty DOUBLE,
                side INTEGER, 
                block VARCHAR,
                PRIMARY KEY (symbol, timestamp, price, qty, side)
            );"""
        )
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS griegas (
                symbol VARCHAR,
                markPrice DOUBLE,
                bidIV DOUBLE,
                askIV DOUBLE,
                markIV DOUBLE,
                delta DOUBLE,
                theta DOUBLE,
                gamma DOUBLE,
                vega DOUBLE,
                highPriceLimit DOUBLE,
                lowPriceLimit DOUBLE,
                riskFreeInterest DOUBLE,
                timestamp BIGINT,
                PRIMARY KEY (symbol, timestamp)
            );"""
        )

    def delete_by_expiration(self, table_name: str, expiration: str):
        query = f"DELETE FROM {table_name} WHERE expiration_date = ?;"
        self.con.execute(query, [expiration])

    def get_sym(self, table_name: str) -> list[str]:
        query = f"SELECT DISTINCT symbol FROM {table_name};"
        result = self.con.execute(query).fetchall()
        return [row[0] for row in result]