import duckdb as db
from core.db import DB_Manager

class Spot_DB(DB_Manager):
    def __init__(self, symbol: str):
        super().__init__(symbol, db_type="spot")

    def create_tables(self):
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
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS ob_diff (
                timestamp BIGINT,
                lastUpdateId BIGINT,
                firstUpdateId BIGINT,
                bids DOUBLE[][],
                asks DOUBLE[][],
                PRIMARY KEY (lastUpdateId)
            );"""
        )

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