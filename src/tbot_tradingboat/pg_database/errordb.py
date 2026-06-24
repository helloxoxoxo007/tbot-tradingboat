# -*- coding: utf-8 -*-
"""
TradingBoat © Copyright, Plusgenie Limited 2023. All Rights Reserved.
"""
from typing import List
from loguru import logger
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from tbot_tradingboat.pg_decoder.ib_api.tbot_api import get_timestamp
from tbot_tradingboat.utils.objects import ErrorDBInfo
from .tbot_db import TbotDatabase


class TbotErrorDB(TbotDatabase):
    """
    Manage order information which is output of placing order onto IB/TWS
    """

    def __init__(self):
        self.engine = None
        super().__init__(self.engine)

    def setup_connection(self, db_url: str):
        """
        Connect to PostgreSQL

        Args:
            db_url (str): SQLAlchemy connection URL,
                e.g. postgresql+psycopg2://user:pass@host:5432/dbname
        """
        try:
            self.engine = create_engine(db_url)
        except SQLAlchemyError as err:
            logger.error(f"{err}: {db_url}")
            raise
        sql_query = """
            CREATE TABLE IF NOT EXISTS TBOTERRORS (
                id BIGSERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
                reqid DOUBLE PRECISION,
                errcode INTEGER,
                symbol VARCHAR(32),
                errstr TEXT
            )
        """
        self._exec(sql_query)
        self.create_trigger("TBOTERRORS", "timestamp")
        logger.success("Connected to error database (PostgreSQL)")

    def insert(self, unique_ts: int, obj: ErrorDBInfo):
        """Insert error information into the table"""
        sql_query = """
            INSERT INTO TBOTERRORS (
                reqid,
                errcode,
                symbol,
                errstr
            ) VALUES (?, ?, ?, ?)
            """
        sql_data = (obj.reqId, obj.code, obj.ticker, obj.msg)
        self._exec(sql_query, sql_data)

    def display(self):
        """Display the Error table"""
        if self.engine is None:
            return
        sql_query = "SELECT * FROM TBOTERRORS ORDER BY timestamp DESC LIMIT 12"
        data_f = pd.read_sql_query(sql_query, self.engine)
        logger.trace("\n" + data_f.to_string())

    def find_error_by_uniquekey(self, unique: str) -> object:
        """
        Find an error by unique key
        """
        timestamp = get_timestamp(unique)
        logger.trace(f"find_order: {timestamp}")
        sql_query = "SELECT * FROM TBOTERRORS WHERE timestamp > ? ORDER BY timestamp DESC LIMIT 1"
        sql_data = (timestamp,)
        rows = self._exec(sql_query, sql_data)
        logger.trace(f"find_order| {timestamp}, num:{len(rows)}")
        return rows[0] if len(rows) > 0 else None

    def find_errors_by_uniquekey(self, unique: str) -> List[object]:
        """
        Find errors by unique key
        """
        timestamp = get_timestamp(unique)
        logger.trace(f"find_order: {timestamp}")
        sql_query = (
            "SELECT * FROM TBOTERRORS WHERE timestamp > ? ORDER BY timestamp DESC"
        )
        sql_data = (timestamp,)
        rows = self._exec(sql_query, sql_data)
        logger.trace(f"find_order| {timestamp}, num:{len(rows)}")
        return rows if len(rows) > 0 else []
