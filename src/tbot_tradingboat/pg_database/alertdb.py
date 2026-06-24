# -*- coding: utf-8 -*-
"""
TradingBoat © Copyright, Plusgenie Limited 2023. All Rights Reserved.
"""
from typing import List, Dict

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from tbot_tradingboat.pg_decoder.ib_api.tbot_api import get_timestamp
from tbot_tradingboat.utils.objects import (
    AlertDBInfo,
    OrderKey,
)
from .tbot_db import TbotDatabase


class TbotAlertDB(TbotDatabase):
    """
    Define database for TradingView Webhook (Alerts)
    """

    def __init__(self):
        self.engine = None
        super().__init__(self.engine)

    def setup_connection(self, db_url):
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
            CREATE TABLE IF NOT EXISTS TBOTALERTS (
                id BIGSERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
                uniquekey VARCHAR(32),
                tv_timestamp VARCHAR(32),
                ticker VARCHAR(32),
                direction VARCHAR(32),
                timeframe VARCHAR(16),
                qty DOUBLE PRECISION,
                orderref VARCHAR(64),
                alertstatus VARCHAR(32),
                entrylimit DOUBLE PRECISION,
                entrystop DOUBLE PRECISION,
                exitlimit DOUBLE PRECISION,
                exitstop DOUBLE PRECISION,
                tv_price DOUBLE PRECISION
            )
        """
        self._exec(sql_query)

        # create an index on uniquekey
        sql_query = "CREATE INDEX IF NOT EXISTS alert_index ON TBOTALERTS(uniquekey);"
        self._exec(sql_query)

        self.create_trigger("TBOTALERTS", "uniquekey")
        logger.success("Connected to Alert Database (PostgreSQL)")

    def insert(self, unique_ts: str, obj: AlertDBInfo):
        sql_query = """
            INSERT INTO TBOTALERTS (
                uniquekey,
                tv_timestamp,
                ticker,
                direction,
                timeframe,
                qty,
                orderref,
                alertstatus,
                entrylimit,
                entrystop,
                exitlimit,
                exitstop,
                tv_price
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        uniq_t = get_timestamp(unique_ts)
        tv_t = get_timestamp(obj.timestamp)
        sql_data = (
            uniq_t,
            tv_t,
            obj.ticker,
            obj.direction,
            obj.timeframe,
            obj.qty,
            obj.orderRef,
            obj.alertStatus,
            obj.entryLimit,
            obj.entryStop,
            obj.exitLimit,
            obj.exitStop,
            obj.tv_price,
        )
        self._exec(sql_query, sql_data)

    def find_specified_orders(self, key: OrderKey, num: int) -> List[Dict]:
        """
        Find N specified orders in the order table.
        """
        if self.engine:
            logger.debug(f"find_specified_orders: {key.symbol}, {key.orderRef}")
            sql_query = (
                "SELECT * FROM TBOTALERTS WHERE (ticker=? and orderref=?) "
                "ORDER BY uniquekey DESC LIMIT ?"
            )
            sql_data = (key.symbol, key.orderRef, num)
            rows = self._exec(sql_query, sql_data)
            if len(rows) > 0:
                logger.trace(f"ask:{num},got:{len(rows)}")
                return rows
        else:
            logger.error("find_specified_orders: database connection is not ready.")
        return []

    def find_specified_order(self, key: OrderKey) -> Dict:
        """
        Find the specified order in the order table.
        """
        rval = self.find_specified_orders(key, 1)
        return rval[0] if rval else {}

    def display(self):
        """Display the Alert table"""
        if self.engine is None:
            return
        sql_query = "SELECT * FROM TBOTALERTS ORDER BY uniquekey DESC LIMIT 12"
        data_f = pd.read_sql_query(sql_query, self.engine)
        logger.debug("\n" + data_f.to_string())
