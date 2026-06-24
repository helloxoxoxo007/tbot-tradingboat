# -*- coding: utf-8 -*-
"""
TradingBoat © Copyright, Plusgenie Limited 2023. All Rights Reserved.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any

from loguru import logger
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


class TbotDatabase(ABC):
    """
    Base class for the SQLAlchemy/PostgreSQL-backed databases used by Tbot
    """

    def __init__(self, engine: Engine = None):
        self.engine = engine
        self.last_rowcount = 0

    @abstractmethod
    def setup_connection(self, db_url: str):
        """Connect to the database

        Args:
            db_url (str): SQLAlchemy connection URL,
                e.g. postgresql+psycopg2://user:pass@host:5432/dbname
        """

    def _exec(self, sql_query, sql_data=None) -> List[Dict]:
        """
        Execute a query written with sqlite-style '?' placeholders and a
        positional tuple of values, translating it to SQLAlchemy named
        binds so existing callers don't need to change their call sites.
        """
        res = []
        if not self.engine:
            logger.error("Connection error: No connection available.")
            return None
        params = {}
        if sql_data:
            translated = []
            count = 0
            for char in sql_query:
                if char == "?":
                    key = f"p{count}"
                    translated.append(f":{key}")
                    params[key] = sql_data[count]
                    count += 1
                else:
                    translated.append(char)
            sql_query = "".join(translated)
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text(sql_query), params)
                self.last_rowcount = result.rowcount
                if result.returns_rows:
                    res = [dict(row) for row in result.mappings()]
        except SQLAlchemyError as err:
            logger.error(f"{err}: {sql_query}")
            raise
        return res

    def create_trigger(
        self, table_name: str, key: str, max_records: int = 3600
    ) -> bool:
        """
        Creates a Postgres function + trigger for the given table that
        deletes old records once the table grows past max_records, sampled
        every 64 inserts.

        :param table_name: The name of the table to create the trigger for
        :param key: The column used to determine "oldest" rows (order by DESC)
        :param max_records: The maximum number of records to keep in the table
        :return: True if the trigger was created successfully, False otherwise
        """
        fn_name = f"trig_trim_{table_name.lower()}"
        trig_name = f"trig_{table_name.lower()}"
        create_fn = f"""
        CREATE OR REPLACE FUNCTION {fn_name}() RETURNS trigger AS $$
        BEGIN
            IF NEW.id % 64 = 0 THEN
                DELETE FROM {table_name} WHERE {key} NOT IN (
                    SELECT {key} FROM {table_name}
                    ORDER BY {key} DESC LIMIT {max_records}
                );
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
        drop_trig = f"DROP TRIGGER IF EXISTS {trig_name} ON {table_name};"
        create_trig = (
            f"CREATE TRIGGER {trig_name} AFTER INSERT ON {table_name} "
            f"FOR EACH ROW EXECUTE FUNCTION {fn_name}();"
        )
        try:
            with self.engine.begin() as conn:
                conn.execute(text(create_fn))
                conn.execute(text(drop_trig))
                conn.execute(text(create_trig))
            logger.trace(f"Created trigger for {table_name} database")
            return True
        except SQLAlchemyError as err:
            logger.exception(err)
            return False

    @abstractmethod
    def insert(self, unique_ts: str, obj: Any):
        """Insert object into the table with the key"""

    @abstractmethod
    def display(self):
        """Display Table"""

    def close(self):
        """Dispose of the database engine and its connection pool"""
        if self.engine:
            self.engine.dispose()
