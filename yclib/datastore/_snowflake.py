"""Module for handling connecting/querying snowflake"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
from os import environ
from typing import Generator

import pandas as pd
import snowflake
from dotenv import load_dotenv
from snowflake.connector.connection import SnowflakeConnection, SnowflakeCursor

from yc_silt.datastores import (
    ConnectionContext,
    ConnectionStatus,
    DataStore,
    NotConnectedError,
)


@dataclass
class SnowflakeCredentials:
    """Handles credentials to log into snowflake"""

    user: str | None
    account: str | None
    password: str | None
    role: str | None
    database: str | None
    schema: str | None
    warehouse: str | None

    @staticmethod
    def from_env() -> SnowflakeCredentials:
        """Load credentials from an .env file or environment variables"""
        load_dotenv()
        return SnowflakeCredentials(
            user=environ.get("SNOWFLAKE_USER"),
            account=environ.get("SNOWFLAKE_ACCOUNT"),
            password=environ.get("SNOWFLAKE_PASSWORD"),
            role=environ.get("SNOWFLAKE_ROLE"),
            database=environ.get("SNOWFLAKE_DATABASE"),
            schema=environ.get("SNOWFLAKE_SCHEMA"),
            warehouse=environ.get("SNOWFLAKE_WAREHOUSE"),
        )

    def asdict(self) -> dict[str, str]:
        """Returns credentials as a dictionary - only including keys that were set"""
        d = asdict(self)
        return {k: v for k, v in d.items() if v is not None}


class Snowflake(DataStore):
    connection: SnowflakeConnection
    credentials: SnowflakeCredentials

    def __init__(self, credentials: SnowflakeCredentials):
        self.credentials = credentials

    @contextmanager
    def connect(self) -> ConnectionContext:
        """Context manager for a snowflake connection"""
        self.connection = snowflake.connector.connect(**self.credentials.asdict())
        try:
            yield
        finally:
            self.connection.close()

    def get_query_results(self, query: str) -> pd.DataFrame:
        """Runs the query, returning results as a dataframe"""

        with self.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetch_pandas_all()

    @property
    def status(self) -> ConnectionStatus:
        """Returns the status of the connection"""
        if self.connection is None or self.connection.is_closed():
            return ConnectionStatus.DISCONNECTED
        else:
            return ConnectionStatus.CONNECTED

    @contextmanager
    def cursor(self) -> Generator[SnowflakeCursor, None, None]:
        """Context manager to create a snowflake cursor"""

        if self.status != ConnectionStatus.CONNECTED:
            raise NotConnectedError

        cursor = self.connection.cursor()

        try:
            yield cursor
        finally:
            cursor.close()
