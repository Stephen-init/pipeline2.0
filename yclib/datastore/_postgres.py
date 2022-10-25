from dataclasses import dataclass
from psycopg2 import sql
from contextlib import contextmanager
import pandas as pd
from typing import Iterable, Optional, Literal
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, cursor, connection
from psycopg2.errors import DuplicateDatabase
from io import StringIO
import yclib.datastore as ds
from typing import Generator
import psycopg2
import re


@dataclass
class Postgres(ds.DataStore):
    """Execute Postgres commands"""

    DB_CREDENTIAL: dict | None

    def __init__(self, DB_CREDENTIAL: dict):
        self.credentials = DB_CREDENTIAL

    @contextmanager
    def connect(self) -> connection:
        """Context manager for a postgres connection"""
        self.connection = psycopg2.connect(**self.credentials)
        try:
            yield
        finally:
            self.connection.close()

    @contextmanager
    def cursor(self) -> Generator[cursor, None, None]:
        """Context manager to create a postgres cursor"""

        if self.status != ds.ConnectionStatus.CONNECTED:
            raise ds.NotConnectedError

        cursor = self.connection.cursor()

        try:
            yield cursor
        finally:
            cursor.close()

    @property
    def status(self) -> ds.ConnectionStatus:
        """Returns the status of the connection"""
        if self.connection is None or self.connection.closed == 1:
            return ds.ConnectionStatus.DISCONNECTED
        else:
            return ds.ConnectionStatus.CONNECTED

    @staticmethod
    def align_datatype(
        df: Optional[pd.DataFrame] = None,
        column_with_dtype: Optional[dict[str, str]] = None,
        output_dtypes: Literal["postgres", "pandas"] = "postgres",
    ) -> dict[str, str]:
        df_postgres_dtype_mapping = {
            "object": "text",
            "int": "INT",
            "float": "numeric",
            "bool": "boolean",
            "datetime": "timestamp",
            "timedelta": "interval",
            "UUID": "UUID",
        }

        def _match_string(target):
            match = re.match(r"([a-z]+)([0-9]+)", target, re.I)
            if match:
                return match.groups()[0]
            else:
                return target

        if output_dtypes == "pandas":
            df_postgres_dtype_mapping = {
                val: key for key, val in df_postgres_dtype_mapping.items()
            }
        if df is not None:
            column_with_dtype = {
                key: _match_string(val)
                for key, val in df.dtypes.astype(str).to_dict().items()
            }

        return {
            key: df_postgres_dtype_mapping[val]
            for key, val in column_with_dtype.items()
        }

    def create_database(self, dbname: str):
        with self.cursor() as cursor:
            try:
                self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cursor.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname))
                )
                message = f"database {dbname} is created"
            except DuplicateDatabase:
                message = f"database {dbname} already exists"
            return message

    def create_schema(self, schema: str):
        with self.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            self.connection.commit()
        return f"schema {schema} is created"

    def create_table(
        self,
        schema: str,
        table: str,
        column_with_dtype: Optional[dict[str, str]] = None,
    ):
        with self.cursor() as cursor:
            query = (
                """CREATE TABLE IF NOT EXISTS {schema}.{table} ({columns});""".format(
                    schema=f'"{schema}"',
                    table=f'"{table}"',
                    columns=",".join(
                        [f'"{key}" {val}' for key, val in column_with_dtype.items()]
                    ),
                )
            )
            cursor.execute(query)
            self.connection.commit()
            return f"table {table} are created with structure {column_with_dtype}"

    def create_column(
        self,
        schema: str,
        table: str,
        column_with_dtype: Optional[dict[str, str]] = None,
    ):
        with self.cursor() as cursor:
            query = sql.SQL("select * from {table} where false;").format(
                table=sql.Identifier(table)
            )
            cursor.execute(query)
            colnames = [desc[0] for desc in cursor.description]
            column_with_dtype = {
                key: val
                for key, val in column_with_dtype.items()
                if key not in colnames
            }
            for col, dt in column_with_dtype.items():
                query = sql.SQL(
                    "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS %s;"
                ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))
            cursor.execute(query, (f"{col} {dt}",))
            self.connection.commit()
            return f"additional columns {str(column_with_dtype)} are created in table {table}"

    def dataframe_insert_to_table(self, schema: str, table: str, df: pd.DataFrame):
        with self.cursor() as cursor:
            buffer = StringIO()
            df.to_csv(buffer, index=False)
            buffer.seek(0)
            cursor.copy_expert(
                f"""COPY "{schema}"."{table}" FROM STDIN (FORMAT 'csv', HEADER True)""",
                buffer,
            )
            self.connection.commit()
            return f'{df.shape[0]} records are inserted into "{schema}.{table}"'

    def query_to_dataframe(self, query: str) -> pd.DataFrame:
        with self.cursor() as cursor:
            cursor.execute(query)
            data = cursor.fetchall()
            df = pd.DataFrame(data=data, columns=[col[0] for col in cursor.description])
            return df

    def inspect_table(self, schema: str, table: str) -> dict[str, str]:
        with self.cursor() as cursor:
            query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '{schema}' and table_name = '{table}';"
            cursor.execute(query)
            result = {rcd[0]: rcd[1] for rcd in cursor.fetchall()}
            return result

    def inspect_schema_existance(self, schema: str) -> list:
        with self.cursor() as cursor:
            query = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema}';"
            cursor.execute(query)
            result = [rcd for rcd in cursor.fetchall()]
            return result

    def inspect_table_existance(self, schema: str, table: str) -> list:
        with self.cursor() as cursor:
            query = f"SELECT * FROM information_schema.tables WHERE table_schema = '{schema}' and table_name = '{table}';"
            cursor.execute(query)
            result = [rcd for rcd in cursor.fetchall()]
            return result

    def inspect_table_shape(self, schema: str, table: str) -> dict[str, str]:
        with self.cursor() as cursor:
            query = f"""SELECT count(*) FROM {schema}.{table}
                        Union
                        SELECT count(column_name) FROM information_schema.columns WHERE table_schema = '{schema}' and table_name = '{table}'"""
            cursor.execute(query)
            result = cursor.fetchall()
            return {"row": result[0][0], "column": result[1][0]}
