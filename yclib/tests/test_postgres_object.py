def inspect_table(pool, schema: str, table: str) -> dict[str, str]:
    with get_connection(pool) as connection:
        with get_cursor(connection) as cursor:
            query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '{schema}' and table_name = '{table}';"
            cursor.execute(query)
            result = {rcd[0]: rcd[1] for rcd in cursor.fetchall()}
            return result


def inspect_schema_existance(pool, schema: str) -> list:
    with get_connection(pool) as connection:
        with get_cursor(connection) as cursor:
            query = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema}';"
            cursor.execute(query)
            result = [rcd for rcd in cursor.fetchall()]
            return result


def inspect_table_existance(pool, schema: str, table: str) -> list:
    with get_connection(pool) as connection:
        with get_cursor(connection) as cursor:
            query = f"SELECT * FROM information_schema.tables WHERE table_schema = '{schema}' and table_name = '{table}';"
            cursor.execute(query)
            result = [rcd for rcd in cursor.fetchall()]
            return result


def inspect_table_shape(pool, schema: str, table: str) -> dict[str, str]:
    with get_connection(pool) as connection:
        with get_cursor(connection) as cursor:
            query = f"""SELECT count(*) FROM {schema}.{table}
                        Union
                        SELECT count(column_name) FROM information_schema.columns WHERE table_schema = '{schema}' and table_name = '{table}'"""
            cursor.execute(query)
            result = cursor.fetchall()
            return {"row": result[0][0], "column": result[1][0]}
