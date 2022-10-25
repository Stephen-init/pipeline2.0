from yclib.datastore import Postgres
from prefect import task, get_run_logger


@task(
    name="create-{db_name}-database",
    tags=["db-setup"],
)
def create_databse(db_creds: dict[str, str or int], db_name: str):
    """create postgres database

    Parameters
    ----------
    db_creds : dict[str, str or int]
        database credentials with database preset to 'postgres'
    db_name : str
        database name
    """
    pgs = Postgres(db_creds)
    with pgs.connect():
        logger = get_run_logger()
        logger.info(pgs.create_database(db_name))


@task(
    name="create-{schema}-schema",
    tags=["db-setup"],
)
def create_schema(db_creds: dict[str, str or int], schema: str):
    """create postgres schema

    Parameters
    ----------
    db_creds : dict[str, str or int]
        database credentials
    schema : str
        schema name
    """
    pgs = Postgres(db_creds)
    with pgs.connect():
        logger = get_run_logger()
        logger.info(pgs.create_schema(schema))


@task(
    name="create-{schema}-schema",
    tags=["db-setup"],
)
def create_table(
    db_creds: dict[str, str or int], schema: str, table: str, structure: dict[str, str]
):
    """Create postgres table

    Parameters
    ----------
    db_creds : dict[str, str or int]
        database credentials
    schema : str
        schema name
    table : str
        table name
    structure : dict[str, str]
        table structure with column name as keys and column data type as values
    """
    pgs = Postgres(db_creds)
    with pgs.connect():
        pgs.create_table(
            schema=schema,
            table=table,
            column_with_dtype=structure,
        )
        logger = get_run_logger()
        logger.info(pgs.create_schema(schema))
