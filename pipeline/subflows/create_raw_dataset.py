from yclib.datastore import Postgres
from pathlib import Path
from yclib.core import (
    ExcelFileHandler,
    PROJECT_NAME,
    AVAILABLE_MEMORY,
    CONCURRENCY_LIMIT,
    POSTGRES_CREDENTIAL,
)
from prefect import flow, task, get_run_logger, unmapped
from prefect.tasks import task_input_hash
from prefect.task_runners import SequentialTaskRunner
import re
import pandas as pd
from collections import ChainMap
from yclib.transform import general_functions

@task(
    name="set-raw-dataset-database",
    tags=["db-setup"],
)
def set_raw_datasets_database(db_creds: dict[str, str or int], schema: str):
    """Create schema per yaml->create_raw_datasets->schema. This schema is for storing raw datasets

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
        logger.info(
            pgs.create_table(
                schema="workflow_info",
                table="create_raw_datasets_reading_config",
                column_with_dtype={
                    "dataset": "text",
                    "batch": "text",
                    "filename": "text",
                    "function_used": "text",
                },
            )
        )


@task(
    name="",
    tags=["pre-setup"],
)
def get_dataset_tables(
    db_creds: dict[str, str or int], source_schema: str, config: dict
) -> dict:
    """according to the settings in yaml->create_raw_datasets->datasets
        a dictionary with the following structure will be returned:
        dataset -> file_group -> [table names under the group]

    Parameters
    ----------
    db_creds : dict[str, str or int]
        postgres connect credentials
    source_schema: str
        schema per yaml->ingest_source_data->schema
    config : dict
        yaml->create_raw_datasets->datasets

    Returns
    -------
    dict
        a dictionary in the following structure representing each datasets and groups in each dataset
        dataset -> file_group -> [table names under the group]
    """
    pgs = Postgres(db_creds)
    with pgs.connect():
        logger = get_run_logger()
        tables = pgs.query_to_dataframe(
            f"""SELECT "table_name" FROM information_schema.tables where "table_schema"='{source_schema}'"""
        )
        tables = tables["table_name"].tolist()
        for dataset in config:
            for batch, set in config[dataset].items():
                config[dataset][batch]["tables"] = ExcelFileHandler.filelist_filter(
                    tables,
                    filename_include=set["filename_include"],
                    filename_exclude=set["filename_exclude"],
                )
        logger.info(f"raw dataset settting reference: {str(config)}")
    return config


@task(
    name="create-raw-datasets",
    tags=["pre-setup"],
)
def create_raw_dataset(
    db_creds: dict[str, str or int], source_schema: str, dataset_config: dict
):
    pgs = Postgres(db_creds)
    with pgs.connect():
        logger = get_run_logger()
        for batch in dataset_config:
            query = "Union all".join(
                [
                    '(select * from "{schema}"."{table}")'.format(
                        schema=source_schema, table=table
                    )
                    for table in dataset_config[batch]["table"]
                ]
            )
            df=pgs.query_to_dataframe(query)
            if "functions" in dataset_config[batch] and dataset_config[batch]["functions"]:
                for func in dataset_config[batch]["functions"]:
                    df=df.getattr(functions, key)(
                                    function_input,
                                    func_config,
                                    stage="TransformStageOne",
                                    data_key=self.data_key,
                                ))
