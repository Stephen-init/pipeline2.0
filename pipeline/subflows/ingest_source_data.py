from this import d
from yclib.datastore import Postgres
from pathlib import Path
from yclib.core import ExcelFileHandler, FileFilter
from _settings import (
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
import tasklib


@task(
    name="get-source-file-paths",
    tags=["pre-setup"],
)
def get_files_path(path: Path) -> list[Path]:
    """get file path list per yaml -> source_files_path

    Parameters
    ----------
    path : Path
        a folder path which will be walked through

    Returns
    -------
    list[Path]
        A list of pathlib.Path objects of all excel/csv/pickle files under argument:path
    """
    filelist = FileFilter.get_filepath_list(path)
    if not filelist:
        raise ValueError(f'No file found under path "{path}"')
    else:
        return filelist


@task(
    name="filter-file-paths",
    tags=["pre-setup"],
)
def get_file_reading_config(
    filepath_list: list[Path], read_settings: dict[str, dict]
) -> dict[Path, list]:
    """get a dictionary with file path as key and pandas attributes as value

    Parameters
    ----------
    filepath_list : list[Path]
        a list of file paths
    read_settings : dict[str, dict]
        config per datasets -> (dataset_name -> {pandas_attributes, filter})

    Returns
    -------
    dict[Path, list]
        a dictionary with file path as keys and [read settings, dataset] as values
    """
    final_dict = dict()

    for file_cat, read_config in read_settings.items():
        filtered_path_list = (
            FileFilter.filelist_filter(filepath_list, **read_config["file_filters"])
            if not read_config["absolute_path_list"]
            else read_config["absolute_path_list"]
        )

        if not filtered_path_list:
            raise ValueError(
                'No file found in dataset "{dataset}", please set absolute_path_list or check: "{data_filter}"'.format(
                    dataset={file_cat},
                    data_filter=str(read_config["file_filters"]),
                )
            )
        else:
            for filepath in filtered_path_list:
                final_dict[filepath] = [read_config["read_file"], file_cat]
                logger = get_run_logger()
                logger.info(
                    "{dataset} | file {filepath} will be read in with pandas attributes: {args}".format(
                        dataset=file_cat,
                        filepath=filepath.name,
                        args=str(read_config["read_file"]["pandas_attributes"]),
                    )
                )
    return final_dict


@task(name="concurrency-setup", tags=["pre-setup"])
def concurrency_setup(
    file_reading_config_dict: dict[Path, dict], check_mem: bool = False
) -> list[dict]:
    """cut a list of files to sublists by total memory size

    Parameters
    ----------
    file_reading_config_dict : dict[Path, dict]
        file_reading_config_dict generated from task: get_file_reading_config.
        path as keys and pandas attributes dict as values
    check_mem: bool
        True if there is a need to check available memory
        False if not
        default False
    Returns
    -------
    list[dict]
       a list of sub file_reading_config_dict cut by concurrency limit and available memory
    """

    def _check_memory_switch(check_dict, check_mem=False):
        return (
            (
                True
                if sum([fp.stat().st_size for fp in check_dict]) < AVAILABLE_MEMORY
                else False
            )
            if check_mem
            else True
        )

    list_of_dict = []
    sub_dict = {}
    for k, v in file_reading_config_dict.items():
        if len(sub_dict) < CONCURRENCY_LIMIT and _check_memory_switch(
            sub_dict, check_mem
        ):
            sub_dict[k] = v
        else:
            list_of_dict.append(sub_dict)
            sub_dict = {k: v}
    list_of_dict.append(sub_dict)
    return list_of_dict


@task(
    name="read-file-to-table",
    cache_key_fn=task_input_hash,
    # cache_expiration=timedelta(days=1),
    tags=["pandas"],
)
def read_file_to_table(
    dataset: str,
    filepath: Path,
    reading_config: dict,
    db_creds: dict[str, str or int],
    schema: str,
):
    """read file to pandas dataframe->create table->insert dataframe to table

    Parameters
    ----------
    dataset:  str
        yaml->datasets->[dataset name]
    filepath : Path
        file path
    read_settings : list
        yaml->ingest_source_data->datasets->[dataset name]->[read_funcs, [dataset name]]
    db_creds : dict[str, str or int]
        postgres db connect credentials
    schema : str
        yaml->ingest_source_data->schema

    """
    logger = get_run_logger()
    df_dict = ExcelFileHandler.read_file(
        ExcelFileHandler.decrypted_file(filepath, reading_config["password"])
        if reading_config["password"]
        else filepath,
        reading_config["pandas_attributes"],
    )

    logger.info(
        f"{Path(filepath).name} with {len(df_dict.keys())} dataframe(s) have been read into pandas.DataFrames"
    )
    postgres = Postgres(db_creds)
    with postgres.connect():
        for key, val in df_dict.items():
            table = re.sub(r"[^a-zA-Z0-9]+", "_", key)
            postgres.create_table(
                schema=schema,
                table=table,
                column_with_dtype={col: "text" for col in val.columns},
            )
            postgres.dataframe_insert_to_table(schema=schema, table=table, df=val)
            logger.info(
                f"{schema}.{table}: total {str(val.shape[0])} rows x {str(val.shape[1])} columns have been ingested"
            )
            postgres.dataframe_insert_to_table(
                schema="workflow",
                table="source_file_reading_config",
                df=pd.DataFrame(
                    {
                        "dataset": [dataset],
                        "filename": [Path(filepath).name],
                        "pandas_attributes": [str(reading_config["pandas_attributes"])],
                    }
                ),
            )
    return len(df_dict.keys())


@flow(
    name="-".join([PROJECT_NAME, "Ingesting-Source-Data"]),
    task_runner=SequentialTaskRunner(),
)
def ingest_source_data(ingest_config: dict):
    """ingest stage flow

    Parameters
    ----------
    pool : SimpleConnectionPool
        connected postgres SimpleConnectionPool
    ingest_config : dict
        yaml settings for ingest stage (ingest_source_data)
    """
    logger = get_run_logger()
    logger.info(
        f"""
---------------------------------------------------------------------------------------------------------------------------------------------
                                        {PROJECT_NAME} Ingest Flow Start
                                            Available Memory: {AVAILABLE_MEMORY}
                                            Concurrency Limit: {CONCURRENCY_LIMIT}
---------------------------------------------------------------------------------------------------------------------------------------------
        """
    )
    tasklib.create_databse(POSTGRES_CREDENTIAL, PROJECT_NAME)
    POSTGRES_CREDENTIAL["dbname"] = PROJECT_NAME
    tasklib.create_schema(
        POSTGRES_CREDENTIAL, ingest_config.get("source_schema", "source_files")
    )
    tasklib.create_schema(POSTGRES_CREDENTIAL, "workflow")

    tasklib.create_table(
        POSTGRES_CREDENTIAL,
        ingest_config.get("workflow_schema", "workflow"),
        "source_file_reading_config",
        {"dataset": "text", "filename": "text", "pandas_attributes": "text"},
    )
    filepath_list = get_files_path(ingest_config.get("source_files_path"))
    file_reading_config_dict = get_file_reading_config(
        filepath_list, ingest_config["datasets"]
    )
    file_after_setup = concurrency_setup(file_reading_config_dict, check_mem=True)

    counter = 0
    for sub_dict in file_after_setup:
        result = read_file_to_table.map(
            [i[1] for i in sub_dict.values()],
            list(sub_dict.keys()),
            [i[0] for i in sub_dict.values()],
            unmapped(POSTGRES_CREDENTIAL),
            unmapped(ingest_config.get("schema", "source_files")),
        )
        counter = counter + len(result)
    logger.info(
        f"""
-----------------------------------------------------------------------------------------------------------------------------------------------
                                        {PROJECT_NAME} Ingest Stage Finished
                                            Source files: total {len(file_reading_config_dict.keys())} files
                                            Database: {PROJECT_NAME}
                                                |-Schema: workflow_info
                                                    |--table: source_file_reading_config
                                                |-Schema: {ingest_config.get("schema", "source_files")}
                                                    |--table: {counter}
-----------------------------------------------------------------------------------------------------------------------------------------------
        """
    )
