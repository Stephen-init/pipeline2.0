import functools
import itertools
import operator
import re
import time
import uuid
import networkx
import numpy as np
import pandas as pd
import traceback
from typing import Any
from datetime import datetime


def func_log(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        args_repr = [
            "dataframe" if isinstance(a, pd.DataFrame) else repr(a) for a in args
        ]
        kwargs_repr = [
            "{" + str(k) + ":dataframe}"
            if isinstance(v, pd.DataFrame)
            else "{" + str(k) + ":" + repr(v) + "}"
            for k, v in kwargs.items()
        ]
        try:
            timestart = time.time()
            result = func(*args, **kwargs)
            run_time = round((time.time() - timestart), 2)
            message = f"Succesfully ran function: {func.__name__} with args {args_repr}. RunTime: {run_time} seconds"
            return {"result": result, "message": message}
        except Exception as e:
            tb = traceback.format_exc()
            raise Exception(f"Failed to run function {func.__name__}. Traceback: {tb}")

    return wrapper


@func_log
def create_columns_with_default_value(
    df: pd.DataFrame, column_name: list, default_value: Any = np.nan
) -> pd.DataFrame:
    """create a pandas dataframe with a default value

    Parameters
    ----------
    df : pd.DataFrame
        source pandas dataframe
    column_name : str
        name of column that will be created
    default_value : Any, optional
        default value for the new column. default to be None

    Returns
    -------
    pd.DataFrame
        pandas dataframe with new column created
    """
    df[column_name] = default_value
    return df


@func_log
def rename_columns(df: pd.DataFrame, name_map: dict[str, str]) -> pd.DataFrame:
    """rename columns in a dataframes according to the name_map dictionary

    Parameters
    ----------
    df : pd.DataFrame
        source pandas dataframe
    name_map : dict[str, str]
        a dictionary with source column names as keys and target names as values

    Returns
    -------
    pd.DataFrame
        pandas dataframe with new column names
    """

    return df.rename(columns=name_map, inplace=True)


@func_log
def change_data_type(df: pd.DataFrame, dtype_map: dict[str, str]) -> pd.DataFrame:
    """change data types in a dataframes according to the dtype_map dictionary

    Parameters
    ----------
    df : pd.DataFrame
        source pandas dataframe
    dtype_map : dict[str, str]
        a dictionary with source column names as keys and data types as values

    Returns
    -------
    pd.DataFrame
        pandas dataframe with new data types
    """
    return df.astype(dtype_map, errors="raise")


@func_log
def flatten_effective_date(
    df: pd.DataFrame,
    key_col: str,
    flat_col: str,
    effective_date: Any,
    agg_settings: dict,
) -> pd.DataFrame:
    """flatten dataframe with 1 effective date column to 2 columns with column name 'StartDate' and 'EndDate'

    Parameters
    ----------
    df : pd.DataFrame
        source pandas dataframe
    key_col : str
        column name of the key column that will be used to group data. usually the column name of the column containing Employee Code
    flat_col : str
        column name of the column that will be flattened. i.e. column containing classification data
    effective_date : Any
        column name of the data effective date. Note: the column can have any data type.
    agg_settings : dict
        this takes statements in pandas groupby.agg() representing how values in other columns will be returned.

    Returns
    -------
    pd.DataFrame
        pandas dataframe with new data types
    """
    df.sort_values([key_col, effective_date], ascending=[True, True], inplace=True)
    df.loc[
        ((df[key_col] != df[key_col].shift()) | (df[flat_col] != df[flat_col].shift())),
        "rolling_index",
    ] = 1
    df["rolling_index"] = df["rolling_index"].fillna(0)
    df["rolling_index"] = df["rolling_index"].cumsum()

    df = df.groupby(["rolling_index"]).agg(agg_settings).reset_index(drop=True)

    def _get_end_date(row):
        row["EndDate"] = row[effective_date].shift(-1)
        return row

    df = df.groupby([key_col]).apply(_get_end_date)
    df.rename(columns={effective_date: "StartDate"}, inplace=True)
    return df


@func_log
def group_multiple_date(
    df: pd.DataFrame,
    key_col: str,
    group_col: str,
    period_start: Any,
    period_end: Any,
    agg_settings: dict,
) -> pd.DataFrame:
    """group multiple data records with period start and period end to one record with minimum period start as the StartDate and maximum period end as the EndDate

    Parameters
    ----------
    df : pd.DataFrame
        source pandas dataframe
    key_col : str
        column name of the key column that will be used to group data. usually the column name of the column containing Employee Code
    group_col : str
        column name of the column that will be grouped on. i.e. column containing classification data
    period_start : Any
        column name of the data period start date. Note: the column can have any data type.
    period_end : Any
        column name of the data period end date. Note: the column can have any data type.
    agg_settings : dict
        this takes statements in pandas groupby.agg() representing how values in other columns will be returned.

    Returns
    -------
    pd.DataFrame
        pandas dataframe with new data types
    """
    df.sort_values([key_col, period_start], ascending=[True, True], inplace=True)
    df.loc[
        (
            (df[key_col] != df[key_col].shift())
            | (df[group_col] != df[group_col].shift())
        ),
        "rolling_index",
    ] = 1
    df["rolling_index"] = df["rolling_index"].fillna(0)
    df["rolling_index"] = df["rolling_index"].cumsum()
    agg_settings[period_start] = "min"
    agg_settings[period_end] = "max"
    df = df.groupby(["rolling_index"]).agg(agg_settings).reset_index(drop=True)
    return df


@func_log
def list_source_to_dict(df: pd.DataFrame, source_col_name: str) -> pd.DataFrame:
    """reformat values in source column. Change a list of sources in string with format:
        [filename1:sheet1:row:1, filename1:sheet1:row:2, filename1:sheet2:row:1, filename1:sheet2:row:2] to the following:
        {filename1:sheet1:row: [1,2],filename1:sheet2:row: [1,2]}

    Parameters
    ----------
    df : pd.DataFrame
        source pandas dataframe
    source_col_name : str
        name of the "Source" column. usually should just be 'Source'

    Returns
    -------
    pd.DataFrame
        pandas dataframe with new source format
    """

    def _source_formatter(row):
        result = {}
        for i in [j.split(":row:") for j in row]:
            i[0] = i[0] + ":row:"
            if i[0] not in result:
                result[i[0]] = []
                if i[1] not in result[i[0]]:
                    result[i[0]].append(i[1])
            else:
                if i[1] not in result[i[0]]:
                    result[i[0]].append(i[1])
        return str(result)

    df[source_col_name] = df[source_col_name].apply(_source_formatter)
    return df
