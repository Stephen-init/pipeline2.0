import os
import boto3
import pandas as pd
import io
from dotenv import load_dotenv
import inspect
from pathlib import Path

load_dotenv()
AWS_REGION = os.getenv("AWS_REGION")
ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")


def get_s3_conn(
    aws_region: str, access_key_id: str, secret_access_key: str
) -> boto3.client:
    """Returns boto3.client used to connect to an s3 bucket
    Parameters
    ----------
    aws_region : str
        AWS region of the target bucket (e.g - 'ap-southeast-2')
    access_key_id : str
        AWS access key used to access the s3 bucket
    secret_access_key : str
        AWS secret access key used to access the s3 bucket
    Returns
    -------
    s3_conn : boto3.client
        boto3.client that can be used for s3 connection
    """

    s3_conn = boto3.client(
        "s3",
        region_name=aws_region,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
    )
    return s3_conn


def get_s3_keys(
    bucket: str,
    s3_conn: boto3.client,
    folder: str = None,
    tabular_data_only: bool = False,
) -> list:
    """Returns a list of keys in an S3 bucket.
    Parameters
    ----------
    bucket : str
        name of the target s3 bucket
    s3_conn : boto3.client
        boto3 client used to connect to s3 bucket
    folder : str, optional
        optional sub-folder name, by default None
    tabular_data_only : bool, optional
        option to return keys for tabular data only
        (excel, csv files), by default False
    Returns
    -------
    keys
        list of s3 keys
    """
    keys = []
    # use specific folder if specified
    if folder:
        resp = s3_conn.list_objects_v2(Bucket=bucket, Prefix=folder)
    else:
        resp = s3_conn.list_objects_v2(Bucket=bucket)
    # get keys present and append to keys list

    return [
        obj["Key"]
        for obj in resp["Contents"]
        if Path(obj["Key"]).suffix.lower()
        in [".xlsx", ".xls", ".xlsm", ".txt", ".csv", ".xlsb"]
    ]


def s3_to_df(s3_bucket: str, s3_keys: list, s3_conn: boto3.client, **kwargs) -> dict:
    """Reads tabular files stored in s3 into a dictionary of pandas dataframes
    Parameters
    ----------
    s3_bucket : str
        name of the target s3 bucket
    s3_keys : list
        list of target s3 keys (files to be read)
    s3_conn : boto3.client
        boto3.client used to connect to the s3 bucket
    kwargs :
        optional list of arguments to pass to pd.read_csv or pd.read_excel
    Returns
    -------
    data : dict
        dictionary of dataframes
    """

    data = {}
    for s3_key in s3_keys:
        response_obj = s3_conn.get_object(Bucket=s3_bucket, Key=s3_key)
        if s3_key.lower().endswith((".xlsx", ".xls", ".xlsm", ".xlsb")):
            # get rid of wrong kwargs
            reading_config = {
                key: val
                for key, val in kwargs.items()
                if key in inspect.signature(pd.read_excel).parameters
            }
            try:
                # read excel file in s3 bucket into dataframe
                data[s3_key] = pd.read_excel(
                    io.BytesIO(response_obj["Body"].read()), **reading_config
                )
            except Exception as e:
                print(f"Failed to read file {s3_key}. Got error: {e}")
        elif s3_key.lower().endswith((".csv")):
            # get rid of wrong kwargs
            reading_config = {
                key: val
                for key, val in kwargs.items()
                if key in inspect.signature(pd.read_csv).parameters
            }
            try:
                # read csv file in s3 bucket into dataframe
                data[s3_key] = pd.read_csv(
                    io.BytesIO(response_obj["Body"].read()),
                    encoding="utf8",
                    **reading_config,
                )
            except Exception as e:
                print(f"Failed to read file {s3_key}. Got error: {e}")

    return data


# --- RUN
if __name__ == "__main__":

    s3_conn = get_s3_conn(
        aws_region=AWS_REGION,
        access_key_id=ACCESS_KEY_ID,
        secret_access_key=SECRET_ACCESS_KEY,
    )

    keys = get_s3_keys(bucket=BUCKET_NAME, s3_conn=s3_conn, tabular_data_only=True)

    data = s3_to_df(
        s3_bucket=BUCKET_NAME,
        s3_keys=keys,
        s3_conn=s3_conn,
        # usecols=['EmployeeCode', 'DateOfBirth'] e.g
    )

    a = 1
