from pickle import TRUE
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner, SequentialTaskRunner
import time
import pandas as pd
from pipeline._settings import POSTGRES_CREDENTIAL
from datastore import Postgres
import numpy as np
from difflib import SequenceMatcher
import os

POSTGRES_CREDENTIAL["dbname"] = "Oberon"
pgs = Postgres(POSTGRES_CREDENTIAL)
with pgs.connect():
    df = pgs.query_to_dataframe(
        'select * from "source_files"."Subject_to_LPP_Employee_List_POS_22_07_2022_SSTD_EMP1"'
    )
df.drop(41170, inplace=True)
agg_settings = {
    "detnumber": "last",
    "posstatus.trn": "last",
    "detterdate": "last",
    "Source": "unique",
}
df["posstart"] = pd.to_datetime(df["posstart"], format="%Y-%m-%d %H:%M:%S")
df["posend"] = pd.to_datetime(df["posend"], format="%Y-%m-%d %H:%M:%S") + pd.Timedelta(
    1, unit="day"
)

df.loc[df["posend"].isna(), "posend"] = pd.Timestamp.max.date()
df.sort_values(["detnumber", "posstart"], ascending=[True, True], inplace=True)
df.loc[
    (
        (df["detnumber"] != df["detnumber"].shift())
        | (df["posstatus.trn"] != df["posstatus.trn"].shift())
    ),
    "rolling_index",
] = 1
df["rolling_index"] = df["rolling_index"].fillna(0)
df["rolling_index"] = df["rolling_index"].cumsum()
agg_settings["posstart"] = "min"
agg_settings["posend"] = "max"
df = df.groupby(["rolling_index"]).agg(agg_settings).reset_index(drop=True)


df["Source"] = df["Source"].apply(source_formatter)
x = 1
