"""Transform Gas Demands.

From:

ID,DT,Usage
1565,33501,0

To:

id      datetime                demand
1565    2009-12-02 00:30:00     0
"""

from glob import glob
from os import path
from typing import Iterable

import dask.dataframe as dd
from prefect import Flow
from prefect import Parameter
from prefect import task


@task
def get_path_to_raw_txt_files(dirpath: str) -> str:

    return path.join(
        dirpath,
        "CER Gas Revised October 2012",
        "CER_Gas_Data",
    )


@task
def read_raw_txt_files(dirpath: str) -> Iterable[dd.DataFrame]:

    filepaths = glob(f"{dirpath}/GasDataWeek*")

    return dd.read_csv(
        filepaths,
        sep=",",
        header=0,
        dtype={"ID": "int16", "DT": "string", "Usage": "float32"},
        engine="c",
    )


@task
def slice_timeid_column(ddf: dd.DataFrame) -> dd.DataFrame:

    ddf["day"] = ddf["DT"].str.slice(0, 3).astype("int16")
    ddf["halfhourly_id"] = ddf["DT"].str.slice(3, 5).astype("int8")

    return ddf.drop(columns=["DT"])


@task
def convert_dayid_to_datetime(ddf: dd.DataFrame) -> dd.DataFrame:

    ddf["datetime"] = (
        dd.to_datetime(
            ddf["day"],
            origin="01/01/2009",
            unit="D",
        )
        + dd.to_timedelta(ddf["halfhourly_id"] / 2, unit="h")
    )

    return ddf.drop(columns=["day", "halfhourly_id"])


@task
def write_parquet(ddf: dd.DataFrame, savepath: str):

    return ddf.to_parquet(savepath)


with Flow("Clean Gas Demands") as flow:

    input_dirpath = Parameter("input_dirpath")
    output_dirpath = Parameter("output_dirpath")
    n_workers = Parameter("n_workers", default=1)

    path_to_raw_txt_files = get_path_to_raw_txt_files(input_dirpath)

    demand_raw = read_raw_txt_files(path_to_raw_txt_files)
    demand_with_times = slice_timeid_column(demand_raw)
    demand_with_datetimes = convert_dayid_to_datetime(demand_with_times)
    
    write_parquet(demand_with_datetimes, output_dirpath)
