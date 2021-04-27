"""Transform Gas Demands.

From:

ID,DT,Usage
1565,33501,0

To:

id      datetime                demand
1565    2009-12-02 00:30:00     0
"""

from pathlib import Path
from typing import Iterable

import dask.dataframe as dd
from dask.diagnostics import ProgressBar


def _read_raw_txt_files(dirpath: str) -> Iterable[dd.DataFrame]:

    filepaths = list(dirpath.glob("GasDataWeek*"))

    return dd.read_csv(
        filepaths,
        sep=",",
        header=0,
        dtype={"ID": "int16", "DT": "string", "Usage": "float32"},
        engine="c",
    )


def _slice_timeid_column(ddf: dd.DataFrame) -> dd.DataFrame:

    ddf["day"] = ddf["DT"].str.slice(0, 3).astype("int16")
    ddf["halfhourly_id"] = ddf["DT"].str.slice(3, 5).astype("int8")

    return ddf.drop(columns=["DT"])


def _convert_dayid_to_datetime(ddf: dd.DataFrame) -> dd.DataFrame:

    ddf["datetime"] = (
        dd.to_datetime(
            ddf["day"],
            origin="01/01/2009",
            unit="D",
        )
        + dd.to_timedelta(ddf["halfhourly_id"] / 2, unit="h")
    )

    return ddf.drop(columns=["day", "halfhourly_id"])


def clean_gas_demands(input_dirpath, output_dirpath="gas_demands"):

    demand_raw = _read_raw_txt_files(
        Path(input_dirpath) / "CER Gas Revised October 2012" / "CER_Gas_Data"
    )
    demand_with_times = _slice_timeid_column(demand_raw)
    demand_with_datetimes = _convert_dayid_to_datetime(demand_with_times)

    print("Cleaning Gas Demands...")
    with ProgressBar():
        demand_with_datetimes.to_parquet(output_dirpath)
