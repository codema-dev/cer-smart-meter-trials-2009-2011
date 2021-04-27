"""Transform Electricity Demands.

From:

1392  19503   0.14

To:

id      datetime                demand
1392    2009-07-15 01:30:00     0.14
"""

from pathlib import Path
from typing import Iterable

import dask.dataframe as dd
from dask.diagnostics import ProgressBar


def _read_raw_txt_files(dirpath: Path) -> Iterable[dd.DataFrame]:

    filepaths = list(dirpath.glob("*.txt.zip"))

    return dd.read_csv(
        filepaths,
        compression="zip",
        blocksize=None,
        header=None,
        sep=" ",
        names=["ID", "timeid", "demand"],
        dtype={"ID": "int16", "timeid": "string", "demand": "float32"},
        engine="c",
    )


def _slice_timeid_column(ddf: dd.DataFrame) -> dd.DataFrame:

    ddf["day"] = ddf["timeid"].str.slice(0, 3).astype("int16")
    ddf["halfhourly_id"] = ddf["timeid"].str.slice(3, 5).astype("int8")

    return ddf.drop(columns=["timeid"])


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


def clean_electricity_demands(input_dirpath, output_dirpath):

    demand_raw = _read_raw_txt_files(
        Path(input_dirpath) / "CER Electricity Revised March 2012"
    )
    demand_with_times = _slice_timeid_column(demand_raw)
    demand_with_datetimes = _convert_dayid_to_datetime(demand_with_times)

    with ProgressBar():
        demand_with_datetimes.to_parquet(output_dirpath)
