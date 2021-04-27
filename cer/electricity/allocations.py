from pathlib import Path
from typing import Dict
from typing import List

import dask.dataframe as dd
import numpy as np
import pandas as pd

HERE = Path(__file__).parent


def _decode_columns(
    df: pd.DataFrame,
    column_names: List[str],
    column_mappings: Dict[str, str],
) -> pd.DataFrame:

    for column_name in column_names:
        df.loc[:, column_name] = (
            df[column_name].astype(str).map(column_mappings[column_name])
        )

    return df


def clean_allocations(input_dirpath, output_filepath="electricity_allocations.csv"):

    column_names = [
        "ID",
        "Code",
        "Residential - Tariff allocation",
        "Residential - stimulus allocation",
        "SME allocation",
    ]
    allocations_raw = pd.read_excel(
        Path(input_dirpath)
        / "CER Electricity Revised March 2012"
        / "CER_Electricity_Documentation"
        / "SME and Residential allocations.xlsx",
        engine="openpyxl",
        usecols=column_names,
    )
    with open(HERE / "allocation_mappings.json") as json_file:
        allocation_mappings = json.load(json_file)

    allocations_decoded = _decode_columns(
        allocations_raw,
        column_names=[
            "Code",
            "Residential - Tariff allocation",
            "Residential - stimulus allocation",
            "SME allocation",
        ],
        column_mappings=allocation_mappings,
    )
    allocations_decoded.to_csv(output_filepath, index=False)
