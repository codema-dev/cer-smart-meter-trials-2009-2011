import json
from pathlib import Path
from typing import Dict
from typing import List

import pandas as pd

HERE = Path(__file__).parent


def _decode_column(
    df: pd.DataFrame,
    column_name: str,
    column_mappings: Dict[str, str],
) -> pd.DataFrame:

    df.loc[:, column_name] = (
        df[column_name].astype(str).map(column_mappings[column_name])
    )

    return df


def clean_allocations(input_dirpath, output_filepath="gas_allocations.csv"):

    column_names = [
        "ID",
        "Allocation",
    ]
    allocations_raw = pd.read_excel(
        Path(input_dirpath)
        / "CER Gas Revised October 2012"
        / "CER_Gas_Documentation"
        / "Residential allocations.xls",
        engine="xlrd",
        usecols=column_names,
    )
    with open(HERE / "allocation_mappings.json") as json_file:
        allocation_mappings = json.load(json_file)

    allocations_decoded = _decode_column(
        allocations_raw,
        column_name="Allocation",
        column_mappings=allocation_mappings,
    )
    allocations_decoded.to_csv(output_filepath, index=False)
