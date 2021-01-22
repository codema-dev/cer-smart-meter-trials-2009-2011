import json
from os import path
from typing import Dict
from typing import List

import pandas as pd
from prefect import Flow
from prefect import Parameter
from prefect import task

here = path.abspath(path.dirname(__file__))


@task
def get_path_to_allocations(dirpath: str) -> str:

    return path.join(
        dirpath,
        "CER Gas Revised October 2012",
        "CER_Gas_Documentation",
        "Residential allocations.xls",
    )


@task
def read_allocations(filepath: str, usecols: List[str]) -> pd.DataFrame:

    return pd.read_excel(
        filepath,
        engine="xlrd",
        usecols=usecols,
    )


@task
def get_allocation_mappings() -> pd.DataFrame:

    filepath = path.join(here, "allocation_mappings.json")

    with open(filepath) as json_file:
        return json.load(json_file)


@task
def decode_column(
    df: pd.DataFrame,
    column_name: str,
    column_mappings: Dict[str, str],
) -> pd.DataFrame:

    df.loc[:, column_name] = (
        df[column_name].astype(str).map(column_mappings[column_name])
    )

    return df


@task
def write_to_parquet(df: pd.DataFrame, filepath: str) -> None:

    df.to_parquet(filepath)


@task
def check_file_exists(filepath: str) -> bool:

    return path.exists(filepath)


with Flow("Map Allocations to Building IDs") as flow:

    input_dirpath = Parameter("input_dirpath")
    output_filepath = Parameter("output_filepath")

    column_names = [
        "ID",
        "Allocation",
    ]
    path_to_allocations = get_path_to_allocations(input_dirpath)

    allocations_raw = read_allocations(path_to_allocations, usecols=column_names)
    allocation_mappings = get_allocation_mappings()

    allocations_decoded = decode_column(
        allocations_raw,
        column_name="Allocation",
        column_mappings=allocation_mappings,
    )

    write_to_parquet(allocations_decoded, output_filepath)
