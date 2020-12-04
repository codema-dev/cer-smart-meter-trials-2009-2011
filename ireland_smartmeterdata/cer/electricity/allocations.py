import json
from os import path
from typing import Dict
from typing import List

import dask.dataframe as dd
import numpy as np
import pandas as pd
from prefect import case
from prefect import Flow
from prefect import Parameter
from prefect import task
from prefect import mapped

here = path.abspath(path.dirname(__file__))
allocation_mappings_filepath = path.join(here, "allocation_mappings.json")


@task
def get_path_to_allocations(dirpath: str) -> str:

    return path.join(
        dirpath,
        "CER Electricity Revised March 2012",
        "CER_Electricity_Documentation",
        "SME and Residential allocations.xlsx",
    )


@task
def read_allocations(filepath: str, usecols: List[str]) -> pd.DataFrame:

    return pd.read_excel(
        filepath,
        engine="openpyxl",
        usecols=usecols,
    )


@task
def read_allocation_mappings(filepath: str) -> pd.DataFrame:

    with open(filepath) as json_file:
        return json.load(json_file)


@task
def decode_columns(
    df: pd.DataFrame,
    column_names: List[str],
    column_mappings: Dict[str, str],
) -> pd.DataFrame:

    for column_name in column_names:
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

    allocations_already_cleaned = check_file_exists(output_filepath)

    with case(allocations_already_cleaned, False):

        column_names = [
            "ID",
            "Code",
            "Residential - Tariff allocation",
            "Residential - stimulus allocation",
            "SME allocation",
        ]
        path_to_allocations = get_path_to_allocations(input_dirpath)

        allocations_raw = read_allocations(path_to_allocations, usecols=column_names)
        allocation_mappings = read_allocation_mappings(allocation_mappings_filepath)

        allocations_decoded = decode_columns(
            allocations_raw,
            column_names=[
                "Code",
                "Residential - Tariff allocation",
                "Residential - stimulus allocation",
                "SME allocation",
            ],
            column_mappings=allocation_mappings,
        )

        write_to_parquet(allocations_decoded, output_filepath)
