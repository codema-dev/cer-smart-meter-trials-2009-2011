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
def get_survey_mappings_filepath() -> str:

    return path.join(here, "residential_survey/mappings.json")


@task
def get_survey_nomappings_filepath() -> str:

    return path.join(here, "residential_survey/nomappings.txt")


@task
def get_path_to_survey(dirpath: str) -> str:

    return path.join(
        dirpath,
        "CER Gas Revised October 2012",
        "CER_Gas_Data",
        "Smart meters Residential pre-trial survey data - Gas.csv",
    )


@task
def read_surveys(filepath: str) -> pd.DataFrame:

    return pd.read_csv(filepath, encoding="latin-1", low_memory=False)


@task
def read_mappings(filepath: str) -> Dict[str, str]:

    with open(filepath) as json_file:
        return json.load(json_file)


@task
def read_nomappings(filepath: str) -> List[str]:

    with open(filepath, "r") as file:
        return file.read().splitlines()


@task
def get_columns(
    df: pd.DataFrame,
    column_mappings: Dict[str, str],
    column_nomappings: List[str],
) -> pd.DataFrame:

    column_names_to_decode = list(column_mappings.keys())
    columns_to_extract = column_nomappings + column_names_to_decode

    return df[columns_to_extract].copy()


@task
def decode_columns(
    df: pd.DataFrame,
    column_mappings: Dict[str, str],
) -> pd.DataFrame:

    column_names_to_decode = list(column_mappings.keys())

    for column_name in column_names_to_decode:
        df.loc[:, column_name] = (
            df[column_name].astype(str).map(column_mappings[column_name])
        )

    return df


@task
def write_to_parquet(df: pd.DataFrame, filepath: str) -> None:

    df.to_parquet(filepath)


with Flow("Map surveys to Building IDs") as flow:

    input_dirpath = Parameter("input_dirpath")
    output_filepath = Parameter("output_filepath")

    path_to_survey = get_path_to_survey(input_dirpath)

    survey_raw = read_surveys(path_to_survey)

    survey_mappings_filepath = get_survey_mappings_filepath()
    survey_nomappings_filepath = get_survey_nomappings_filepath()
    survey_mappings = read_mappings(survey_mappings_filepath)
    survey_nomappings = read_nomappings(survey_nomappings_filepath)

    survey_columns = get_columns(survey_raw, survey_mappings, survey_nomappings)
    survey_decoded = decode_columns(
        survey_columns,
        column_mappings=survey_mappings,
    )

    write_to_parquet(survey_decoded, output_filepath)
