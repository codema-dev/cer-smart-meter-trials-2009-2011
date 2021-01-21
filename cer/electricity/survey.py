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
def get_survey_mappings_filepath(building_type: str) -> str:

    if building_type == "residential":
        filepath = path.join(here, "residential_survey/mappings.json")
    elif building_type == "sme":
        filepath = path.join(here, "sme_survey/mappings.json")
    else:
        raise ValueError(
            f"'building_type' was {building_type}\n Value must be 'residential' or 'sme'"
        )

    return filepath


@task
def get_survey_nomappings_filepath(building_type: str) -> str:

    if building_type == "residential":
        filepath = path.join(here, "residential_survey/nomappings.txt")
    elif building_type == "sme":
        filepath = path.join(here, "sme_survey/nomappings.txt")
    else:
        raise ValueError(
            f"'building_type' was {building_type}\n Value must be 'residential' or 'sme'"
        )

    return filepath


@task
def get_path_to_survey(dirpath: str, building_type: str) -> str:

    readdir = path.join(
        dirpath,
        "CER Electricity Revised March 2012",
        "CER_Electricity_Data",
        "Survey data - CSV format",
    )

    if building_type == "residential":
        filepath = path.join(
            readdir, "Smart meters Residential pre-trial survey data.csv"
        )
    elif building_type == "sme":
        filepath = path.join(readdir, "Smart meters SME pre-trial survey data.csv")
    else:
        raise ValueError(
            f"'building_type' was {building_type}\n Value must be 'residential' or 'sme'"
        )

    return filepath


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
    building_type = Parameter("building_type")

    path_to_survey = get_path_to_survey(input_dirpath, building_type)

    survey_raw = read_surveys(path_to_survey)

    survey_mappings_filepath = get_survey_mappings_filepath(building_type)
    survey_nomappings_filepath = get_survey_nomappings_filepath(building_type)
    survey_mappings = read_mappings(survey_mappings_filepath)
    survey_nomappings = read_nomappings(survey_nomappings_filepath)

    survey_columns = get_columns(survey_raw, survey_mappings, survey_nomappings)
    survey_decoded = decode_columns(
        survey_columns,
        column_mappings=survey_mappings,
    )

    write_to_parquet(survey_decoded, output_filepath)
