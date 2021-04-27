import json
from pathlib import Path
from typing import Dict
from typing import List

import pandas as pd

HERE = Path(__file__).parent


def _get_columns(
    df: pd.DataFrame,
    column_mappings: Dict[str, str],
    column_nomappings: List[str],
) -> pd.DataFrame:

    column_names_to_decode = list(column_mappings.keys())
    columns_to_extract = column_nomappings + column_names_to_decode

    return df[columns_to_extract].copy()


def _decode_columns(
    df: pd.DataFrame,
    column_mappings: Dict[str, str],
) -> pd.DataFrame:

    column_names_to_decode = list(column_mappings.keys())

    for column_name in column_names_to_decode:
        df.loc[:, column_name] = (
            df[column_name].astype(str).map(column_mappings[column_name])
        )

    return df


def clean_residential_survey(
    input_dirpath, output_filepath="electricity_residential_survey.csv"
):

    survey_raw = pd.read_csv(
        Path(input_dirpath)
        / "CER Electricity Revised March 2012"
        / "CER_Electricity_Data"
        / "Survey data - CSV format"
        / "Smart meters Residential pre-trial survey data.csv",
        encoding="latin-1",
        low_memory=False,
    )
    with open(HERE / "residential_survey" / "mappings.json") as json_file:
        column_mappings = json.load(json_file)
    with open(HERE / "residential_survey" / "nomappings.txt", "r") as file:
        column_nomappings = file.read().splitlines()

    survey_columns = _get_columns(survey_raw, column_mappings, column_nomappings)
    survey_decoded = _decode_columns(
        survey_columns,
        column_mappings,
    )
    survey_decoded.to_csv(output_filepath, index=False)


def clean_sme_survey(input_dirpath, output_filepath="electricity_sme_survey.csv"):

    survey_raw = pd.read_csv(
        Path(input_dirpath)
        / "CER Electricity Revised March 2012"
        / "CER_Electricity_Data"
        / "Survey data - CSV format"
        / "Smart meters SME pre-trial survey data.csv",
        encoding="latin-1",
        low_memory=False,
    )
    with open(HERE / "sme_survey" / "mappings.json") as json_file:
        column_mappings = json.load(json_file)
    with open(HERE / "sme_survey" / "nomappings.txt", "r") as file:
        column_nomappings = file.read().splitlines()

    survey_columns = _get_columns(survey_raw, column_mappings, column_nomappings)
    survey_decoded = _decode_columns(
        survey_columns,
        column_mappings,
    )
    survey_decoded.to_csv(output_filepath, index=False)
