from pathlib import Path

from cer.electricity.demand import clean_electricity_demands
from cer.electricity.allocations import clean_electricity_allocations
from cer.electricity.survey import clean_electricity_residential_survey
from cer.electricity.survey import clean_electricity_sme_survey
from cer.gas.demand import clean_gas_demands
from cer.gas.allocations import clean_gas_allocations
from cer.gas.survey import clean_gas_residential_survey


def clean_cer_data(
    dirpath: Path, output_dirpath: Path = "(Clean) CER Smart Metering Project"
) -> None:

    print("Cleaning Electricity Data...\n")

    electricity_demand_dirpath = Path(output_dirpath) / "electricity_demands"
    print(f"Cleaning Electricity Demands to {electricity_demand_dirpath}")
    clean_electricity_demands(dirpath, electricity_demand_dirpath)

    electricity_allocations_filepath = (
        Path(output_dirpath) / "electricity_allocations.csv"
    )
    print(f"Cleaning Electricity Allocations to {electricity_demand_dirpath}")
    clean_electricity_allocations(dirpath, electricity_allocations_filepath)

    electricity_residential_survey_filepath = (
        Path(output_dirpath) / "electricity_residential_survey.csv"
    )
    electricity_sme_survey_filepath = (
        Path(output_dirpath) / "electricity_sme_survey.csv"
    )
    print(
        f"Cleaning Electricity Residential Survey to {electricity_residential_survey_filepath}"
    )
    print(f"Cleaning Electricity SME Survey to {electricity_sme_survey_filepath}")
    clean_electricity_residential_survey(
        dirpath, Path(output_dirpath) / "electricity_residential_survey.csv"
    )
    clean_electricity_sme_survey(
        dirpath, Path(output_dirpath) / "electricity_sme_survey.csv"
    )

    print("\nCleaning Gas Data...\n")

    gas_demand_dirpath = Path(output_dirpath) / "gas_demands"
    print(f"Cleaning Gas Demands to {gas_demand_dirpath}")
    clean_gas_demands(dirpath, gas_demand_dirpath)

    gas_allocations_filepath = Path(output_dirpath) / "gas_allocations.csv"
    print(f"Cleaning Gas Allocations to {gas_demand_dirpath}")
    clean_gas_allocations(dirpath, gas_allocations_filepath)

    gas_residential_survey_filepath = (
        Path(output_dirpath) / "gas_residential_survey.csv"
    )
    print(f"Cleaning Gas Residential Survey to {gas_residential_survey_filepath}")
    clean_gas_residential_survey(dirpath, gas_residential_survey_filepath)
