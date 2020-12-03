from os import mkdir
from pathlib import Path

import pandas as pd
import pytest

from pandas.testing import assert_frame_equal
from prefect import Task

from ireland_smartmeterdata.cer.electricity import convert_dayid_to_datetime
from ireland_smartmeterdata.cer.electricity import read_raw_txt_files
from ireland_smartmeterdata.cer.electricity import slice_timeid_column


@pytest.fixture
def raw_elec_demands_dirpath(tmp_path: Path) -> Path:
    """Create a temporary directory containing dummy data called 'raw'.

    Args:
        tmp_path (Path): see https://docs.pytest.org/en/stable/tmpdir.html

    Returns:
        Path: Path to a directory containing dummy data files called 'raw'
    """
    dirpath = tmp_path / "raw"
    mkdir(dirpath)
    for filename in ("File1", "File2"):
        with open(dirpath / f"{filename}.txt", "w") as file:
            file.writelines("1392 19503 0.14\n1392 19504 0.138\n")

    return dirpath


@pytest.fixture
def clean_elec_demands_dirpath(tmp_path: Path) -> Path:
    """Create a temporary, empty directory called 'processed'.

    Args:
        tmp_path (Path): see https://docs.pytest.org/en/stable/tmpdir.html

    Returns:
        Path: Path to a temporary, empty directory called 'processed'.
    """
    dirpath = tmp_path / "processed"
    mkdir(dirpath)

    return dirpath / "SM_electricity"


@pytest.fixture
def clean_cru_elec_demand(
    raw_elec_demands_dirpath: Path, clean_elec_demands_dirpath: Path,
) -> Task:
    """Create an instance of Task CleanCRUElecDemand using temporary data.

    Args:
        raw_elec_demands_dirpath (Path): Path to a directory containing dummy data files
            called 'raw'
        clean_elec_demands_dirpath (Path):  Path to a temporary, empty directory called
            'processed'

    Returns:
        Task: An instance of Task CleanCRUElecDemand
    """
    return CleanCRUElecDemand(
        dirpath=raw_elec_demands_dirpath, savepath=clean_elec_demands_dirpath,
    )


def test_read_raw_txt_files_sm_elec(raw_elec_demands_dirpath: Path) -> None:
    """Read data from multiple text files into a list/interator of Dask DataFrames.

    Args:
        raw_elec_demands_dirpath (Path): A temporary directory containing dummy data
    """
    expected_output = pd.DataFrame(
        {
            "id": pd.Series([1392, 1392], dtype="int16"),
            "timeid": pd.Series(["19503", "19504"], dtype="string"),
            "demand": pd.Series([0.14, 0.138], dtype="float32"),
        },
    )

    outputs = _read_txt_files.run(raw_elec_demands_dirpath)

    assert_frame_equal(outputs[0].compute(), expected_output)
    assert_frame_equal(outputs[1].compute(), expected_output)


def test_slice_timeid_column() -> None:
    """Slice 19503 into 195 and 3 for all rows."""
    timeid = pd.DataFrame({"timeid": pd.Series(["19503", "19504"], dtype="string")})
    expected_output = pd.DataFrame(
        {
            "day": pd.Series([195, 195], dtype="int16"),
            "halfhourly_id": pd.Series([3, 4], dtype="int8"),
        },
    )

    output = _slice_timeid_column.run(timeid)

    assert_frame_equal(output, expected_output)


def test_convert_dayid_to_datetime() -> None:
    """Convert each day/halfhourly_id into a corresponding datetime."""
    dayid = pd.DataFrame(
        {
            "day": pd.Series([195, 195], dtype="int16"),
            "halfhourly_id": pd.Series([3, 4], dtype="int8"),
        },
    )
    expected_output = pd.DataFrame(
        {
            "datetime": pd.Series(
                ["2009-07-15 01:30:00", "2009-07-15 02:00:00"], dtype="datetime64[ns]",
            ),
        },
    )

    output = _convert_dayid_to_datetime.run(dayid)

    assert_frame_equal(output, expected_output)


def test_clean_cru_elec_demand_run(clean_cru_elec_demand: Task) -> None:
    """Test run output matches expected.

    Args:
        clean_cru_elec_demand (Task): An instance of Task CleanCRUElecDemand
    """
    expected_output = pd.DataFrame(
        {
            "id": pd.Series([1392, 1392, 1392, 1392], dtype="int16"),
            "demand": pd.Series([0.14, 0.138, 0.14, 0.138], dtype="float32"),
            "datetime": pd.Series(
                [
                    "2009-07-15 01:30:00",
                    "2009-07-15 02:00:00",
                    "2009-07-15 01:30:00",
                    "2009-07-15 02:00:00",
                ],
                dtype="datetime64[ns]",
            ),
        },
    )

    output = clean_cru_elec_demand.run().compute().reset_index(drop=True)

    assert_frame_equal(output, expected_output)
