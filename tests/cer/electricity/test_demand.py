from os import mkdir
from pathlib import Path

import pandas as pd
import pytest

from pandas.testing import assert_frame_equal
from prefect import Task

from ireland_smartmeterdata.cer.electricity import demand


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


def test_slice_timeid_column() -> None:
    """Slice 19503 into 195 and 3 for all rows."""
    timeid = pd.DataFrame({"timeid": pd.Series(["19503", "19504"], dtype="string")})
    expected_output = pd.DataFrame(
        {
            "day": pd.Series([195, 195], dtype="int16"),
            "halfhourly_id": pd.Series([3, 4], dtype="int8"),
        },
    )

    output = demand.slice_timeid_column.run(timeid)

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
                ["2009-07-15 01:30:00", "2009-07-15 02:00:00"],
                dtype="datetime64[ns]",
            ),
        },
    )

    output = demand.convert_dayid_to_datetime.run(dayid)

    assert_frame_equal(output, expected_output)