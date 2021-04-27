from io import StringIO
from pathlib import Path
from shutil import make_archive

import pandas as pd
import pytest

from pandas.testing import assert_frame_equal

from cer.electricity import demand


@pytest.fixture
def electricity_data_dir(tmp_path: Path) -> Path:

    electricity_data = pd.read_csv(
        StringIO(
            """1565,33501,0
            1565,33502,0
            """
        )
    )
    electricity_data_filepaths = [
        tmp_path / "File1.txt",
        tmp_path / "File2.txt",
    ]
    electricity_data.to_csv(electricity_data_filepaths[0], sep=" ", index=False)
    electricity_data.to_csv(electricity_data_filepaths[1], sep=" ", index=False)

    make_archive(
        base_name=electricity_data_filepaths[0],
        format="zip",
        root_dir=tmp_path,
    )
    make_archive(
        base_name=electricity_data_filepaths[1],
        format="zip",
        root_dir=tmp_path,
    )

    return tmp_path


def test_read_raw_txt_files(electricity_data_dir: Path) -> None:

    expected_output = pd.read_csv(
        StringIO(
            """ID,timeid,demand
            0, 1565,33501,0
            1, 1565,33502,0
            0, 1565,33501,0
            1, 1565,33502,0
            """
        ),
        dtype={"ID": "int16", "timeid": "string", "demand": "float32"},
        index_col=0,
    )

    output = demand._read_raw_txt_files(electricity_data_dir).compute()

    assert_frame_equal(output, expected_output)


def test_slice_timeid_column() -> None:
    """Slice 19503 into 195 and 3 for all rows."""
    timeid = pd.DataFrame({"timeid": pd.Series(["19503", "19504"], dtype="string")})
    expected_output = pd.DataFrame(
        {
            "day": pd.Series([195, 195], dtype="int16"),
            "halfhourly_id": pd.Series([3, 4], dtype="int8"),
        },
    )

    output = demand._slice_timeid_column(timeid)

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

    output = demand._convert_dayid_to_datetime(dayid)

    assert_frame_equal(output, expected_output)