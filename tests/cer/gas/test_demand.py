from io import StringIO
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from ireland_smartmeterdata.cer.gas import demand


@pytest.fixture
def gas_data_dir(tmp_path: Path) -> Path:

    gas_data = pd.read_csv(
        StringIO(
            """ID,DT,Usage
            1565,33501,0
            1565,33502,0
            """
        )
    )
    gas_data_filepaths = [
        tmp_path / "GasDataWeek 0",
        tmp_path / "GasDataWeek 1",
    ]
    gas_data.to_csv(gas_data_filepaths[0], index=False)
    gas_data.to_csv(gas_data_filepaths[1], index=False)

    return tmp_path


def test_read_raw_txt_files(gas_data_dir: Path) -> None:

    expected_output = pd.read_csv(
        StringIO(
            """ID,DT,Usage
            0, 1565,33501,0
            1, 1565,33502,0
            0, 1565,33501,0
            1, 1565,33502,0
            """
        ),
        dtype={"ID": "int16", "DT": "string", "Usage": "float32"},
        index_col=0,
    )

    output = demand.read_raw_txt_files.run(gas_data_dir).compute()

    assert_frame_equal(output, expected_output)


def test_slice_timeid_column() -> None:
    """Slice 19503 into 195 and 3 for all rows."""
    timeid = pd.DataFrame({"DT": pd.Series(["19503", "19504"], dtype="string")})
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