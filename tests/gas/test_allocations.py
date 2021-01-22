from io import StringIO

import pandas as pd
from pandas.testing import assert_frame_equal

from ireland_smartmeterdata.cer.gas import allocations


def test_decode_columns():

    allocation_mappings = allocations.get_allocation_mappings.run()
    input_df = pd.read_csv(
        StringIO(
            """ID,Allocation
            1000,1
            1001,3
            1002,4
            1003,C
            """
        )
    )
    expected_output = pd.read_csv(
        StringIO(
            """ID,Allocation
            1000,Bi-monthly bill
            1001,Bi-monthly bill + IHD
            1002,Bi-monthly bill + IHD + tariff
            1003,Control
            """
        )
    )

    output = allocations.decode_column.run(
        input_df,
        column_name="Allocation",
        column_mappings=allocation_mappings,
    )

    assert_frame_equal(output, expected_output)