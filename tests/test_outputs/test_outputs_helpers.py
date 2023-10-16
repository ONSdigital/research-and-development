from msilib import schema
from src.outputs.outputs_helpers import create_output_df

import pandas as pd
import unittest
from pandas._testing import assert_frame_equal


class TestSelectCols(unittest.TestCase):
    """Unittest for selecting common columns from schema and df"""

    def data_frame(self):
        # create sample data
        data = {
            "reference": [1234],
            "period": [202201],
            "unused": [0]}

        df = pd.DataFrame(data)
        return df

    def test_select_cols(self):
        # Call the create_output_df funtion
        df_input = self.data_frame()
        schema = "./tests/test_outputs/test_outputs_helpers.toml"
        actual_result = create_output_df(df_input, schema)
        expected_result = pd.DataFrame(
            {
                "ref": [1234],
                "period": [202201],
            }
        )

        assert_frame_equal(actual_result, expected_result)


if __name__ == '__main__':
    unittest.main()
