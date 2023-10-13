import pandas as pd
import unittest
from pandas._testing import assert_frame_equal
import os
print(os.getcwd())
from src.outputs.outputs_helpers import create_output_df


class TestSelectCols(unittest.TestCase):
    """Unittest for selecting common columns from schema and df"""

    def data_frame(self):
        # create sample data
        data = {
            "reference": ["1234"],
            "period": [202201],
            "unused": [0]}

        df = pd.DataFrame(data)
        return df

    def schema(self):
        # Create sample schema
        schema = {
            "ref": {'Deduced_Data_Type': 'Int64', 'old_name': 'reference'},
            "period": {'Deduced_Data_Type': 'Int64', 'old_name': 'period'},
            "unavailable": {'Deduced_Data_Type': 'Int64', 'old_name': 'not_ready'}}
        return schema

    def test_select_cols(self):
        # Call the create_output_df funtion
        df_input = self.data_frame()
        actual_result = create_output_df(df_input)

        expected_result = pd.DataFrame(
            {
                "ref": [1234],
                "period": [202201],
            }
        )

        assert_frame_equal(actual_result, expected_result)
