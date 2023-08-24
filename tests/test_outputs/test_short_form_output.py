import numpy as np
import pandas as pd
import unittest
from pandas._testing import assert_frame_equal

from src.outputs.short_form_out import create_new_cols


class TestCreateNewCols(unittest.TestCase):
    """Unittest for create news columns"""

    def data_frame(self):
        # create sample data
        data = {
            "ref": ["4990405", "4990404", "4990403", "4990402"],
            "period": ["202212", "202212", "202212", "202212"],
            "715": [116, 187, 958, 158],
        }
        df = pd.DataFrame(data)
        return df

    def test_create_new_cols(self):
        # Call the create_new_cols funtion
        df_input = self.data_frame()
        actual_result = create_new_cols(df_input)

        expected_result = pd.DataFrame(
            {
                "ref": ["4990405", "4990404", "4990403", "4990402"],
                "period": ["202212", "202212", "202212", "202212"],
                "715": [116, 187, 958, 158],
                "freeze_id": [np.nan, np.nan, np.nan, np.nan],
                "inquiry_id": [np.nan, np.nan, np.nan, np.nan],
                "period_contributor_id": [np.nan, np.nan, np.nan, np.nan],
                "post_code": [np.nan, np.nan, np.nan, np.nan],
                "ua_county": [np.nan, np.nan, np.nan, np.nan],
                "wowentref": [np.nan, np.nan, np.nan, np.nan],
                "foreign_owner": [np.nan, np.nan, np.nan, np.nan],
                "product_group": [np.nan, np.nan, np.nan, np.nan],
                "sizeband": [np.nan, np.nan, np.nan, np.nan],
                "period_year": ["2022", "2022", "2022", "2022"],
            }
        )

        assert_frame_equal(actual_result, expected_result)
