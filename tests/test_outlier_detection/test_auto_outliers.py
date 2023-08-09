from pandas._testing import assert_frame_equal
from pandas import DataFrame as pandasDF

from src.outlier_detection.auto_outliers import flag_outliers


class TestOutlierFlagging:
    """Unit tests for flag_outliers functtion."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = ["reference", "selectiontype", "period", "cellnumber", "value"]

        data = [
            [1, "P", 2020, 10, 1.1],
            [2, "P", 2020, 10, 1.2],
            [3, "P", 2020, 10, 1.3],
            [4, "P", 2020, 10, 1.4],
            [5, "P", 2020, 10, 1.5],
            [6, "P", 2020, 10, 1.6],
            [7, "P", 2020, 10, 1.7],
            [8, "P", 2020, 10, 1.8],
            [9, "P", 2020, 10, 1.9],
            [10, "P", 2020, 10, 2.0],
            [11, "P", 2020, 20, 2.1],
            [12, "P", 2020, 20, 2.2],
            [13, "P", 2020, 20, 2.3],
            [14, "P", 2020, 20, 2.4],
            [15, "P", 2020, 20, 2.5],
            [16, "P", 2020, 20, 2.6],
            [17, "P", 2020, 20, 2.7],
            [18, "P", 2020, 20, 2.8],
            [19, "P", 2020, 20, 2.9],
            [20, "P", 2020, 20, 3.0],
            [21, "P", 2020, 20, 0.0],
            [22, "P", 2020, 20, 0.0],
            [23, "C", 2020, 20, 4.0],         
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_expected_df(self):
        """Create dataframe for the expected output of the test."""
        exp_cols = [
            "reference",
            "selectiontype",
            "period",
            "cellnumber",
            "value",
            "auto_outlier",
        ]

        data = [
            [1, "P", 2020, 10, 1.1, False],
            [2, "P", 2020, 10, 1.2, False],
            [3, "P", 2020, 10, 1.3, False],
            [4, "P", 2020, 10, 1.4, False],
            [5, "P", 2020, 10, 1.5, False],
            [6, "P", 2020, 10, 1.6, False],
            [7, "P", 2020, 10, 1.7, False],
            [8, "P", 2020, 10, 1.8, False],
            [9, "P", 2020, 10, 1.9, False],
            [10, "P", 2020, 10, 2.0, True],
            [11, "P", 2020, 20, 2.1, False],
            [12, "P", 2020, 20, 2.2, False],
            [13, "P", 2020, 20, 2.3, False],
            [14, "P", 2020, 20, 2.4, False],
            [15, "P", 2020, 20, 2.5, False],
            [16, "P", 2020, 20, 2.6, False],
            [17, "P", 2020, 20, 2.7, False],
            [18, "P", 2020, 20, 2.8, False],
            [19, "P", 2020, 20, 2.9, False],
            [20, "P", 2020, 20, 3.0, True],
            [21, "P", 2020, 20, 0.0, False],
            [22, "P", 2020, 20, 0.0, False],
            [23, "C", 2020, 20, 4.0, False],     
        ]

        expected_df = pandasDF(data=data, columns=exp_cols)
        return expected_df

    def test_flag_outliers(self):
        """Test for flag_outliers function."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        upper_clip = 0.1
        lower_clip = 0
        groupby_cols: list = ["period", "cellnumber"]
        value_col: str = "value"
        result_df = flag_outliers(
            input_df, upper_clip, lower_clip, groupby_cols, value_col
        )

        assert_frame_equal(result_df, expected_df)
