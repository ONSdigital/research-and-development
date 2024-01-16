import logging

from pandas._testing import assert_frame_equal
from pandas import DataFrame as pandasDF

from src.outlier_detection.auto_outliers import flag_outliers, decide_outliers, log_outlier_info, normal_round


class TestOutlierFlagging:
    """Unit tests for flag_outliers functtion."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "selectiontype",
            "statusencoded",
            "period",
            "cellnumber",
            "701",
        ]

        data = [
            [1, 0, "P", "210", 2020, 10, 1.1],
            [2, 0, "P", "210", 2020, 10, 1.2],
            [3, 0, "P", "210", 2020, 10, 1.3],
            [4, 0, "P", "210", 2020, 10, 1.4],
            [5, 0, "P", "210", 2020, 10, 1.5],
            [6, 0, "P", "210", 2020, 10, 1.6],
            [7, 0, "P", "210", 2020, 10, 1.7],
            [8, 0, "P", "210", 2020, 10, 1.8],
            [9, 0, "P", "210", 2020, 10, 1.9],
            [10, 0, "P", "210", 2020, 10, 2.0],
            [11, 0, "P", "210", 2020, 20, 2.1],
            [12, 0, "P", "210", 2020, 20, 2.2],
            [13, 0, "P", "210", 2020, 20, 2.3],
            [14, 0, "P", "210", 2020, 20, 2.4],
            [15, 0, "P", "210", 2020, 20, 2.5],
            [16, 0, "P", "210", 2020, 20, 2.6],
            [17, 0, "P", "210", 2020, 20, 2.7],
            [18, 0, "P", "210", 2020, 20, 2.8],
            [19, 0, "P", "210", 2020, 20, 2.9],
            [20, 0, "P", "210", 2020, 20, 3.0],
            [21, 0, "P", "210", 2020, 20, 0.0],
            [22, 0, "P", "210", 2020, 20, 0.0],
            [23, 0, "C", "210", 2020, 20, 4.0],
            [24, 0, "P", "999", 2020, 20, 5.0],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_expected_df(self):
        """Create dataframe for the expected output of the test."""
        exp_cols = [
            "reference",
            "instance",
            "selectiontype",
            "statusencoded",
            "period",
            "cellnumber",
            "701",
            "701_outlier_flag",
        ]

        data = [
            [1, 0, "P", "210", 2020, 10, 1.1, False],
            [2, 0, "P", "210", 2020, 10, 1.2, False],
            [3, 0, "P", "210", 2020, 10, 1.3, False],
            [4, 0, "P", "210", 2020, 10, 1.4, False],
            [5, 0, "P", "210", 2020, 10, 1.5, False],
            [6, 0, "P", "210", 2020, 10, 1.6, False],
            [7, 0, "P", "210", 2020, 10, 1.7, False],
            [8, 0, "P", "210", 2020, 10, 1.8, False],
            [9, 0, "P", "210", 2020, 10, 1.9, False],
            [10, 0, "P", "210", 2020, 10, 2.0, True],
            [11, 0, "P", "210", 2020, 20, 2.1, False],
            [12, 0, "P", "210", 2020, 20, 2.2, False],
            [13, 0, "P", "210", 2020, 20, 2.3, False],
            [14, 0, "P", "210", 2020, 20, 2.4, False],
            [15, 0, "P", "210", 2020, 20, 2.5, False],
            [16, 0, "P", "210", 2020, 20, 2.6, False],
            [17, 0, "P", "210", 2020, 20, 2.7, False],
            [18, 0, "P", "210", 2020, 20, 2.8, False],
            [19, 0, "P", "210", 2020, 20, 2.9, False],
            [20, 0, "P", "210", 2020, 20, 3.0, True],
            [21, 0, "P", "210", 2020, 20, 0.0, False],
            [22, 0, "P", "210", 2020, 20, 0.0, False],
            [23, 0, "C", "210", 2020, 20, 4.0, False],
            [24, 0, "P", "999", 2020, 20, 5.0, False],
        ]

        expected_df = pandasDF(data=data, columns=exp_cols)
        return expected_df

    def test_flag_outliers(self):
        """Test for flag_outliers function."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        upper_clip = 0.1
        lower_clip = 0
        value_col = "701"
        result_df = flag_outliers(input_df, upper_clip, lower_clip, value_col)

        assert_frame_equal(result_df, expected_df)


class TestDecideOutliers:
    """Unit tests for decide_outliers functtion."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "701_outlier_flag",
            "702_outlier_flag",
            "703_outlier_flag",
        ]

        data = [
            [1, True, False, False],
            [2, False, False, False],
            [3, False, True, False],
            [4, True, True, False],
            [5, True, True, True],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_expected_df(self):
        """Create an input dataframe for the test."""
        exp_cols = [
            "reference",
            "701_outlier_flag",
            "702_outlier_flag",
            "703_outlier_flag",
            "auto_outlier",
        ]

        data = [
            [1, True, False, False, True],
            [2, False, False, False, False],
            [3, False, True, False, True],
            [4, True, True, False, True],
            [5, True, True, True, True],
        ]

        exp_df = pandasDF(data=data, columns=exp_cols)
        return exp_df

    def test_decide_outliers(self):
        """Test for decide_outliers function."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        flag_cols = ["701", "702", "703"]
        result_df = decide_outliers(input_df, flag_cols)

        assert_frame_equal(result_df, expected_df)


class TestOutlierNumbers:
    """Unit tests for log_outlier_info functtion."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_columns = [
            "Reference",
            "selectiontype",
            "statusencoded",
            "value_col",
            "instance",
            "value_col_outlier_flag"
        ]

        data = [
            [1, "P", 210, 1, 0, True],
            [2, "P", 211, 1000, 0, False],
            [3, "X", 999, 0, 1, False],
            [4, "X", 211, 50, 0, False],
            [5, "P", 999, 50, 0, False],
            [6, "P", 211, -1, 0, False],
            [7, "P", 210, 50, 1, False],
        ]

        input_df = pandasDF(data=data, columns=input_columns)
        return input_df

    def test_log_outlier_info(self, caplog):
        """Test for log_outlier_info function."""

        input_df = self.create_input_df()

        with caplog.at_level(logging.INFO):
            log_outlier_info(input_df, "value_col")

        assert "1 outliers were detected out of a total of 2 valid entries in column value_col" in caplog.text


class TestNormalRound:
    """Unit tests for _normal_round functtion."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_columns = [
            "Reference",
            "to_round",
        ]

        data = [
            [1, 2.4],
            [2, 2.5],
            [3, 2.6],
            [4, 3.4],
            [5, 3.5],
            [6, 3.6],
        ]

        input_df = pandasDF(data=data, columns=input_columns)
        return input_df

    def create_expected_df(self):
        """Create an input dataframe for the test."""
        input_columns = [
            "Reference",
            "to_round",
            "rounded"
        ]

        data = [
            [1, 2.4, 2],
            [2, 2.5, 3],
            [3, 2.6, 3],
            [4, 3.4, 3],
            [5, 3.5, 4],
            [6, 3.6, 4],
        ]

        expected_df = pandasDF(data=data, columns=input_columns)
        return expected_df

    def test_normal_round(self):
        """Test for decide_outliers function."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        input_df["rounded"] = input_df.apply(
            lambda row: normal_round(row["to_round"]), axis=1
        )

        assert_frame_equal(input_df, expected_df)
