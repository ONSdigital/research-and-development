import logging

from pandas._testing import assert_frame_equal
from pandas import DataFrame as pandasDF
import numpy as np
import pytest

import src.outlier_detection.auto_outliers as auto


# Four tests for `validate_config()`:
# Test for warning (where both clips = 0)
# Test for error (where a clip is a minus number)
# Test for error (where a clip is over 1)
# Test for error (where flag_value_cols is not a list)


class TestValidateConfigZero:
    """Unit tests for validate_config function."""

    def test_validate_config(self, caplog):
        """Test for validate_config function."""

        with caplog.at_level(logging.WARNING):
            auto.validate_config(0.0, 0.0, ["701", "702", "703"])

        assert "upper_clip and lower_clip both zero:" in caplog.text


class TestValidateConfigMinus:
    """Unit tests for validate_config function."""

    def test_validate_config(self, caplog):
        """Test for validate_config function."""

        with pytest.raises(ImportError):
            auto.validate_config(-1.0, 0.0, ["701", "702", "703"])


class TestValidateConfigOverOne:
    """Unit tests for validate_config function."""

    def test_validate_config(self, caplog):
        """Test for validate_config function."""

        with pytest.raises(ImportError):
            auto.validate_config(0.0, 2.0, ["701", "702", "703"])


class TestValidateConfigNotList:
    """Unit tests for validate_config function."""

    def test_validate_config(self, caplog):
        """Test for validate_config function."""

        with pytest.raises(ImportError):
            auto.validate_config(0.05, 0.0, ("701", "702", "703"))


# Two tests for `filter_valid()`:
# Test that the filtering removes the correct rows
# Test an error is raised if df empty, and log returned


class TestFilterValid:
    """Unit tests for filter_valid function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "selectiontype",
            "status",
            "period",
            "cellnumber",
            "701",
        ]

        data = [
            [1, 1, "P", "Clear", 2020, 10, 1.1],
            [2, 0, "P", "Clear", 2020, 10, 1.2],
            [3, 0, "P", "Check needed", 2020, 10, 1.3],
            [4, 0, "C", "Clear", 2020, 10, 1.4],
            [5, 0, "P", "Clear", 2020, 10, 0],
            [6, 0, "P", "Clear", 2020, 10, 1.6],
            [7, 0, "P", "Clear", 2020, 10, 1.7],
            [8, 0, "P", "Clear", 2020, 10, 1.8],
            [9, 0, "P", "Clear", 2020, 10, 1.9],
            [10, 1, "C", "Check needed", 2020, 10, 2.0],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_expected_df(self):
        """Create dataframe for the expected output of the test."""
        exp_cols = [
            "reference",
            "instance",
            "selectiontype",
            "status",
            "period",
            "cellnumber",
            "701",
        ]

        data = [
            [2, 0, "P", "Clear", 2020, 10, 1.2],
            [6, 0, "P", "Clear", 2020, 10, 1.6],
            [7, 0, "P", "Clear", 2020, 10, 1.7],
            [8, 0, "P", "Clear", 2020, 10, 1.8],
            [9, 0, "P", "Clear", 2020, 10, 1.9],
        ]

        expected_df = pandasDF(data=data, columns=exp_cols)
        return expected_df

    def test_filter_valid(self):
        """Test for filter_valid function."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        value_col = "701"
        result_df = auto.filter_valid(input_df, value_col)

        assert_frame_equal(
            result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
        )


class TestFilterValidEmpty:
    """Unit tests for filter_valid function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "selectiontype",
            "status",
            "period",
            "cellnumber",
            "701",
        ]

        data = []

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def test_filter_valid(self, caplog):
        """Test for filter_valid function."""
        input_df = self.create_input_df()

        with pytest.raises(ValueError):
            auto.filter_valid(input_df, "701")

        assert "has no valid returns for outliers" in caplog.text


# Seven tests for `flag_outliers()`:
# Test the correct rows are flagged as outliers
# Test that no outliers are flagged if both clips = 0
# Tests for the rounding for the number of outliers flag is correct -
# this is done in five tests where the clip remains the same, but the size
# of the dataframe changes (there is a `_normal_rounding()` function
# within `flag_outliers()` to change rounding from banking rounding to
# simple rounding)


class TestOutlierFlagging:
    """Unit tests for flag_outliers function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "selectiontype",
            "status",
            "period",
            "cellnumber",
            "701",
        ]

        data = [
            [1, 0, "P", "Clear", 2020, 10, 1.1],
            [2, 0, "P", "Clear", 2020, 10, 1.2],
            [3, 0, "P", "Clear", 2020, 10, 1.3],
            [4, 0, "P", "Clear", 2020, 10, 1.4],
            [5, 0, "P", "Clear", 2020, 10, 1.5],
            [6, 0, "P", "Clear", 2020, 10, 1.6],
            [7, 0, "P", "Clear", 2020, 10, 1.7],
            [8, 0, "P", "Clear", 2020, 10, 1.8],
            [9, 0, "P", "Clear", 2020, 10, 1.9],
            [10, 0, "P", "Clear", 2020, 10, 2.0],
            [11, 0, "P", "Clear", 2020, 20, 2.1],
            [12, 0, "P", "Clear", 2020, 20, 2.2],
            [13, 0, "P", "Clear", 2020, 20, 2.3],
            [14, 0, "P", "Clear", 2020, 20, 2.4],
            [15, 0, "P", "Clear", 2020, 20, 2.5],
            [16, 0, "P", "Clear", 2020, 20, 2.6],
            [17, 0, "P", "Clear", 2020, 20, 2.7],
            [18, 0, "P", "Clear", 2020, 20, 2.8],
            [19, 0, "P", "Clear", 2020, 20, 2.9],
            [20, 0, "P", "Clear", 2020, 20, 3.0],
            [21, 0, "P", "Clear", 2020, 20, 0.0],
            [22, 0, "P", "Clear", 2020, 20, 0.0],
            [23, 0, "C", "Clear", 2020, 20, 4.0],
            [24, 0, "P", "Check needed", 2020, 20, 5.0],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_expected_df(self):
        """Create dataframe for the expected output of the test."""
        exp_cols = [
            "reference",
            "instance",
            "selectiontype",
            "status",
            "period",
            "cellnumber",
            "701",
            "701_outlier_flag",
        ]

        data = [
            [1, 0, "P", "Clear", 2020, 10, 1.1, False],
            [2, 0, "P", "Clear", 2020, 10, 1.2, False],
            [3, 0, "P", "Clear", 2020, 10, 1.3, False],
            [4, 0, "P", "Clear", 2020, 10, 1.4, False],
            [5, 0, "P", "Clear", 2020, 10, 1.5, False],
            [6, 0, "P", "Clear", 2020, 10, 1.6, False],
            [7, 0, "P", "Clear", 2020, 10, 1.7, False],
            [8, 0, "P", "Clear", 2020, 10, 1.8, False],
            [9, 0, "P", "Clear", 2020, 10, 1.9, False],
            [10, 0, "P", "Clear", 2020, 10, 2.0, True],
            [11, 0, "P", "Clear", 2020, 20, 2.1, False],
            [12, 0, "P", "Clear", 2020, 20, 2.2, False],
            [13, 0, "P", "Clear", 2020, 20, 2.3, False],
            [14, 0, "P", "Clear", 2020, 20, 2.4, False],
            [15, 0, "P", "Clear", 2020, 20, 2.5, False],
            [16, 0, "P", "Clear", 2020, 20, 2.6, False],
            [17, 0, "P", "Clear", 2020, 20, 2.7, False],
            [18, 0, "P", "Clear", 2020, 20, 2.8, False],
            [19, 0, "P", "Clear", 2020, 20, 2.9, False],
            [20, 0, "P", "Clear", 2020, 20, 3.0, True],
            [21, 0, "P", "Clear", 2020, 20, 0.0, False],
            [22, 0, "P", "Clear", 2020, 20, 0.0, False],
            [23, 0, "C", "Clear", 2020, 20, 4.0, False],
            [24, 0, "P", "Check needed", 2020, 20, 5.0, False],
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
        result_df = auto.flag_outliers(input_df, upper_clip, lower_clip, value_col)

        assert_frame_equal(result_df, expected_df)


class TestNoOutliers:
    """Unit tests for flag_outliers function."""

    """No outliers to calculate"""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "selectiontype",
            "status",
            "period",
            "cellnumber",
            "701",
        ]

        data = [
            [1, 0, "P", "Clear", 2020, 10, 1.1],
            [2, 0, "P", "Clear", 2020, 10, 1.1],
            [3, 0, "P", "Clear", 2020, 10, 1.1],
            [4, 0, "P", "Clear", 2020, 10, 1.1],
            [5, 0, "P", "Clear", 2020, 10, 1.1],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_expected_df(self):
        """Create dataframe for the expected output of the test."""
        exp_cols = [
            "reference",
            "instance",
            "selectiontype",
            "status",
            "period",
            "cellnumber",
            "701",
            "701_outlier_flag",
        ]

        data = [
            [1, 0, "P", "Clear", 2020, 10, 1.1, False],
            [2, 0, "P", "Clear", 2020, 10, 1.1, False],
            [3, 0, "P", "Clear", 2020, 10, 1.1, False],
            [4, 0, "P", "Clear", 2020, 10, 1.1, False],
            [5, 0, "P", "Clear", 2020, 10, 1.1, False],
        ]

        expected_df = pandasDF(data=data, columns=exp_cols)
        return expected_df

    def test_flag_outliers(self):
        """Test for flag_outliers function."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        upper_clip = 0
        lower_clip = 0
        value_col = "701"
        result_df = auto.flag_outliers(input_df, upper_clip, lower_clip, value_col)

        assert_frame_equal(result_df, expected_df)


class TestRoundingOutliers1:
    """Unit tests for flag_outliers function."""

    """Checking correct rounding 1/5"""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "selectiontype",
            "status",
            "period",
            "cellnumber",
            "701",
        ]

        data = [
            [1, 0, "P", "Clear", 2020, 10, 1.0],
            [2, 0, "P", "Clear", 2020, 10, 1.1],
            [3, 0, "P", "Clear", 2020, 10, 1.2],
            [4, 0, "P", "Clear", 2020, 10, 1.3],
            [5, 0, "P", "Clear", 2020, 10, 1.4],
            [6, 0, "P", "Clear", 2020, 10, 1.5],
            [7, 0, "P", "Clear", 2020, 10, 1.6],
            [8, 0, "P", "Clear", 2020, 10, 1.7],
            [9, 0, "P", "Clear", 2020, 10, 1.8],
            [10, 0, "P", "Clear", 2020, 10, 1.9],
            [11, 0, "P", "Clear", 2020, 10, 2.0],
            [12, 0, "P", "Clear", 2020, 10, 2.1],
            [13, 0, "P", "Clear", 2020, 10, 2.2],
            [14, 0, "P", "Clear", 2020, 10, 2.3],
            [15, 0, "P", "Clear", 2020, 10, 2.4],
            [16, 0, "P", "Clear", 2020, 10, 2.5],
            [17, 0, "P", "Clear", 2020, 10, 2.6],
            [18, 0, "P", "Clear", 2020, 10, 2.7],
            [19, 0, "P", "Clear", 2020, 10, 2.8],
            [20, 0, "P", "Clear", 2020, 10, 2.9],
            [21, 0, "P", "Clear", 2020, 10, 1.0],
            [22, 0, "P", "Clear", 2020, 10, 1.1],
            [23, 0, "P", "Clear", 2020, 10, 1.2],
            [24, 0, "P", "Clear", 2020, 10, 1.3],
            [25, 0, "P", "Clear", 2020, 10, 1.4],
            [26, 0, "P", "Clear", 2020, 10, 1.5],
            [27, 0, "P", "Clear", 2020, 10, 1.6],
            [28, 0, "P", "Clear", 2020, 10, 1.7],
            [29, 0, "P", "Clear", 2020, 10, 1.8],
            [30, 0, "P", "Clear", 2020, 10, 1.9],
            [31, 0, "P", "Clear", 2020, 10, 2.0],
            [32, 0, "P", "Clear", 2020, 10, 2.1],
            [33, 0, "P", "Clear", 2020, 10, 2.2],
            [34, 0, "P", "Clear", 2020, 10, 2.3],
            [35, 0, "P", "Clear", 2020, 10, 2.4],
            [36, 0, "P", "Clear", 2020, 10, 2.5],
            [37, 0, "P", "Clear", 2020, 10, 2.6],
            [38, 0, "P", "Clear", 2020, 10, 2.7],
            [39, 0, "P", "Clear", 2020, 10, 2.8],
            [40, 0, "P", "Clear", 2020, 10, 2.9],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_expected_df(self):
        """Create dataframe for the expected output of the test."""
        exp_cols = [
            "reference",
            "instance",
            "selectiontype",
            "status",
            "period",
            "cellnumber",
            "701",
            "701_outlier_flag",
        ]

        data = [
            [1, 0, "P", "Clear", 2020, 10, 1.0, False],
            [2, 0, "P", "Clear", 2020, 10, 1.1, False],
            [3, 0, "P", "Clear", 2020, 10, 1.2, False],
            [4, 0, "P", "Clear", 2020, 10, 1.3, False],
            [5, 0, "P", "Clear", 2020, 10, 1.4, False],
            [6, 0, "P", "Clear", 2020, 10, 1.5, False],
            [7, 0, "P", "Clear", 2020, 10, 1.6, False],
            [8, 0, "P", "Clear", 2020, 10, 1.7, False],
            [9, 0, "P", "Clear", 2020, 10, 1.8, False],
            [10, 0, "P", "Clear", 2020, 10, 1.9, False],
            [11, 0, "P", "Clear", 2020, 10, 2.0, False],
            [12, 0, "P", "Clear", 2020, 10, 2.1, False],
            [13, 0, "P", "Clear", 2020, 10, 2.2, False],
            [14, 0, "P", "Clear", 2020, 10, 2.3, False],
            [15, 0, "P", "Clear", 2020, 10, 2.4, False],
            [16, 0, "P", "Clear", 2020, 10, 2.5, False],
            [17, 0, "P", "Clear", 2020, 10, 2.6, False],
            [18, 0, "P", "Clear", 2020, 10, 2.7, False],
            [19, 0, "P", "Clear", 2020, 10, 2.8, False],
            [20, 0, "P", "Clear", 2020, 10, 2.9, True],
            [21, 0, "P", "Clear", 2020, 10, 1.0, False],
            [22, 0, "P", "Clear", 2020, 10, 1.1, False],
            [23, 0, "P", "Clear", 2020, 10, 1.2, False],
            [24, 0, "P", "Clear", 2020, 10, 1.3, False],
            [25, 0, "P", "Clear", 2020, 10, 1.4, False],
            [26, 0, "P", "Clear", 2020, 10, 1.5, False],
            [27, 0, "P", "Clear", 2020, 10, 1.6, False],
            [28, 0, "P", "Clear", 2020, 10, 1.7, False],
            [29, 0, "P", "Clear", 2020, 10, 1.8, False],
            [30, 0, "P", "Clear", 2020, 10, 1.9, False],
            [31, 0, "P", "Clear", 2020, 10, 2.0, False],
            [32, 0, "P", "Clear", 2020, 10, 2.1, False],
            [33, 0, "P", "Clear", 2020, 10, 2.2, False],
            [34, 0, "P", "Clear", 2020, 10, 2.3, False],
            [35, 0, "P", "Clear", 2020, 10, 2.4, False],
            [36, 0, "P", "Clear", 2020, 10, 2.5, False],
            [37, 0, "P", "Clear", 2020, 10, 2.6, False],
            [38, 0, "P", "Clear", 2020, 10, 2.7, False],
            [39, 0, "P", "Clear", 2020, 10, 2.8, False],
            [40, 0, "P", "Clear", 2020, 10, 2.9, True],
        ]

        expected_df = pandasDF(data=data, columns=exp_cols)
        return expected_df

    def test_flag_outliers(self):
        """Test for flag_outliers function."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        upper_clip = 0.05
        lower_clip = 0
        value_col = "701"
        result_df = auto.flag_outliers(input_df, upper_clip, lower_clip, value_col)

        assert_frame_equal(result_df, expected_df)


class TestRoundingOutliers2:
    """Unit tests for flag_outliers function."""

    """Checking correct rounding 2/5"""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "selectiontype",
            "status",
            "period",
            "cellnumber",
            "701",
        ]

        data = [
            [1, 0, "P", "Clear", 2020, 10, 1.0],
            [2, 0, "P", "Clear", 2020, 10, 1.1],
            [3, 0, "P", "Clear", 2020, 10, 1.2],
            [4, 0, "P", "Clear", 2020, 10, 1.3],
            [5, 0, "P", "Clear", 2020, 10, 1.4],
            [6, 0, "P", "Clear", 2020, 10, 1.5],
            [7, 0, "P", "Clear", 2020, 10, 1.6],
            [8, 0, "P", "Clear", 2020, 10, 1.7],
            [9, 0, "P", "Clear", 2020, 10, 1.8],
            [10, 0, "P", "Clear", 2020, 10, 1.9],
            [11, 0, "P", "Clear", 2020, 10, 2.0],
            [12, 0, "P", "Clear", 2020, 10, 2.1],
            [13, 0, "P", "Clear", 2020, 10, 2.2],
            [14, 0, "P", "Clear", 2020, 10, 2.3],
            [15, 0, "P", "Clear", 2020, 10, 2.4],
            [16, 0, "P", "Clear", 2020, 10, 2.5],
            [17, 0, "P", "Clear", 2020, 10, 2.6],
            [18, 0, "P", "Clear", 2020, 10, 2.7],
            [19, 0, "P", "Clear", 2020, 10, 2.8],
            [20, 0, "P", "Clear", 2020, 10, 2.9],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_expected_df(self):
        """Create dataframe for the expected output of the test."""
        exp_cols = [
            "reference",
            "instance",
            "selectiontype",
            "status",
            "period",
            "cellnumber",
            "701",
            "701_outlier_flag",
        ]

        data = [
            [1, 0, "P", "Clear", 2020, 10, 1.0, False],
            [2, 0, "P", "Clear", 2020, 10, 1.1, False],
            [3, 0, "P", "Clear", 2020, 10, 1.2, False],
            [4, 0, "P", "Clear", 2020, 10, 1.3, False],
            [5, 0, "P", "Clear", 2020, 10, 1.4, False],
            [6, 0, "P", "Clear", 2020, 10, 1.5, False],
            [7, 0, "P", "Clear", 2020, 10, 1.6, False],
            [8, 0, "P", "Clear", 2020, 10, 1.7, False],
            [9, 0, "P", "Clear", 2020, 10, 1.8, False],
            [10, 0, "P", "Clear", 2020, 10, 1.9, False],
            [11, 0, "P", "Clear", 2020, 10, 2.0, False],
            [12, 0, "P", "Clear", 2020, 10, 2.1, False],
            [13, 0, "P", "Clear", 2020, 10, 2.2, False],
            [14, 0, "P", "Clear", 2020, 10, 2.3, False],
            [15, 0, "P", "Clear", 2020, 10, 2.4, False],
            [16, 0, "P", "Clear", 2020, 10, 2.5, False],
            [17, 0, "P", "Clear", 2020, 10, 2.6, False],
            [18, 0, "P", "Clear", 2020, 10, 2.7, False],
            [19, 0, "P", "Clear", 2020, 10, 2.8, False],
            [20, 0, "P", "Clear", 2020, 10, 2.9, True],
        ]

        expected_df = pandasDF(data=data, columns=exp_cols)
        return expected_df

    def test_flag_outliers(self):
        """Test for flag_outliers function."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        upper_clip = 0.05
        lower_clip = 0
        value_col = "701"
        result_df = auto.flag_outliers(input_df, upper_clip, lower_clip, value_col)

        assert_frame_equal(result_df, expected_df)


class TestRoundingOutliers3:
    """Unit tests for flag_outliers function."""

    """Checking correct rounding 3/5"""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "selectiontype",
            "status",
            "period",
            "cellnumber",
            "701",
        ]

        data = [
            [1, 0, "P", "Clear", 2020, 10, 1.0],
            [2, 0, "P", "Clear", 2020, 10, 1.1],
            [3, 0, "P", "Clear", 2020, 10, 1.2],
            [4, 0, "P", "Clear", 2020, 10, 1.3],
            [5, 0, "P", "Clear", 2020, 10, 1.4],
            [6, 0, "P", "Clear", 2020, 10, 1.5],
            [7, 0, "P", "Clear", 2020, 10, 1.6],
            [8, 0, "P", "Clear", 2020, 10, 1.7],
            [9, 0, "P", "Clear", 2020, 10, 1.8],
            [10, 0, "P", "Clear", 2020, 10, 1.9],
            [11, 0, "P", "Clear", 2020, 10, 2.0],
            [12, 0, "P", "Clear", 2020, 10, 2.1],
            [13, 0, "P", "Clear", 2020, 10, 2.2],
            [14, 0, "P", "Clear", 2020, 10, 2.3],
            [15, 0, "P", "Clear", 2020, 10, 2.4],
            [16, 0, "P", "Clear", 2020, 10, 2.5],
            [17, 0, "P", "Clear", 2020, 10, 2.6],
            [18, 0, "P", "Clear", 2020, 10, 2.7],
            [19, 0, "P", "Clear", 2020, 10, 2.8],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_expected_df(self):
        """Create dataframe for the expected output of the test."""
        exp_cols = [
            "reference",
            "instance",
            "selectiontype",
            "status",
            "period",
            "cellnumber",
            "701",
            "701_outlier_flag",
        ]

        data = [
            [1, 0, "P", "Clear", 2020, 10, 1.0, False],
            [2, 0, "P", "Clear", 2020, 10, 1.1, False],
            [3, 0, "P", "Clear", 2020, 10, 1.2, False],
            [4, 0, "P", "Clear", 2020, 10, 1.3, False],
            [5, 0, "P", "Clear", 2020, 10, 1.4, False],
            [6, 0, "P", "Clear", 2020, 10, 1.5, False],
            [7, 0, "P", "Clear", 2020, 10, 1.6, False],
            [8, 0, "P", "Clear", 2020, 10, 1.7, False],
            [9, 0, "P", "Clear", 2020, 10, 1.8, False],
            [10, 0, "P", "Clear", 2020, 10, 1.9, False],
            [11, 0, "P", "Clear", 2020, 10, 2.0, False],
            [12, 0, "P", "Clear", 2020, 10, 2.1, False],
            [13, 0, "P", "Clear", 2020, 10, 2.2, False],
            [14, 0, "P", "Clear", 2020, 10, 2.3, False],
            [15, 0, "P", "Clear", 2020, 10, 2.4, False],
            [16, 0, "P", "Clear", 2020, 10, 2.5, False],
            [17, 0, "P", "Clear", 2020, 10, 2.6, False],
            [18, 0, "P", "Clear", 2020, 10, 2.7, False],
            [19, 0, "P", "Clear", 2020, 10, 2.8, True],
        ]

        expected_df = pandasDF(data=data, columns=exp_cols)
        return expected_df

    def test_flag_outliers(self):
        """Test for flag_outliers function."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        upper_clip = 0.05
        lower_clip = 0
        value_col = "701"
        result_df = auto.flag_outliers(input_df, upper_clip, lower_clip, value_col)

        assert_frame_equal(result_df, expected_df)


class TestRoundingOutliers4:
    """Unit tests for flag_outliers function."""

    """Checking correct rounding 4/5"""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "selectiontype",
            "status",
            "period",
            "cellnumber",
            "701",
        ]

        data = [
            [1, 0, "P", "Clear", 2020, 10, 1.0],
            [2, 0, "P", "Clear", 2020, 10, 1.1],
            [3, 0, "P", "Clear", 2020, 10, 1.2],
            [4, 0, "P", "Clear", 2020, 10, 1.3],
            [5, 0, "P", "Clear", 2020, 10, 1.4],
            [6, 0, "P", "Clear", 2020, 10, 1.5],
            [7, 0, "P", "Clear", 2020, 10, 1.6],
            [8, 0, "P", "Clear", 2020, 10, 1.7],
            [9, 0, "P", "Clear", 2020, 10, 1.8],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_expected_df(self):
        """Create dataframe for the expected output of the test."""
        exp_cols = [
            "reference",
            "instance",
            "selectiontype",
            "status",
            "period",
            "cellnumber",
            "701",
            "701_outlier_flag",
        ]

        data = [
            [1, 0, "P", "Clear", 2020, 10, 1.0, False],
            [2, 0, "P", "Clear", 2020, 10, 1.1, False],
            [3, 0, "P", "Clear", 2020, 10, 1.2, False],
            [4, 0, "P", "Clear", 2020, 10, 1.3, False],
            [5, 0, "P", "Clear", 2020, 10, 1.4, False],
            [6, 0, "P", "Clear", 2020, 10, 1.5, False],
            [7, 0, "P", "Clear", 2020, 10, 1.6, False],
            [8, 0, "P", "Clear", 2020, 10, 1.7, False],
            [9, 0, "P", "Clear", 2020, 10, 1.8, False],
        ]

        expected_df = pandasDF(data=data, columns=exp_cols)
        return expected_df

    def test_flag_outliers(self):
        """Test for flag_outliers function."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        upper_clip = 0.05
        lower_clip = 0
        value_col = "701"
        result_df = auto.flag_outliers(input_df, upper_clip, lower_clip, value_col)

        assert_frame_equal(result_df, expected_df)


class TestRoundingOutliers5:
    """Unit tests for flag_outliers function."""

    """Checking correct rounding 5/5"""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "selectiontype",
            "status",
            "period",
            "cellnumber",
            "701",
        ]

        data = [
            [1, 0, "P", "Clear", 2020, 10, 1.0],
            [2, 0, "P", "Clear", 2020, 10, 1.1],
            [3, 0, "P", "Clear", 2020, 10, 1.2],
            [4, 0, "P", "Clear", 2020, 10, 1.3],
            [5, 0, "P", "Clear", 2020, 10, 1.4],
            [6, 0, "P", "Clear", 2020, 10, 1.5],
            [7, 0, "P", "Clear", 2020, 10, 1.6],
            [8, 0, "P", "Clear", 2020, 10, 1.7],
            [9, 0, "P", "Clear", 2020, 10, 1.8],
            [10, 0, "P", "Clear", 2020, 10, 1.9],
            [11, 0, "P", "Clear", 2020, 10, 2.0],
            [12, 0, "P", "Clear", 2020, 10, 2.1],
            [13, 0, "P", "Clear", 2020, 10, 2.2],
            [14, 0, "P", "Clear", 2020, 10, 2.3],
            [15, 0, "P", "Clear", 2020, 10, 2.4],
            [16, 0, "P", "Clear", 2020, 10, 2.5],
            [17, 0, "P", "Clear", 2020, 10, 2.6],
            [18, 0, "P", "Clear", 2020, 10, 2.7],
            [19, 0, "P", "Clear", 2020, 10, 2.8],
            [20, 0, "P", "Clear", 2020, 10, 2.9],
            [21, 0, "P", "Clear", 2020, 10, 1.0],
            [22, 0, "P", "Clear", 2020, 10, 1.1],
            [23, 0, "P", "Clear", 2020, 10, 1.2],
            [24, 0, "P", "Clear", 2020, 10, 1.3],
            [25, 0, "P", "Clear", 2020, 10, 1.4],
            [26, 0, "P", "Clear", 2020, 10, 1.5],
            [27, 0, "P", "Clear", 2020, 10, 1.6],
            [28, 0, "P", "Clear", 2020, 10, 1.7],
            [29, 0, "P", "Clear", 2020, 10, 1.8],
            [30, 0, "P", "Clear", 2020, 10, 1.9],
            [31, 0, "P", "Clear", 2020, 10, 2.0],
            [32, 0, "P", "Clear", 2020, 10, 2.1],
            [33, 0, "P", "Clear", 2020, 10, 2.2],
            [34, 0, "P", "Clear", 2020, 10, 2.3],
            [35, 0, "P", "Clear", 2020, 10, 2.4],
            [36, 0, "P", "Clear", 2020, 10, 2.5],
            [37, 0, "P", "Clear", 2020, 10, 2.6],
            [38, 0, "P", "Clear", 2020, 10, 2.7],
            [39, 0, "P", "Clear", 2020, 10, 2.8],
            [40, 0, "P", "Clear", 2020, 10, 2.9],
            [41, 0, "P", "Clear", 2020, 10, 1.5],
            [42, 0, "P", "Clear", 2020, 10, 1.5],
            [43, 0, "P", "Clear", 2020, 10, 1.5],
            [44, 0, "P", "Clear", 2020, 10, 1.5],
            [45, 0, "P", "Clear", 2020, 10, 1.5],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_expected_df(self):
        """Create dataframe for the expected output of the test."""
        exp_cols = [
            "reference",
            "instance",
            "selectiontype",
            "status",
            "period",
            "cellnumber",
            "701",
            "701_outlier_flag",
        ]

        data = [
            [1, 0, "P", "Clear", 2020, 10, 1.0, False],
            [2, 0, "P", "Clear", 2020, 10, 1.1, False],
            [3, 0, "P", "Clear", 2020, 10, 1.2, False],
            [4, 0, "P", "Clear", 2020, 10, 1.3, False],
            [5, 0, "P", "Clear", 2020, 10, 1.4, False],
            [6, 0, "P", "Clear", 2020, 10, 1.5, False],
            [7, 0, "P", "Clear", 2020, 10, 1.6, False],
            [8, 0, "P", "Clear", 2020, 10, 1.7, False],
            [9, 0, "P", "Clear", 2020, 10, 1.8, False],
            [10, 0, "P", "Clear", 2020, 10, 1.9, False],
            [11, 0, "P", "Clear", 2020, 10, 2.0, False],
            [12, 0, "P", "Clear", 2020, 10, 2.1, False],
            [13, 0, "P", "Clear", 2020, 10, 2.2, False],
            [14, 0, "P", "Clear", 2020, 10, 2.3, False],
            [15, 0, "P", "Clear", 2020, 10, 2.4, False],
            [16, 0, "P", "Clear", 2020, 10, 2.5, False],
            [17, 0, "P", "Clear", 2020, 10, 2.6, False],
            [18, 0, "P", "Clear", 2020, 10, 2.7, False],
            [19, 0, "P", "Clear", 2020, 10, 2.8, False],
            [20, 0, "P", "Clear", 2020, 10, 2.9, True],
            [21, 0, "P", "Clear", 2020, 10, 1.0, False],
            [22, 0, "P", "Clear", 2020, 10, 1.1, False],
            [23, 0, "P", "Clear", 2020, 10, 1.2, False],
            [24, 0, "P", "Clear", 2020, 10, 1.3, False],
            [25, 0, "P", "Clear", 2020, 10, 1.4, False],
            [26, 0, "P", "Clear", 2020, 10, 1.5, False],
            [27, 0, "P", "Clear", 2020, 10, 1.6, False],
            [28, 0, "P", "Clear", 2020, 10, 1.7, False],
            [29, 0, "P", "Clear", 2020, 10, 1.8, False],
            [30, 0, "P", "Clear", 2020, 10, 1.9, False],
            [31, 0, "P", "Clear", 2020, 10, 2.0, False],
            [32, 0, "P", "Clear", 2020, 10, 2.1, False],
            [33, 0, "P", "Clear", 2020, 10, 2.2, False],
            [34, 0, "P", "Clear", 2020, 10, 2.3, False],
            [35, 0, "P", "Clear", 2020, 10, 2.4, False],
            [36, 0, "P", "Clear", 2020, 10, 2.5, False],
            [37, 0, "P", "Clear", 2020, 10, 2.6, False],
            [38, 0, "P", "Clear", 2020, 10, 2.7, False],
            [39, 0, "P", "Clear", 2020, 10, 2.8, False],
            [40, 0, "P", "Clear", 2020, 10, 2.9, True],
            [41, 0, "P", "Clear", 2020, 10, 1.5, False],
            [42, 0, "P", "Clear", 2020, 10, 1.5, False],
            [43, 0, "P", "Clear", 2020, 10, 1.5, False],
            [44, 0, "P", "Clear", 2020, 10, 1.5, False],
            [45, 0, "P", "Clear", 2020, 10, 1.5, False],
        ]

        expected_df = pandasDF(data=data, columns=exp_cols)
        return expected_df

    def test_flag_outliers(self):
        """Test for flag_outliers function."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        upper_clip = 0.05
        lower_clip = 0
        value_col = "701"
        result_df = auto.flag_outliers(input_df, upper_clip, lower_clip, value_col)

        assert_frame_equal(result_df, expected_df)


# One test for `decide_outliers()`:
# Test that one outlier flag will flag the whole row as an outlier


class TestDecideOutliers:
    """Unit tests for decide_outliers function."""

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
        result_df = auto.decide_outliers(input_df, flag_cols)

        assert_frame_equal(result_df, expected_df)


# One test for `log_outlier_info()`:
# Test for information log of outliers detected


class TestLogOutlierNumbers:
    """Unit tests for log_outlier_info function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_columns = [
            "Reference",
            "selectiontype",
            "status",
            "value_col",
            "instance",
            "value_col_outlier_flag",
        ]

        data = [
            [1, "P", "Clear", 1, 0, True],
            [2, "P", "Clear", 1000, 0, False],
            [3, "X", "Check needed", 0, 1, False],
            [4, "X", "Clear", 50, 0, False],
            [5, "P", "Check needed", 50, 0, False],
            [6, "P", "Clear", -1, 0, False],
            [7, "P", "Clear", 50, 1, False],
        ]

        input_df = pandasDF(data=data, columns=input_columns)
        return input_df

    def test_log_outlier_info(self, caplog):
        """Test for log_outlier_info function."""

        input_df = self.create_input_df()

        with caplog.at_level(logging.INFO):
            auto.log_outlier_info(input_df, "value_col")

        assert (
            "1 outliers were detected out of a total of 2 valid entries in column"
            " value_col"
            in caplog.text
        )


# One test for `run_auto_flagging()`:
# Test that the auto outliering runs correctly end to end and that
# one outlier flag will flag the whole row as an outlier


class TestOneValueOutlier:
    """Unit test for one value causes the whole RU ref to be an outlier"""

    """Using run_auto_flagging function to allow for multiple outlier cols"""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "selectiontype",
            "formtype",
            "status",
            "period",
            "cellnumber",
            "701",
            "702",
            "703",
            "704",
        ]

        data = [
            [1, 0, "P", "0001", "Clear", 2020, 10, 1.1, 1.1, 1.1, 0.1],
            [2, 0, "P", "0001", "Clear", 2020, 10, 1.1, 5.1, 1.1, 1.1],
            [3, 0, "P", "0001", "Clear", 2020, 10, 1.1, 1.1, 1.1, 5.1],
            [4, 0, "P", "0001", "Clear", 2020, 10, 1.1, 0.1, 1.1, 1.1],
            [5, 0, "P", "0001", "Clear", 2020, 10, 1.1, 1.1, 1.1, 1.1],
            [6, 0, "P", "0001", "Clear", 2020, 10, 1.1, 1.1, 0.1, 1.1],
            [7, 0, "P", "0001", "Clear", 2020, 10, 1.1, 1.1, 5.1, 1.1],
            [8, 0, "P", "0001", "Clear", 2020, 10, 0.1, 1.1, 1.1, 1.1],
            [9, 0, "P", "0001", "Clear", 2020, 10, 1.1, 1.1, 1.1, 1.1],
            [10, 0, "P", "0001", "Clear", 2020, 10, 5.1, 1.1, 1.1, 1.1],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_expected_df(self):
        """Create dataframe for the expected output of the test."""
        exp_cols = [
            "reference",
            "instance",
            "selectiontype",
            "formtype",
            "status",
            "period",
            "cellnumber",
            "701",
            "702",
            "703",
            "704",
            "701_outlier_flag",
            "702_outlier_flag",
            "703_outlier_flag",
            "704_outlier_flag",
            "auto_outlier",
        ]

        data = [
            [1, 0, "P", "0001", "Clear", 2020, 10, 1.1, 1.1, 1.1, 0.1, False, False, False, True, True],
            [2, 0, "P", "0001", "Clear", 2020, 10, 1.1, 5.1, 1.1, 1.1, False, True, False, False, True],
            [3, 0, "P", "0001", "Clear", 2020, 10, 1.1, 1.1, 1.1, 5.1, False, False, False, True, True],
            [4, 0, "P", "0001", "Clear", 2020, 10, 1.1, 0.1, 1.1, 1.1, False, True, False, False, True],
            [5, 0, "P", "0001", "Clear", 2020, 10, 1.1, 1.1, 1.1, 1.1, False, False, False, False, False],
            [6, 0, "P", "0001", "Clear", 2020, 10, 1.1, 1.1, 0.1, 1.1, False, False, True, False, True],
            [7, 0, "P", "0001", "Clear", 2020, 10, 1.1, 1.1, 5.1, 1.1, False, False, True, False, True],
            [8, 0, "P", "0001", "Clear", 2020, 10, 0.1, 1.1, 1.1, 1.1, True, False, False, False, True],
            [9, 0, "P", "0001", "Clear", 2020, 10, 1.1, 1.1, 1.1, 1.1, False, False, False, False, False],
            [10, 0, "P", "0001", "Clear", 2020, 10, 5.1, 1.1, 1.1, 1.1, True, False, False, False, True],
        ]

        expected_df = pandasDF(data=data, columns=exp_cols)
        return expected_df

    def test_run_auto_flagging(self):
        """Test for flag_outliers function."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        upper_clip = 0.05
        lower_clip = 0.05
        flag_cols = ["701", "702", "703", "704"]

        result_df = auto.run_auto_flagging(input_df, upper_clip, lower_clip, flag_cols)

        assert_frame_equal(result_df, expected_df)


# One test for `apply_short_form_filter()`:
# Test that the short form filtering removes the correct rows


class TestShortFormFilter:
    """Unit tests for apply_short_form_filters function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
            "instance",
            "selectiontype",
            "formtype",
        ]

        data = [
            [1, 0, "P", "0006"],
            [2, 1, "P", "0006"],
            [3, 0, "C", "0006"],
            [4, 0, "P", "0001"],
            [5, 1, "C", "0001"],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_expected_df(self):
        """Create dataframe for the expected output of the test."""
        exp_cols = [
            "reference",
            "instance",
            "selectiontype",
            "formtype",
        ]

        data = [
            [1, 0, "P", "0006"],
        ]

        expected_df = pandasDF(data=data, columns=exp_cols)
        return expected_df

    def test_filter_valid(self):
        """Test for flag_outliers function."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        form_type_no: str = "0006"
        sel_type: str = "P"

        result_df = auto.apply_short_form_filters(input_df, form_type_no, sel_type)

        assert_frame_equal(
            result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
        )


# One test for `normal_round()`:
# Test that the rounding function is working correctly


class TestNormalRound:
    """Unit tests for normal_round function."""

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
        input_columns = ["Reference", "to_round", "rounded"]
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
            lambda row: auto.normal_round(row["to_round"]), axis=1
        )
        assert_frame_equal(input_df, expected_df)
