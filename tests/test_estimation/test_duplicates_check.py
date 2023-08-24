from pandas._testing import assert_frame_equal
from pandas import DataFrame as pandasDF

from src.estimation.duplicates_check import count_unique, filter_short_forms

class TestCountUnique:
    """Unit tests for count_unique function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference"
        ]

        data = [
            [1],
            [1],
            [2],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_input_df_nodup(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "reference",
        ]

        data = [
            [1],
            [2],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_expected_df(self):
        """Create an input dataframe for the test."""
        exp_cols = [
            "reference",
            "count"
        ]

        data = [
            [1, 2],
            [2, 1],
        ]

        exp_df = pandasDF(data=data, columns=exp_cols)
        return exp_df

    def create_expected_df_nodup(self):
        """Create an input dataframe for the test."""
        exp_cols = [
            "reference",
            "count"
        ]

        data = [
            [1, 1],
            [2, 1],
        ]

        exp_df = pandasDF(data=data, columns=exp_cols)
        return exp_df

    def test_count_unique(self):
        """Test for count_unique function."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        result_df = count_unique(input_df, "reference")

        assert_frame_equal(result_df, expected_df)

    def test_count_unique_nodup(self):
        """Test for count_unique function."""
        input_df = self.create_input_df_nodup()
        expected_df = self.create_expected_df_nodup()

        result_df = count_unique(input_df, "reference")

        assert_frame_equal(result_df, expected_df)


class TestFilterShortForms:
    """Unit tests for filter_short_forms function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = [
            "formtype"
        ]

        data = [
            ["0006"],
            ["NOT0006"],
        ]

        input_df = pandasDF(data=data, columns=input_cols)
        return input_df

    def create_expected_df(self):
        """Create an input dataframe for the test."""
        exp_cols = [
            "formtype"
        ]

        data = [
            ["0006"],
        ]

        exp_df = pandasDF(data=data, columns=exp_cols)
        return exp_df

    def test_filter_short_forms_nodup(self):
        """Test for filter_short_forms function."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        result_df = filter_short_forms(input_df)

        assert_frame_equal(result_df, expected_df)
