import pandas as pd
import numpy as np
import logging
from pandas import DataFrame as pandasDF
from pandas._testing import assert_frame_equal, assert_series_equal

from src.site_apportionment.site_apportionment import create_notnull_mask, sort_rows_order_cols


class TestCreateNotnullMask():

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_columns = [
            "reference",
            "col",
            "flag_is_null",
            "flag_is_emty",
            "flag_is_notnull",
            "flag_not_is_null",
            "len",
            "flag_length_greater_than_zero",
        ]

        data = [
            [0, 1, np.nan, np.nan, True, True, np.nan, np.nan],
            [1, "a", np.nan, np.nan, True, True, 1.0, True],
            [2, "", np.nan, True, True, True, 0.0, np.nan],
            [3, pd.NA, True, np.nan, np.nan, np.nan, np.nan, np.nan],
            [4, np.nan, True, np.nan, np.nan, np.nan, np.nan, np.nan],
        ]

        input_df = pandasDF(data=data, columns=input_columns)
        return input_df

    def create_exp_output(self):
        """Create a series for the expected output."""
        name = "col"
        data = [False, True, False, False, False]
        return pd.Series(data, name=name)

    def test_create_notnull_mask(self):
        """Test for create_notnull_mask function."""
        df=pd.DataFrame(
            {"reference": [0, 1, 2, 3, 4],
            "col":[1, "a", "", pd.NA, np.nan]})

        # add extra cols to the df to illustrate how logic of nulls
        df.loc[df["col"].isnull(), "flag_is_null"] = True
        df.loc[df["col"] == "", "flag_is_emty"] = True
        df.loc[df["col"].notnull(), "flag_is_notnull"] = True
        df.loc[~df["col"].isnull(), "flag_not_is_null"] = True
        df["len"] = df["col"].str.len()
        df.loc[df["len"] > 0, "flag_length_greater_than_zero"] = True

        input_df = self.create_input_df()
        assert_frame_equal(df, input_df)

        result = create_notnull_mask(input_df, "col")
        expected_output = self.create_exp_output()
        assert_series_equal(result, expected_output)


class TestSortRowsOrderCols():

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_columns = [
            "instance",
            "Forth",
            "reference",
            "Remove",
            "period",
        ]

        data = [
            [2, "X", 3, "test", 22],
            [1, "X", 3, "test", 21],
            [np.nan, "X", 3, "test", 22],
            [2, "Y", 1, "test", 21],
            [1, "X", 1, "test", 22],
            [2, "X", 1, "test", 22],
            [1, "Y", 4, "test", 21],
            [1, "Z", 2, "test", 22],
            [1, "X", 1, "test", 21],
            [3, "Z", 1, "test", 22],

        ]

        input_df = pandasDF(data=data, columns=input_columns)
        return input_df

    def create_output_df(self):
        """Create an input dataframe for the test."""
        output_columns = [
            "reference",
            "period",
            "instance",
            "Forth",
        ]

        data = [
            [1, 21, 1.0, "X"],
            [1, 21, 2.0, "Y"],
            [3, 21, 1.0, "X"],
            [4, 21, 1.0, "Y"],
            [1, 22, 1.0, "X"],
            [1, 22, 2.0, "X"],
            [1, 22, 3.0, "Z"],
            [2, 22, 1.0, "Z"],
            [3, 22, 2.0, "X"],
            [3, 22, np.nan, "X"],
        ]

        expected_df = pandasDF(data=data, columns=output_columns)
        return expected_df

    def test_sort_rows_order_cols(self, caplog):

        cols_in_order = ["reference", "period", "instance", "Forth"]

        input_df = self.create_input_df()
        expected_df = self.create_output_df()

        with caplog.at_level(logging.DEBUG):
            result_df = sort_rows_order_cols(input_df, cols_in_order)

            assert "Removing {'Remove'} from df_cart" in caplog.text
            assert_frame_equal(result_df, expected_df)
