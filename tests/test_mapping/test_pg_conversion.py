"""Unit testing module."""
# Import testing packages
import pandas as pd
import pytest
import numpy as np

from src.mapping.pg_conversion import (
    pg_to_pg_mapper,
    sic_to_pg_mapper,
    run_pg_conversion,
)


class TestSicPgMapper(object):
    """Tests for the sic to pg mapper function."""

    @pytest.fixture(scope="function")
    def sic_dummy_data(self) -> pd.DataFrame:
        # Set up the dummyinput  data
        columns = ["201", "rusic"]
        data = [
            [53, 2500],
            [np.nan, 1600],
            [np.nan, 4300],
        ]
        return pd.DataFrame(data, columns=columns)

    @pytest.fixture(scope="function")
    def sic_mapper(self):
        columns = ["sic", "pg"]
        mapper_rows = [
            [1600, 36],
            [2500, 95],
            [7300, 45],
            [2500, 53],
        ]
        return pd.DataFrame(mapper_rows, columns=columns)

    @pytest.fixture(scope="function")
    def sic_expected_output(self) -> pd.DataFrame:
        # Set up the dummy output data
        columns = ["201", "rusic"]
        data = [
            [53, 2500],
            [36, 1600],
            [np.nan, 4300],
        ]
        return pd.DataFrame(data, columns=columns)

    def test_sic_mapper(self, sic_dummy_data, sic_expected_output, sic_mapper):
        """Tests for pg mapper function."""

        expected_output_data = sic_expected_output

        df_result = sic_to_pg_mapper(
            sic_dummy_data,
            sic_mapper,
            pg_column="201",
            from_col="sic",
            to_col="pg",
        )

        pd.testing.assert_frame_equal(df_result, expected_output_data)


class TestPgToPgMapper(object):
    """Tests for the pg to pg"""

    @pytest.fixture(scope="function")
    def mapper(self):
        mapper_rows = [
            [36, "N"],
            [37, "Y"],
            [45, "AC"],
            [47, "AD"],
            [49, "AD"],
            [50, "AD"],
            [58, "AH"],
        ]
        columns = ["pg_numeric", "pg_alpha"]
        mapper_df = pd.DataFrame(mapper_rows, columns=columns)

        return mapper_df

    def test_pg_to_pg_mapper_with_many_to_one(self, mapper):
        """Tests for pg mapper function."""

        columns = ["formtype", "201", "other_col"]
        row_data = [["0001", 45, "2020"], ["0001", 49, "2020"], ["0002", 50, "2020"]]

        test_df = pd.DataFrame(row_data, columns=columns)

        expected_columns = ["formtype", "201", "other_col", "pg_numeric"]

        expected_data = [
            ["0001", "AC", "2020", 45],
            ["0001", "AD", "2020", 49],
            ["0002", "AD", "2020", 50],
        ]

        type_dict = {"201": "category", "pg_numeric": "category"}

        # Build the expected result dataframe. Set the dtype of prod group to cat, like the result_df
        expected_result_df = pd.DataFrame(expected_data, columns=expected_columns)
        expected_result_df = expected_result_df.astype(type_dict)

        result_df = pg_to_pg_mapper(test_df.copy(), mapper.copy())

        pd.testing.assert_frame_equal(result_df, expected_result_df, check_dtype=False)

    def test_pg_to_pg_mapper_success(self, mapper):
        columns = ["formtype", "201", "other_col"]
        row_data = [
            ["0001", 36, "2020"],
            ["0001", 45, "2020"],
            ["0002", 58, "2020"],
            ["0001", 49, "2020"],
        ]

        test_df = pd.DataFrame(row_data, columns=columns)

        expected_columns = ["formtype", "201", "other_col", "pg_numeric"]
        expected_data = [
            ["0001", "N", "2020", 36],
            ["0001", "AC", "2020", 45],
            ["0002", "AH", "2020", 58],
            ["0001", "AD", "2020", 49],
        ]

        expected_result_df = pd.DataFrame(expected_data, columns=expected_columns)

        type_dict = {"201": "category", "pg_numeric": "category"}
        expected_result_df = expected_result_df.astype(type_dict)

        result_df = pg_to_pg_mapper(test_df.copy(), mapper.copy())

        pd.testing.assert_frame_equal(result_df, expected_result_df)


class TestRunPgConversion(object):
    @pytest.fixture(scope="function")
    def ni_full_responses(self) -> pd.DataFrame:
        """'ni_full_responses' input as input for the tests.

        NOTE: This is a subset of columns for testing purposes
        """
        columns = [
            "reference",
            "instance",
            "period_year",
            "foc",
            "407",
            "201",  # pg
            "rusic",  # sic
        ]
        data = [
            [39900001577, 1, 2021, "GB ", 0.72, np.nan, 10130],
            [39900006060, 1, 2021, "GB ", 0.6, 9, 17219],
            [39900008752, 1, 2021, "GB ", 0.72, np.nan, 46420],
            [39900008767, 1, 2021, "GB ", 0.72, np.nan, 14190],
            [39900008807, 1, 2021, "GB ", 0.0, np.nan, 46390],
            [39900008914, 1, 2021, "US ", 0.0, np.nan, 17219],
            [39900008968, 1, 2021, "GB ", 1.44, np.nan, 46420],
            [39900009016, 1, 2021, "GB ", 4.0, np.nan, 71129],
            [39900009078, 1, 2021, "GB ", 0.72, np.nan, 23610],
        ]
        ni_full_responses = pd.DataFrame(data=data, columns=columns)
        return ni_full_responses

    @pytest.fixture(scope="function")
    def sic_pg_num(self) -> pd.DataFrame:
        """'sic_pg_num' input for pg_conversion.

        NOTE: This is a subset of columns for testing purposes
        """
        columns = ["Unnamed: 0", "SIC 2007_CODE", "2016 > Form PG"]
        data = [
            [100, 10130, 3],
            [198, 14190, 6],
            [223, 17219, 9],
            [253, 20120, 12],
            [311, 23610, 15],
            [772, 46390, 40],
            [774, 46420, 40],
            [1088, 71129, 50],
        ]
        sic_pg_num = pd.DataFrame(data=data, columns=columns)
        return sic_pg_num

    @pytest.fixture(scope="function")
    def pg_num_alpha(self) -> pd.DataFrame:
        """'pg_num_alpha' input for pg_conversion.

        NOTE: This is a subset of columns for testing purposes
        """
        columns = ["Unnamed: 0", "pg_numeric", "pg_alpha"]
        data = [
            [2, 3, "C"],
            [5, 6, "D"],
            [8, 9, "E"],
            [11, 12, "G"],
            [14, 15, "J"],
            [39, 40, "AA"],
            [48, 50, "AD"],
        ]
        pg_num_alpha = pd.DataFrame(data=data, columns=columns)
        return pg_num_alpha

    @pytest.fixture(scope="function")
    def ni_expected(self) -> pd.DataFrame:
        columns = [
            "reference",
            "instance",
            "period_year",
            "foc",
            "407",
            "201",
            "rusic",
            "pg_numeric",
        ]
        # pg_numeric as float to match outputs
        data = [
            [39900001577, 1, 2021, "GB ", 0.72, "C", 10130, 3.0],
            [39900006060, 1, 2021, "GB ", 0.6, "E", 17219, 9.0],
            [39900008752, 1, 2021, "GB ", 0.72, "AA", 46420, 40.0],
            [39900008767, 1, 2021, "GB ", 0.72, "D", 14190, 6.0],
            [39900008807, 1, 2021, "GB ", 0.0, "AA", 46390, 40.0],
            [39900008914, 1, 2021, "US ", 0.0, "E", 17219, 9.0],
            [39900008968, 1, 2021, "GB ", 1.44, "AA", 46420, 40.0],
            [39900009016, 1, 2021, "GB ", 4.0, "AD", 71129, 50.0],
            [39900009078, 1, 2021, "GB ", 0.72, "J", 23610, 15.0],
        ] 

        ni_expected = pd.DataFrame(data=data, columns=columns)
        ni_expected["201"] = ni_expected["201"].astype("category")
        ni_expected["pg_numeric"] = ni_expected["pg_numeric"].astype("category")
        return ni_expected

    def test_run_pg_conversion(
        self, sic_pg_num, pg_num_alpha, ni_full_responses, ni_expected
    ):
        """Tests for run_pg_conversion."""

        result = run_pg_conversion(
            df=ni_full_responses,
            pg_num_alpha=pg_num_alpha,
            sic_pg_num=sic_pg_num,
            pg_column="201",
        )

        expected = ni_expected
        pd.testing.assert_frame_equal(result, expected)
