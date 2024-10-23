"""Tests for 'staging_helpers.py'."""
# Standard Library Imports
import os
import pytest
import pathlib
from unittest.mock import Mock
from typing import Tuple
from datetime import date
import logging
from unittest.mock import patch

# Third Party Imports
import pandas as pd
import numpy as np
from pandas import DataFrame as pandasDF
import pyarrow.feather as feather

# Local Imports
from src.staging.staging_helpers import (
    fix_anon_data,
    getmappername,
    load_validate_mapper,
    load_snapshot_feather,
    load_val_snapshot_json,
    df_to_feather,
    stage_validate_harmonise_postcodes,
    filter_pnp_data,
)
from src.utils.local_file_mods import (
    rd_file_exists as check_file_exists,
    rd_read_feather as read_feather,
    rd_write_feather as write_feather,
    rd_read_csv as read_csv,
    rd_write_csv as write_csv,
)


def match_col_type(df1: pd.DataFrame, df2: pd.DataFrame, col_name: str, _type: str):
    """Convert DataFrame columns of the same name to matching types."""
    df1[col_name] = df1[col_name].astype(_type)
    df2[col_name] = df2[col_name].astype(_type)
    return (df1, df2)


class TestFixAnonData(object):
    """Tests for fix_anon_data."""

    @pytest.fixture(scope="function")
    def dummy_config(self) -> dict:
        """Config used for test data."""
        config = {"devtest": {"seltype_list": [1, 2, 3, 5, 6, 7, 9, 10]}}
        return config

    @pytest.fixture(scope="function")
    def input_data(self):
        """Input data for fix_anon_data tests."""
        columns = ["col1"]
        data = [
            [1],
            [2],
            [3],
            [4],
            [5],
            [6],
            [7],
            [8],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    @pytest.fixture(scope="function")
    def expected_output(self):
        """Expected output for fix_anon_data tests"""
        columns = ["col1", "instance", "selectiontype", "cellnumber"]
        data = [
            [1, 0, "L", 3],
            [2, 0, "P", 9],
            [3, 0, "L", 3],
            [4, 0, "L", 3],
            [5, 0, "P", 10],
            [6, 0, "P", 6],
            [7, 0, "L", 5],
            [8, 0, "C", 10],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    def test_fix_anon_data(self, input_data, dummy_config, expected_output):
        """General tests for fix_anon_data."""
        output = fix_anon_data(input_data, dummy_config)
        output, expected_output = match_col_type(
            output, expected_output, "cellnumber", "int32"
        )
        assert output.equals(expected_output), "fix_anon_data not behaving as expected."


class TestGetMapperName(object):
    """Tests for getmappername."""

    def test_getmappername(self):
        """General tests for getmappername."""
        test_str = "cellno_path"
        # with split
        assert (
            getmappername(test_str, True) == "cellno"
        ), "getmappername not behaving as expected when split=True"
        # without split
        assert (
            getmappername(test_str, False) == "cellno"
        ), "getmappername not behaving as expected when split=False"


class TestLoadValidateMapper(object):
    """Tests for load_validate_mapper."""

    @patch("src.utils.local_file_mods.rd_file_exists")
    @patch("src.utils.local_file_mods.rd_read_csv")
    @patch("src.staging.validation.validate_data_with_schema")
    def test_load_validate_mapper(
        self,
        mock_val_with_schema_func,
        mock_read_csv_func,
        mock_file_exists_func,
    ):

        # Create a logger for this test
        test_logger = logging.getLogger("test_load_validate_mapper")
        test_logger.setLevel(logging.DEBUG)

        # Mock data
        mapper_path_key = "test_mapper_path"

        config = {
            "mapping_paths": {
                "test_mapper_path": "/path/to/mapper.csv"
            },
            "global": {
                "network_or_hdfs": "network",
            },
        }

        mapper_df = pd.DataFrame(
            {"col_one": [1, 2, 3, 4, 5, 6], "col_many": ["A", "A", "B", "C", "D", "D"]}
        )
        schema_path = "./config/test_schema.toml"

        # Set mock return values
        mock_file_exists_func.return_value = True
        mock_read_csv_func.return_value = mapper_df

        # Call the function
        output = load_validate_mapper(
            mapper_path_key,
            config,
            test_logger,
        )

        # Assertions
        mock_file_exists_func.assert_called_once_with("/path/to/mapper.csv", raise_error=True)
        mock_read_csv_func.assert_called_once_with("/path/to/mapper.csv")
        mock_val_with_schema_func.assert_called_once_with(mapper_df, schema_path)
        assert output.equals(mapper_df), "load_validate_mapper not behaving as expected."


class TestLoadSnapshotFeather(object):
    """Tests for load_snapshot_feather."""

    def test_load_snapshot_feather(self, tmp_path):
        """General tests for load_snapshot_feather."""
        # create 'snapshot'
        empty_df = pd.DataFrame({"test": [1, 2, 3]})
        path = os.path.join(tmp_path, "snapshot_test.feather")
        feather.write_feather(empty_df, path)
        # test loading 'snapshot'
        snapshot = load_snapshot_feather(path, read_feather)
        assert np.array_equal(
            snapshot.test, [1, 2, 3]
        ), "load_snapshot_feather not behaving as expected"
        assert (
            len(snapshot.columns) == 1
        ), "Snapshot df has more columnss than expected."


class TestLoadValSnapshotJson(object):
    """Tests for the load_val_snapshot_json function."""

    @patch("src.utils.local_file_mods.rd_load_json")
    @patch("src.staging.validation.validate_data_with_schema")
    @patch("src.staging.spp_snapshot_processing.full_responses")
    @patch("src.staging.validation.combine_schemas_validate_full_df")
    def test_load_val_snapshot_json_correct_response_rate(
        self,
        mock_combine_schemas_validate_full_df,
        mock_full_responses,
        mock_validate_data_with_schema,
        mock_load_json
    ):
        """Ensure load_val_snapshot_json behaves correctly."""
        snapshot_path = "path/to/snapshot.json"
        network_or_hdfs = "network"
        expected_res_rate = "0.50"
        config = {
            "global": {"dev_test": False},
            "schema_paths": {
                "contributors_schema": "path/to/contributors_schema.toml",
                "long_response_schema": "path/to/long_response_schema.toml",
                "wide_responses_schema": "path/to/wide_responses_schema.toml",
            },
        }

        # Setup mock return value
        mock_load_json.return_value = {
            "contributors": [{"reference": i} for i in range(1, 5)],
            "responses": [{"reference": i} for i in range(1, 3)],
        }

        # Execute the function under test
        full_responses, res_rate = load_val_snapshot_json(
            snapshot_path, mock_load_json, config, network_or_hdfs
        )

        # Assertions
        assert (
            res_rate == expected_res_rate
        ), "The response rate calculated by load_val_snapshot_json does not match the expected value."
        mock_load_json.assert_called_once_with(snapshot_path)
        mock_validate_data_with_schema.assert_called()
        mock_combine_schemas_validate_full_df.assert_called()


class TestDfToFeather(object):
    """Tests for df_to_Feather."""

    @pytest.fixture(scope="function")
    def f1(self):
        """Snapshot 1 test data."""
        f1 = pd.DataFrame({"f1": [1]})
        return f1

    def test_df_to_feather_defences(self, tmp_path, f1):
        """Defensive tests for df_to_feather."""
        # test invalid save location
        fake_path = "test_test/test/this_is_a_test"
        match = rf"The passed directory.*{fake_path}.*"
        with pytest.raises(FileNotFoundError, match=match):
            df_to_feather(fake_path, "test", f1, write_feather)
        # test raise for file already exists
        df_to_feather(tmp_path, "test", f1, write_feather)
        match = r"File already saved at .*"
        with pytest.raises(FileExistsError, match=match):
            df_to_feather(
                tmp_path,
                "test",
                f1,
                write_feather,
                False,
            )

    def test_df_to_feather(self, tmp_path, f1):
        """General tests for df_to_feather."""
        # default save
        df_to_feather(
            tmp_path,
            "test.feather",
            f1,
            write_feather,
            False,
        )
        assert os.path.exists(
            os.path.join(tmp_path, "test.feather")
        ), "Feather not saved out correctly."
        # save without .feather extension passed
        df_to_feather(
            tmp_path,
            "test2",
            f1,
            write_feather,
            False,
        )
        assert os.path.exists(
            os.path.join(tmp_path, "test2.feather")
        ), ".feather. extension not applied to path."
        # test with overwrite=True
        # using try/except since nullcontext not supported in python=3.6
        try:
            df_to_feather(
                tmp_path,
                "test2",
                f1,
                write_feather,
                True,
            )
        except Exception as e:
            raise Exception(f"Overwriting feather file caused error: {e}")
        # test with two file extensions (one non-feather)
        df_to_feather(
            tmp_path,
            "test3.test",
            f1,
            write_feather,
            False,
        )
        assert os.path.exists(
            os.path.join(tmp_path, "test3.test.feather")
        ), ".feather. extension not applied when another extension exists."


class TestStageValidateHarmonisePostcodes(object):
    """Tests for stage_validate_harmonise_postcodes."""

    @pytest.fixture(scope="function")
    def config(self, tmp_path) -> pd.DataFrame:
        """Test config."""
        config = {
            "global": {"postcode_csv_check": True},
            "years": {"survey_year": 2022},
            "staging_paths": {"pcode_val_path": tmp_path, "postcode_masterlist": "ml"},
            "mapping_paths": {"postcode_mapper": "ml"},
        }
        return config

    # def create_paths(self, pc_path, pc_ml) -> pd.DataFrame:
    #     """Test paths."""
    #     paths = {"pcode_val_path": pc_path, "postcode_masterlist": pc_ml}
    #     return paths

    @pytest.fixture(scope="function")
    def full_responses(self) -> pd.DataFrame:
        """Test data for stag_validate_harmonise_postcodes."""
        columns = ["reference", "instance", "formtype", "601", "referencepostcode"]
        data = [
            [39900000404, 0.0, 6, None, "NP442NZ"],  # add white space
            [39900000404, 1.0, 6, "NP442NZ", "NP442NZ"],
            [39900000960, 0.0, 1, None, "CE1 4OY"],  # add white space
            [39900000960, 1.0, 1, "CE1 4OY", "CE1 4OY"],
            [39900001530, 0.0, 6, "CE2", "CE2"],  # invalid
            [39900001601, 0.0, 1, None, "RH12 1XL"],  # normal
            [39900001601, 1.0, 1, "RH12 1XL", "RH12 1XL"],
            [39900003110, 0.0, 6, None, "CE11 8IU"],
            [39900003110, 1.0, 6, "CE11 8iu", "CE11 8IU"],
            [39900003110, 2.0, 6, "Ce11 8iU", "CE11 8IU"],
            [38880003110, 0.0, 6, None, "NP22 8UI"],  # not in postcode list
            [38880003110, 1.0, 6, "NP22 8UI", "NP22 8UI"],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    def mock_read_csv(self, file_path, **kwargs):
        """Mock function to read a CSV file."""
        return pd.DataFrame({"pcd2": ["NP44 2NZ", "CE1  4OY", "RH12 1XL", "CE11 8IU"]})

    def mock_check_file_exists(self, file_path, raise_error=True):
        """Mock function to check if a file exists."""
        return True

    @pytest.fixture(scope="function")
    def full_responses_output(self) -> pd.DataFrame:
        """Expected output for full_responses."""
        columns = [
            "reference",
            "instance",
            "formtype",
            "601",
            "referencepostcode",
            "postcodes_harmonised",
        ]
        data = [
            [39900000404, 0.0, 6, None, "NP442NZ", "NP44 2NZ"],
            [39900000404, 1.0, 6, "NP44 2NZ", "NP442NZ", "NP44 2NZ"],
            [39900000960, 0.0, 1, None, "CE1 4OY", "CE1  4OY"],
            [39900000960, 1.0, 1, "CE1  4OY", "CE1 4OY", "CE1  4OY"],
            [39900001530, 0.0, 6, "CE2     ", "CE2", None],
            [39900001601, 0.0, 1, None, "RH12 1XL", "RH12 1XL"],
            [39900001601, 1.0, 1, "RH12 1XL", "RH12 1XL", "RH12 1XL"],
            [39900003110, 0.0, 6, None, "CE11 8IU", "CE11 8IU"],
            [39900003110, 1.0, 6, "CE11 8IU", "CE11 8IU", "CE11 8IU"],
            [39900003110, 2.0, 6, "CE11 8IU", "CE11 8IU", "CE11 8IU"],
            [38880003110, 0.0, 6, None, "NP22 8UI", None],
            [38880003110, 1.0, 6, "NP22 8UI", "NP22 8UI", None],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    @pytest.fixture(scope="function")
    def pc_mapper_output(self) -> pd.DataFrame:
        """Expected output for postcode_mapper"""
        columns = ["pcd2"]
        data = [
            ["NP44 2NZ"],
            ["CE1  4OY"],
            ["RH12 1XL"],
            ["CE11 8IU"],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    def get_todays_date(self) -> str:
        """Get the date in the format YYYY-MM-DD. Used for filenames."""
        today = date.today()
        today_str = today.strftime(r"%y-%m-%d")
        return today_str

    def test_stage_validate_harmonise_postcodes(
        self, full_responses, config, pc_mapper_output, full_responses_output, tmp_path
    ):
        """General tests for stage_validate_harmonise_postcodes."""
        fr, pm = stage_validate_harmonise_postcodes(
            config=config,
            full_responses=full_responses,
            run_id=1,
            check_file_exists=self.mock_check_file_exists,
            read_csv=self.mock_read_csv,
            write_csv=write_csv,
        )
        # test direct function outputs
        assert fr.equals(full_responses_output), (
            "full_responses output from stage_validate_harmonise_postcodes not"
            " as expected."
        )
        assert pm.equals(pc_mapper_output), (
            "postcode_mapper output from stage_validate_harmonise_postcodes not"
            " as expected."
        )
        # assert that invalid postcodes have been saved out
        files = os.listdir(tmp_path)
        filename = (
            f"2022_invalid_postcodes_{self.get_todays_date()}_v1.csv"
        )
        assert (
            filename in files
        ), "stage_validate_harmonise_postcodes failed to save out invalid PCs"


class TestFilterPnpData:
    """Tests for the filter_pnp_data function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_columns = [
            "reference",
            "instance",
            "legalstatus",
            "statusencoded",
            "postcodes_harmonised",
        ]

        data = [
            [49900000404, 0, "1", "210", "AB15 3GU"],
            [49900000406, np.NaN, "2", "210", "BA1 5DA"],
            [49900000409, 1, "1", "100", "CB1 3NF"],
            [49900000510, 2, "7", "201", "BA1 5DA"],
            [49912758922, 3, "1", "303", "DE72 3AU"],
            [49900187320, 4, "2", "304", "NP30 7ZZ"],
            [49900184433, 1, "7", "210", "CF10 BZZ"],
            [49911791786, 1, "4", "201", "CF10 BZZ"],
            [49901183959, 4, "1", "309", "SA50 5BE"],
        ]

        input_df = pandasDF(data=data, columns=input_columns)
        input_df["legalstatus"].astype("category")
        input_df["statusencoded"].astype("category")
        return input_df

    def create_exp_output_df(self):
        """Create an output dataframe for the test."""
        exp_output_columns = [
            "reference",
            "instance",
            "legalstatus",
            "statusencoded",
            "postcodes_harmonised",
        ]

        data1 = [
            [49900000404, 0, "1", "210", "AB15 3GU"],
            [49900000406, np.NaN, "2", "210", "BA1 5DA"],
            [49900000409, 1, "1", "100", "CB1 3NF"],
            [49912758922, 3, "1", "303", "DE72 3AU"],
            [49900187320, 4, "2", "304", "NP30 7ZZ"],
            [49911791786, 1, "4", "201", "CF10 BZZ"],
            [49901183959, 4, "1", "309", "SA50 5BE"],
        ]
        exp1_output_df = pandasDF(data=data1, columns=exp_output_columns)
        exp1_output_df["legalstatus"].astype("category")
        exp1_output_df["statusencoded"].astype("category")

        data2 = [
            [49900000510, 2.0, "7", "201", "BA1 5DA"],
            [49900184433, 1.0, "7", "210", "CF10 BZZ"],
        ]
        exp2_output_df = pandasDF(data=data2, columns=exp_output_columns)
        exp2_output_df["legalstatus"].astype("category")
        exp2_output_df["statusencoded"].astype("category")

        return exp1_output_df, exp2_output_df

    def test_filter_pnp_data(self):
        """Test for the filter_pnp_data function."""
        input_df = self.create_input_df()
        exp1_df, exp2_df = self.create_exp_output_df()

        result1_df, result2_df = filter_pnp_data(input_df)

        pd.testing.assert_frame_equal(
            result1_df.reset_index(drop=True), exp1_df.reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(
            result2_df.reset_index(drop=True), exp2_df.reset_index(drop=True)
        )
