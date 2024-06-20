"""Tests for 'staging_helpers.py'."""
# Standard Library Imports
import os
import pytest
import pathlib
from typing import Tuple
from datetime import date

# Third Party Imports
import pandas as pd
import numpy as np
import pyarrow.feather as feather

# Local Imports
from src.staging.staging_helpers import (
    fix_anon_data,
    update_ref_list,
    getmappername,
    check_snapshot_feather_exists,
    load_snapshot_feather,
    df_to_feather,
    stage_validate_harmonise_postcodes,
)
from src.utils.local_file_mods import (
    local_file_exists as check_file_exists,
    local_read_feather as read_feather,
    local_write_feather as write_feather,
    read_local_csv as read_csv,
    write_local_csv as write_csv,
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


class TestUpdateRefList(object):
    """Tests for update_ref_list."""

    @pytest.fixture(scope="function")
    def full_input_df(self):
        """Main input data for update_ref_list tests."""
        columns = ["reference", "instance", "formtype", "cellnumber"]
        data = [
            [49900001031, 0.0, 6, 674],
            [49900001530, 0.0, 6, 805],
            [49900001601, 0.0, 1, 117],
            [49900001601, 1.0, 1, 117],
            [49900003099, 0.0, 6, 41],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        df["formtype"] = df["formtype"].apply(lambda x: str(x))
        return df

    @pytest.fixture(scope="function")
    def ref_list_input(self):
        """Reference list df input for update_ref_list tests."""
        columns = ["reference", "cellnumber", "selectiontype", "formtype"]
        data = [[49900001601, 117, "C", "1"]]
        df = pd.DataFrame(columns=columns, data=data)
        df["formtype"] = df["formtype"].apply(lambda x: str(x))
        return df

    @pytest.fixture(scope="function")
    def expected_output(self):
        """Expected output for update_ref_list tests."""
        columns = ["reference", "instance", "formtype", "cellnumber", "selectiontype"]
        data = [
            [49900001031, 0.0, "6", 674, np.nan],
            [49900001530, 0.0, "6", 805, np.nan],
            [49900001601, 0.0, "1", 817, "L"],
            [49900001601, 1.0, "1", 817, "L"],
            [49900003099, 0.0, "6", 41, np.nan],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    def test_update_ref_list(self, full_input_df, ref_list_input, expected_output):
        """General tests for update_ref_list."""
        output = update_ref_list(full_input_df, ref_list_input)
        assert output.equals(
            expected_output
        ), "update_ref_list not behaving as expected"

    def test_update_ref_list_raises(self, full_input_df, ref_list_input):
        """Test the raises in update_ref_list."""
        # add a non valid reference
        ref_list_input.loc[1] = [34567123123, 117, "C", "1"]
        error_msg = r"The following references in the reference list mapper are.*"
        with pytest.raises(ValueError, match=error_msg):
            update_ref_list(full_input_df, ref_list_input)


class TestGetMapperName(object):
    """Tests for getmappername."""

    def test_getmappername(self):
        """General tests for getmappername."""
        test_str = "cellno_2022_path"
        # with split
        assert (
            getmappername(test_str, True) == "cellno 2022"
        ), "getmappername not behaving as expected when split=True"
        # without split
        assert (
            getmappername(test_str, False) == "cellno_2022"
        ), "getmappername not behaving as expected when split=False"


class TestCheckSnapshotFeatherExists(object):
    """Tests for check_snapshot_feather_exists."""

    def create_feather_files(
        self, dir: pathlib.Path, first: bool, second: bool
    ) -> Tuple[pathlib.Path, pathlib.Path]:
        """Create feather files in a given path for testing."""
        empty_df = pd.DataFrame()
        # define feather paths
        f_path = os.path.join(dir, "first_feather.feather")
        s_path = os.path.join(dir, "second_feather.feather")
        # create feather files accordingly
        if first:
            feather.write_feather(empty_df.copy(), f_path)
        if second:
            feather.write_feather(empty_df.copy(), s_path)
        return (pathlib.Path(f_path), pathlib.Path(s_path))

    @pytest.mark.parametrize(
        "first, second, check_both, result",
        (
            [True, False, False, True],  # create first, check first
            [True, False, True, False],  # create first, check both
            [False, True, False, False],  # create second, check first
            [False, True, True, False],  # create second, check both
            [True, True, True, True],  # create both, check both
            [True, True, False, True],  # create both, check first
        ),
    )
    def test_check_snapshot_feather_exists(
        self,
        tmp_path,
        first: bool,
        second: bool,
        check_both: bool,
        result: bool,
    ):
        """General tests for check_snapshot_feather_exists."""
        # create feather files
        (a, b) = self.create_feather_files(tmp_path, first, second)
        # define config
        config = {"global": {"load_updated_snapshot": check_both}}
        # get result
        output = check_snapshot_feather_exists(
            config=config,
            check_file_exists=check_file_exists,
            feather_file_to_check=a,
            secondary_feather_file=b,
        )
        # assert result
        assert (
            output == result
        ), "check_snapshot_feather_exists not behaving as expected."


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


# load_val_snapshot_json [CANT TEST: TOO MANY HARD CODED PATHS]


# load_validate_secondary_snapshot [CANT TEST: TOO MANY HARD CODED PATHS]


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
    def config(self) -> pd.DataFrame:
        """Test config."""
        config = {"global": {"postcode_csv_check": True}}
        return config

    def create_paths(self, pc_path, pc_ml) -> pd.DataFrame:
        """Test paths."""
        paths = {"postcode_path": pc_path, "postcode_masterlist": pc_ml}
        return paths

    @pytest.fixture(scope="function")
    def full_responses(self) -> pd.DataFrame:
        """Test data for stag_validate_harmonise_postcodes."""
        columns = ["reference", "instance", "formtype", "601", "referencepostcode"]
        data = [
            [39900000404, 0.0, 6, np.nan, "NP442NZ"],  # add white space
            [39900000404, 1.0, 6, "NP442NZ", "NP442NZ"],
            [39900000960, 0.0, 1, np.nan, "CE1 4OY"],  # add white space
            [39900000960, 1.0, 1, "CE1 4OY", "CE1 4OY"],
            [39900001530, 0.0, 6, "CE2", "CE2"],  # invalid
            [39900001601, 0.0, 1, np.nan, "RH12 1XL"],  # normal
            [39900001601, 1.0, 1, "RH12 1XL", "RH12 1XL"],
            [39900003110, 0.0, 6, np.nan, "CE11 8IU"],
            [39900003110, 1.0, 6, "CE11 8iu", "CE11 8IU"],
            [39900003110, 2.0, 6, "Ce11 8iU", "CE11 8IU"],
            [38880003110, 0.0, 6, np.nan, "NP22 8UI"],  # not in postcode list
            [38880003110, 1.0, 6, "NP22 8UI", "NP22 8UI"],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    def postcode_masterlist(self, dir: pathlib.Path) -> pathlib.Path:
        """Write the postcode masterlist and return path."""
        postcode_df = pd.DataFrame(
            {"pcd2": ["NP44 2NZ", "CE1  4OY", "RH12 1XL", "CE11 8IU"]}
        )
        save_path = pathlib.Path(os.path.join(dir, "postcodes_masterlist.csv"))
        postcode_df.to_csv(save_path)
        return save_path

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
            [39900000404, 0.0, 6, np.nan, "NP442NZ", "NP44 2NZ"],
            [39900000404, 1.0, 6, "NP44 2NZ", "NP442NZ", "NP44 2NZ"],
            [39900000960, 0.0, 1, np.nan, "CE1 4OY", "CE1  4OY"],
            [39900000960, 1.0, 1, "CE1  4OY", "CE1 4OY", "CE1  4OY"],
            [39900001530, 0.0, 6, "     CE2", "CE2", np.nan],
            [39900001601, 0.0, 1, np.nan, "RH12 1XL", "RH12 1XL"],
            [39900001601, 1.0, 1, "RH12 1XL", "RH12 1XL", "RH12 1XL"],
            [39900003110, 0.0, 6, np.nan, "CE11 8IU", "CE11 8IU"],
            [39900003110, 1.0, 6, "CE11 8IU", "CE11 8IU", "CE11 8IU"],
            [39900003110, 2.0, 6, "CE11 8IU", "CE11 8IU", "CE11 8IU"],
            [38880003110, 0.0, 6, np.nan, "NP22 8UI", np.nan],
            [38880003110, 1.0, 6, "NP22 8UI", "NP22 8UI", np.nan],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    @pytest.fixture(scope="function")
    def pc_mapper_output(self) -> pd.DataFrame:
        """Expected output for postcode_mapper"""
        columns = ["Unnamed: 0", "pcd2"]
        data = [
            [0, "NP44 2NZ"],
            [1, "CE1  4OY"],
            [2, "RH12 1XL"],
            [3, "CE11 8IU"],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    def get_todays_date(self) -> str:
        """Get the date in the format YYYY-MM-DD. Used for filenames."""
        today = date.today()
        today_str = today.strftime(r"%Y-%m-%d")
        return today_str

    def test_stage_validate_harmonise_postcodes(
        self, full_responses, config, pc_mapper_output, full_responses_output, tmp_path
    ):
        """General tests for stage_validate_harmonise_postcodes."""
        pc_path = self.postcode_masterlist(tmp_path)
        paths = self.create_paths(tmp_path, pc_path)
        fr, pm = stage_validate_harmonise_postcodes(
            config=config,
            paths=paths,
            full_responses=full_responses,
            run_id=1,
            check_file_exists=check_file_exists,
            read_csv=read_csv,
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
        filename = f"invalid_unrecognised_postcodes_{self.get_todays_date()}_v1.csv"
        assert (
            filename in files
        ), "stage_validate_harmonise_postcodes failed to save out invalid PCs"
