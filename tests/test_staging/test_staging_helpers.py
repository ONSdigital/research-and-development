"""Tests for 'staging_helpers.py'."""
# Standard Library Imports
import pytest
import pathlib
import os
from typing import Tuple

# Third Party Imports
import pandas as pd
import numpy as np
import pyarrow.feather as feather

# Local Imports
from src.staging.staging_helpers import (
    # postcode_topup,
    fix_anon_data,
    update_ref_list,
    getmappername,
    load_validate_mapper,
    load_historic_data,
    check_snapshot_feather_exists,
    load_val_snapshot_json,
    load_validate_secondary_snapshot,
    write_snapshot_to_feather,
    stage_validate_harmonise_postcodes
)
from src.utils.local_file_mods import local_file_exists as check_file_exists


# class TestPostcodeTopup(object):
#     """Tests for postcode_topup."""

#     @pytest.fixture(scope="function")
#     def input_data(self):
#         """Input data for postcode_topup tests."""
#         columns = ["key", "postcode"]
#         data = [
#             [1, "NP44 2NZ"], # normal
#             [2, "np44 2nz"], # lower case
#             [3, "NP4 2NZ"], # only 7 chars
#             [4, "NP44 2NZ 7Y"], # extra parts
#             [5, "NP44 2NZZ"], # 9 chars (extra)
#             [6, "NP442NZ"], # one part, 7 chars
#             [7, ""], #empty str
#         ]
#         df = pd.DataFrame(columns=columns, data=data)
#         return df
    

#     def test_postcode_topup(self, input_data):
#         """General tests for postcode_topup."""
#         output = input_data.copy()
#         output["postcode"] = output["postcode"].apply(
#             lambda x: postcode_topup(x)
#             )
#         print(output)


class TestFixAnonData(object):
    """Tests for fix_anon_data."""

    @pytest.fixture(scope="function")
    def dummy_config(self) -> dict:
        """Config used for test data."""
        config = {
            "devtest": {
                "seltype_list": [1, 2, 3, 5, 6, 7, 9, 10]
            }
                }
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
        df["cellnumber"] = df["cellnumber"].astype("int32")
        return df


    def test_fix_anon_data(self, input_data, dummy_config, expected_output):
        """General tests for fix_anon_data."""
        output = fix_anon_data(input_data, dummy_config)
        pd.testing.assert_frame_equal(output, expected_output)
        assert(output.equals(expected_output)), (
            "fix_anon_data not behaving as expected."
        )


class TestUpdateRefList(object):
    """Tests for update_ref_list."""

    @pytest.fixture(scope="function")
    def full_input_df(self):
        """Main input data for update_ref_list tests."""
        columns = ['reference', 'instance', 'formtype', 'cellnumber']
        data = [
            [49900001031, 0.0, 6, 674],
            [49900001530, 0.0, 6, 805],
            [49900001601, 0.0, 1, 117],
            [49900001601, 1.0, 1, 117],
            [49900003099, 0.0, 6, 41]
        ]
        df = pd.DataFrame(columns=columns, data=data)
        df["formtype"] = df["formtype"].apply(lambda x: str(x))
        return df
    

    @pytest.fixture(scope="function")
    def ref_list_input(self):
        """Reference list df input for update_ref_list tests."""
        columns = ['reference', 'cellnumber', 'selectiontype', 'formtype']
        data = [
            [49900001601, 117, 'C', '1']
        ]
        df = pd.DataFrame(columns=columns, data=data)
        df["formtype"] = df["formtype"].apply(lambda x: str(x))
        return df
    
    
    @pytest.fixture(scope="function")
    def expected_output(self):
        """Expected output for update_ref_list tests."""
        columns = ['reference', 'instance', 'formtype', 'cellnumber', 'selectiontype']
        data = [
            [49900001031, 0.0, "6", 674, np.nan],
            [49900001530, 0.0, "6", 805, np.nan],
            [49900001601, 0.0, "1", 817, "L"],
            [49900001601, 1.0, "1", 817, "L"],
            [49900003099, 0.0, "6", 41, np.nan]
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df
    

    def test_update_ref_list(self, full_input_df, ref_list_input, expected_output):
        """General tests for update_ref_list."""
        output = update_ref_list(full_input_df, ref_list_input)
        assert (output.equals(expected_output)), (
            "update_ref_list not behaving as expected"
        )


    def test_update_ref_list_raises(self, full_input_df, ref_list_input):
        """Test the raises in update_ref_list."""
        # add a non valid reference
        ref_list_input.loc[1] = [34567123123, 117, "C", "1"]
        error_msg = (
            r"The following references in the reference list mapper are.*"
        )
        with pytest.raises(ValueError, match=error_msg):
            update_ref_list(full_input_df, ref_list_input)
        

class TestGetMapperName(object):
    """Tests for getmappername."""

    def test_getmappername(self):
        """General tests for getmappername."""
        test_str = "cellno_2022_path"
        # with split
        assert getmappername(test_str, True) == "cellno 2022", (
            "getmappername not behaving as expected when split=True"
        )
        # without split
        assert getmappername(test_str, False) == "cellno_2022", (
            "getmappername not behaving as expected when split=False"
        )


# load_validate_mapper


# load_historic_data


class TestCheckSnapshotFeatherExists(object):
    """Tests for check_snapshot_feather_exists."""

    def create_feather_files(
            self,
            dir: pathlib.Path,
            first: bool,
            second: bool
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
                [True, False, False, True], # create first, check first
                [True, False, True, False], # create first, check both
                [False, True, False, False], # create second, check first
                [False, True, True, False], # create second, check both
                [True, True, True, True], # create both, check both
                [True, True, False, True] # create both, check first
            )
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
            secondary_feather_file=b
        )
        # assert result
        assert output == result, (
            "check_snapshot_feather_exists not behaving as expected."
        )
