"""Tests for 'staging_helpers.py'."""
# Standard Library Imports
import pytest

# Third Party Imports
import pandas as pd
import numpy as np

# Local Imports
from src.staging.staging_helpers import (
    # postcode_topup,
    fix_anon_data,
    update_ref_list,
    getmappername,
    load_valdiate_mapper,
    load_historic_data,
    check_snapshot_feather_exists,
    load_val_snapshot_json,
    load_validate_secondary_snapshot,
    write_snapshot_to_feather,
    stage_validate_harmonise_postcodes
)


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
        