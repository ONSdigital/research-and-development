"""Unit testing module."""
# Import testing packages
import pandas as pd
import pytest
import numpy as np

from src.staging.pg_conversion import pg_to_pg_mapper, sic_to_pg_mapper


# @pytest.fixture
# def dummy_data() -> pd.DataFrame:
#     # Set up the dummyinput  data
#     data = pd.DataFrame(
#         {"201": [0, 1, 2, 3, 4], "formtype": ["0001", "0001", "0001", "0001", "0001"]}
#     )
#     return data


# @pytest.fixture
# def mapper() -> pd.DataFrame:
#     # Set up the dummy mapper data
#     mapper = {
#         "pg_numeric": [0, 1, 2, 3, 4],
#         "pg_alpha": [np.nan, "A", "B", "C", "C"],
#     }
#     return pd.DataFrame(mapper)


# @pytest.fixture
# def expected_output() -> pd.DataFrame:
#     # Set up the dummy output data
#     expected_output = pd.DataFrame(
#         {
#             "201": [np.nan, "A", "B", "C", "C"],
#             "formtype": ["0001", "0001", "0001", "0001", "0001"],
#         }
#     )

#     expected_output["201"] = expected_output["201"].astype("category")
#     return expected_output


# @pytest.fixture
# def sic_dummy_data() -> pd.DataFrame:
#     # Set up the dummyinput  data
#     data = pd.DataFrame(
#         {"rusic": [1110, 10101], "201": [np.nan, np.nan], "formtype": ["0006", "0006"]}
#     )
#     return data


# @pytest.fixture
# def sic_mapper() -> pd.DataFrame:
#     # Set up the dummy mapper data
#     mapper = {
#         "sic": [1110, 10101],
#         "pg_alpha": ["A", "B"],
#     }
#     return pd.DataFrame(mapper)


# @pytest.fixture
# def sic_expected_output() -> pd.DataFrame:
#     # Set up the dummy output data
#     expected_output = pd.DataFrame(
#         {"rusic": [1110, 10101], "201": ["A", "B"], "formtype": ["0006", "0006"]}
#     )
#     expected_output["201"] = expected_output["201"].astype("category")
#     return expected_output


# def test_sic_mapper(sic_dummy_data, sic_expected_output, sic_mapper):
#     """Tests for pg mapper function."""

#     expected_output_data = sic_expected_output

#     df_result = sic_to_pg_mapper(sic_dummy_data, sic_mapper, target_col="201")

#     pd.testing.assert_frame_equal(df_result, expected_output_data)

@pytest.fixture
def pg_alpha_num_dummy():
    dummy_mapper = pd.DataFrame({
        "pg_numeric": [36, 37, 45, 47, 49, 50, 58],  # Include all mappings
        "pg_alpha": ["N", "Y", "AC", "AD", "AD", "AD", "AH"]  # Handle many-to-one
    })
    return dummy_mapper

def test_pg_to_pg_mapper_with_many_to_one(pg_alpha_num_dummy):
    dum_mapper = pg_alpha_num_dummy
    
    df = pd.DataFrame({
        "formtype": ["0001", "0001", "0002"],
        "201": [47, 49, 50],  # Using values from the mappings
        "other_col": ["A", "B", "C"]})
    
    expected_result = pd.DataFrame({
        "formtype": ["0001", "0001", "0002"],
        "201": [47, 49, 50],
        "other_col": ["A", "B", "C"],
        "product_group": ["AD", "AD", "AD"]
    })
    
    result = pg_to_pg_mapper(df.copy(), dum_mapper.copy())
    pd.testing.assert_frame_equal(result, expected_result)

def test_pg_to_pg_mapper_success(pg_alpha_num_dummy):
    dum_mapper = pg_alpha_num_dummy
    
    test_df = pd.DataFrame({
        "formtype": ["0001", "0001", "0002", "0001"],
        "201": [36, 45, 58, 49],  # Using values from the mappings
        "other_col": ["A", "B", "C", "D"]})
        
    expected_result = pd.DataFrame({
        "formtype": ["0001", "0001", "0002", "0001"],
        "201": [36, 45, 58, 49],
        "other_col": ["A", "B", "C", "D"],
        "product_group": ["N", "AC", "AH", "AD"]  # Updated expected values
    })
    
    result = pg_to_pg_mapper(test_df.copy(), dum_mapper.copy())
    pd.testing.assert_frame_equal(result, expected_result)