import pytest
import pandas as pd
import numpy as np

# Local Imports
from src.mapping.mapping_helpers import (
    mapper_null_checks,
    col_validation_checks,
    check_mapping_unique,
    update_ref_list,
    create_additional_ni_cols,
    join_with_null_check
)

from src.utils.path_helpers import validate_mapping_filenames

class TestJoinWithNullCheck(object):
    """Tests for join_with_null_check function."""

    def main_input_df(self):
        """Main input data for join_with_null_check tests."""
        columns = ["reference", "instance", "formtype", "cellnumber", "selectiontype"]
        data = [
            [49900001031, 0, "0006", 674, "C"],
            [49900001530, 0, "0006", 805, "P"],
            [49900001601, 0, "0001", 117, "C"],
            [49900001601, 1, "0001", 117, "C"],
            [49900003099, 0, "0006", 41, "L"],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    def mapper_df(self):
        """Sample mapper for testing."""
        columns = ["cellnumber", "uni_count"]
        data = [
            [674, 23],
            [805, 14],
            [117, 13147],
            [41, 87577],
            [817, 9],
        ]
        df = pd.DataFrame(data=data, columns=columns)
        return df

    def expected_output(self):
        """Expected output for join_with_null_check tests."""
        columns = [
            "reference",
            "instance",
            "formtype",
            "cellnumber",
            "selectiontype",
            "uni_count",
        ]
        data = [
            [49900001031, 0, "0006", 674, "C", 23],
            [49900001530, 0, "0006", 805, "P", 14],
            [49900001601, 0, "0001", 117, "C", 13147],
            [49900001601, 1, "0001", 117, "C", 13147],
            [49900003099, 0, "0006", 41, "L", 87577],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    def test_join_with_null_check_success(self):
        """General tests for join_with_null_check."""
        main_input_df = self.main_input_df()
        mapper_df = self.mapper_df()
        expected_output = self.expected_output()
        output = join_with_null_check(
            main_input_df, mapper_df, "test_mapper", "cellnumber"
        )
        assert output.equals(
            expected_output
        ), "join_with_null_check not behaving as expected."

    def test_join_with_null_check_failure(self):
        """Test the raises in join_with_null_check."""
        main_input_df = self.main_input_df()
        mapper_df = self.mapper_df()
        mapper_df = mapper_df.drop(0)
        error_msg = (
            "Nulls found in the join on cellnumber of test_mapper mapper."
            "The following cellnumber values are not in the test_mapper mapper: \[674\]"
        )

        with pytest.raises(ValueError, match=error_msg):
            join_with_null_check(main_input_df, mapper_df, "test_mapper", "cellnumber")


@pytest.fixture(scope="module")
def test_mapper_df():
    """Sample mapper for testing."""
    columns = ["ruref", "ultfoc"]
    data = [
        ["abc", "AB"],
        ["def", "EF"],
        ["ghi", "IJ"],
        ["jkl", "MN"],
        ["mno", "QR"],
        ["pqr", None],
    ]
    return pd.DataFrame(data=data, columns=columns)


class TestColValidationChecks(object):
    """Tests for col_validation_checks."""

    def test_col_validation_checks_type(self, test_mapper_df):
        """Test col_validation_checks for type validation."""
        with pytest.raises(ValueError):
            col_validation_checks(test_mapper_df, "test_mapper", "ruref", int, None)

    def test_col_validation_checks_length(self, test_mapper_df):
        """Test col_validation_checks for length validation."""
        with pytest.raises(ValueError):
            col_validation_checks(test_mapper_df, "test_mapper", "ultfoc", None, 3)

    def test_col_validation_checks_capitalisation(self, test_mapper_df):
        """Test col_validation_checks for capitalisation validation."""
        with pytest.raises(ValueError):
            col_validation_checks(
                test_mapper_df, "test_mapper", "ruref", None, None, True
            )

    def test_col_validation_checks_pass(self, test_mapper_df):
        """Test col_validation_checks for passing all checks."""
        col_validation_checks(test_mapper_df, "test_mapper", "ruref", str, None)
        col_validation_checks(test_mapper_df, "test_mapper", "ultfoc", str, 2)
        col_validation_checks(test_mapper_df, "test_mapper", "ultfoc", str, None, True)


class TestCheckMappingUnique(object):
    """Tests for check_mapping_unique."""

    @pytest.fixture(scope="function")
    def test_mapper_nonunique_df(self):
        """Sample mapper for testing."""
        columns = ["ruref", "ultfoc"]
        data = [
            ["abc", "AB"],
            ["def", "EF"],
            ["ghi", "AB"],
            ["jkl", "MN"],
            ["mno", "AB"],
            ["pqr", None],
        ]
        return pd.DataFrame(data=data, columns=columns)

    def test_check_mapping_unique_unique(self, test_mapper_nonunique_df):
        """Test check_mapping_unique for a column with unique values."""
        check_mapping_unique(test_mapper_nonunique_df, "ruref")

    def test_check_mapping_unique_not_unique(self, test_mapper_nonunique_df):
        """Test check_mapping_unique for a column without unique values."""
        with pytest.raises(ValueError):
            check_mapping_unique(test_mapper_nonunique_df, "ultfoc")


class TestUpdateRefList(object):
    """Tests for update_ref_list."""

    @pytest.fixture(scope="function")
    def full_input_df(self):
        """Main input data for update_ref_list tests."""
        columns = ["reference", "instance", "formtype", "cellnumber", "selectiontype"]
        data = [
            [49900001031, 0, "0006", 674, "A"],
            [49900001530, 0, "0006", 805, "B"],
            [49900001601, 0, "0001", 117, "C"],
            [49900001601, 1, "0001", 117, "D"],
            [49900003099, 0, "0006", 41, "E"],
        ]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    @pytest.fixture(scope="function")
    def ref_list_input(self):
        """Reference list df input for update_ref_list tests."""
        columns = ["reference", "cellnumber", "selectiontype", "formtype"]
        data = [[49900001601, 117, "C", "1"]]
        df = pd.DataFrame(columns=columns, data=data)
        return df

    @pytest.fixture(scope="function")
    def expected_output(self):
        """Expected output for update_ref_list tests."""
        columns = ["reference", "instance", "formtype", "cellnumber", "selectiontype"]
        data = [
            [49900001031, 0, "0006", 674, "A"],
            [49900001530, 0, "0006", 805, "B"],
            [49900001601, 0, "0001", 817, "L"],
            [49900001601, 1, "0001", 817, "L"],
            [49900003099, 0, "0006", 41, "E"],
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


class TestCreateAdditionalNiCols(object):
    """Tests for create_additional_ni_cols function."""

    def test_create_additional_ni_cols(self):
        """Test create_additional_ni_cols function."""
        # Create sample input DataFrame
        columns = ["reference", "value"]
        data = [
            [1, 10],
            [2, 20],
            [3, 30],
        ]
        df = pd.DataFrame(data=data, columns=columns)

        # Expected output DataFrame
        expected_columns = [
            "reference",
            "value",
            "a_weight",
            "604",
            "form_status",
            "602",
            "formtype",
        ]
        expected_data = [
            [1, 10, 1, "Yes", 600, 100.0, "0003"],
            [2, 20, 1, "Yes", 600, 100.0, "0003"],
            [3, 30, 1, "Yes", 600, 100.0, "0003"],
        ]
        expected_df = pd.DataFrame(data=expected_data, columns=expected_columns)

        # Call the function
        output_df = create_additional_ni_cols(df)

        # Check if the output matches the expected DataFrame
        assert output_df.equals(
            expected_df
        ), "Output from create_additional_ni_cols not as expected."

class TestValidateMappingFilenames(object):
    @pytest.fixture(scope="function")
    def test_validate_mapper_config_raises_file_incorrect(self):
        config = {
            'years' : {'survey_year': 2022,},
            '2022_mappers': {'itl_mapper_path': "itl_2022.cs",
            'ultfoc_mapper_path': "BERD_2022_ultfoc.csv",},
            '2023_mappers': {'itl_mapper_path': "itl_2023.csv",
            'ultfoc_mapper_path': None,}
            } 

        result, message = validate_mapping_filenames(config)
                
        expected_bool = {False, True}

        assert result.equals(
            expected_bool
            ), "Output from test_validate_mapper_config_raises_file_incorrect not as expected."

    @pytest.fixture(scope="function")
    def test_validate_mapper_config_raises_year_incorrect(self):
        config = {
            'years' : {'survey_year': 2022,},
            '2022_mappers': {'itl_mapper_path': "itl_202.csv",
            'ultfoc_mapper_path': "BERD_2022_ultfoc.csv",},
            '2023_mappers': {'itl_mapper_path': "itl_2023.csv",
            'ultfoc_mapper_path': None,}
            } 

        result, message = validate_mapping_filenames(config)

        expected_bool = {False, True}

        assert result.equals(
            expected_bool
            ), "Output from test_validate_mapper_config_raises_year_incorrect not as expected."


    @pytest.fixture(scope="function")
    def test_validate_mapper_config_raises_missing_file(self):
        config = {
            'years' : {'survey_year': 2022,},
            '2022_mappers': {'itl_mapper_path': "",
            'ultfoc_mapper_path': "BERD_2022_ultfoc.csv",},
            '2023_mappers': {'itl_mapper_path': "itl_2023.csv",
            'ultfoc_mapper_path': None,}
            } 

        result, message = validate_mapping_filenames(config)

        expected_bool = {False, False}

        assert result.equals(
            expected_bool
            ), "Output from test_validate_mapper_config_raises_missing_file not as expected."
