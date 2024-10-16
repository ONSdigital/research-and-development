import pandas as pd
import pytest
import logging

from src.utils.breakdown_validation import (
    get_equality_dicts,
    get_all_wanted_columns,
    run_breakdown_validation,
    replace_nulls_with_zero,
    remove_all_nulls_rows,
    equal_validation,
    greater_than_validation,
)

@pytest.fixture(scope="module")
def create_config():
    """Create a config dictionary for the tests."""
    test_config = {"consistency_checks": {
        "2xx_totals": {
            "purchases": ["222", "223", "203"],
            "sal_oth_expend": ["202", "203", "204"],
            "research_expend": ["205", "206", "207", "204"],
            "capex": ["219", "220", "209", "210"],
            "intram": ["204", "210", "211"],
            "funding": ["212", "214", "216", "242", "250", "243", "244", "245", "246", "247", "248", "249", "218"],
            "ownership": ["225", "226", "227", "228", "229", "237", "218"],
            "equality": ["211", "218"]
        },
        "3xx_totals": {
            "purchases": ['302', '303', '304', '305']
        },
        "4xx_totals": {
            "emp_civil": ["405", "407", "409", "411"],
            "emp_defence": ["406", "408", "410", "412"]
        },
        "5xx_totals": {
            "hc_res_m": ['501', '503', '505', '507'],
            "hc_res_f": ['502', '504', '506', '508'],
        },
        "apportioned_totals": {
            "employment": ["emp_researcher", "emp_technician", "emp_other", "emp_total"],
            "hc_male": ["headcount_res_m", "headcount_tec_m", "headcount_oth_m", "headcount_tot_m"],
            "hc_female": ["headcount_res_f", "headcount_tec_f", "headcount_oth_f", "headcount_tot_f"],
            "hc_tot": ["heacount_tot_m", "headcount_tot_f", "headcount_tot"]
        }
    }}
    return test_config


@pytest.fixture(scope="module")
def create_equality_dict():
    equality_dict = {
        "purchases": ["222", "223", "203"],
        "sal_oth_expend": ["202", "203", "204"],
        "research_expend": ["205", "206", "207", "204"],
        "capex": ["219", "220", "209", "210"],
        "intram": ["204", "210", "211"],
        "funding": ["212", "214", "216", "242", "250", "243", "244", "245", "246", "247", "248", "249", "218"],
        "ownership": ["225", "226", "227", "228", "229", "237", "218"],
        "equality": ["211", "218"],
        "purchases": ['302', '303', '304', '305'],
        "emp_civil": ["405", "407", "409", "411"],
        "emp_defence": ["406", "408", "410", "412"],
        "hc_res_m": ['501', '503', '505', '507'],
        "hc_res_f": ['502', '504', '506', '508'],
    }

    return equality_dict


@pytest.fixture(scope="module")
def create_equality_dict_imputation():
    equality_dict = {
        "purchases": ["222", "223", "203"],
        "sal_oth_expend": ["202", "203", "204"],
        "research_expend": ["205", "206", "207", "204"],
        "capex": ["219", "220", "209", "210"],
        "intram": ["204", "210", "211"],
        "funding": ["212", "214", "216", "242", "250", "243", "244", "245", "246", "247", "248", "249", "218"],
        "ownership": ["225", "226", "227", "228", "229", "237", "218"],
        "equality": ["211", "218"],
        "purchases": ['302', '303', '304', '305']
    }

    return equality_dict


def test_get_equality_dicts_construction(create_config, create_equality_dict):
    """Test for get_equality_dicts function in the construction case."""
    config = create_config
    expected_output = create_equality_dict
    result = get_equality_dicts(config, "default")
    assert result == expected_output


def test_get_equality_dicts_imputation(create_config, create_equality_dict_imputation):
    """Test for get__equality_dicts function in the imputation case."""
    config = create_config
    expected_output = create_equality_dict_imputation
    result = get_equality_dicts(config, "imputation")
    assert result == expected_output


def test_get_all_wanted_columns(create_config):
    """Test for get_all_wanted_columns function."""
    config = create_config
    expected_output = [
    '202', '203', '204', '205', '206', '207', "209",
    "210", "211", "212", "214", "216", "218", "219", "220",
    "225", "226", "227", "228", "229", "237",
    "242",  "243", "244", "245", "246", "247", "248", "249", "250",
     "302", "303", "304", "305",
     "501", "503", "505", "507", "502", "504", "506", "508", "405",
    "407", "409", "411", "406", "408", "410", "412"
    ]
    print(list(expected_output))

    result = get_all_wanted_columns(config)

    print([c for c in result if c not in expected_output])

    assert set(result) == set(expected_output)


class TestRemoveAllNullRows:
    """Unit tests for replace_nulls_with_zero function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = ["reference", "instance", "202", "203", "222", "223", "204", "205", "206", "207", "219", "220", "209",
                        "221", "210", "211", '212', '214', '216', '242', '250', '243', '244', '245', '246', '247', '248', '249', '218',
                        '225', '226', '227', '228', '229', '237', '302', '303', '304', '305', '501', '503', '505', '507', '502', '504', '506',
                        '508', '405', '407', '409', '411', '406', '408', '410', '412', "999"]

        data = [
            ['A', 1, 10, 30, 15, 15, 40, 10, 10, 20, 10, 15, 20, 10, 45, 85, 10, 10, 10, 10, 10, 10, 10, 10, 1, 1, 2, 1, 85, 20, 20, 20, 20, 2, 3, 50, 20, 10, 80, 50, 20, 25, 95, 60, 20, 10, 90, 80, 20, 10, 110, 80, 90, 20, 190, None],
            ['B', 1, 1, 30, 15, 15, None, 10, 10, 20, 10, 15, 20, 10, 45, None, 10, 10, None, 10, 10, 10, 10, 10, 1, 1, 2, 1, 85, 20, 20, 20, 20, 2, 3, 50, 20, 10, 80, 50, 20, 25, 95, 60, 20, 10, 90, 80, 20, 10, 110, 80, 90, 20, 190, None],
            ['C', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
        ]

        input_df = pd.DataFrame(data=data, columns=input_cols)

        return input_df

    def create_expected_df(self):
        """Create an expected dataframe for the test."""
        input_cols = ["reference", "instance", "202", "203", "222", "223", "204", "205", "206", "207", "219", "220", "209",
                        "221", "210", "211", '212', '214', '216', '242', '250', '243', '244', '245', '246', '247', '248', '249', '218',
                        '225', '226', '227', '228', '229', '237', '302', '303', '304', '305', '501', '503', '505', '507', '502', '504', '506',
                        '508', '405', '407', '409', '411', '406', '408', '410', '412', "999"]

        data = [
            ['A', 1.0, 10.0, 30.0, 15.0, 15.0, 40.0, 10.0, 10.0, 20.0, 10.0, 15.0, 20.0, 10.0, 45.0, 85.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 1.0, 1.0, 2.0, 1.0, 85.0, 20.0, 20.0, 20.0, 20.0, 2.0, 3.0, 50.0, 20.0, 10.0, 80.0, 50.0, 20.0, 25.0, 95.0, 60.0, 20.0, 10.0, 90.0, 80.0, 20.0, 10.0, 110.0, 80.0, 90.0, 20.0, 190.0, None],
            ['B', 1.0, 1.0, 30.0, 15.0, 15.0, None, 10.0, 10.0, 20.0, 10.0, 15.0, 20.0, 10.0, 45.0, None, 10.0, 10.0, None, 10.0, 10.0, 10.0, 10.0, 10.0, 1.0, 1.0, 2.0, 1.0, 85.0, 20.0, 20.0, 20.0, 20.0, 2.0, 3.0, 50.0, 20.0, 10.0, 80.0, 50.0, 20.0, 25.0, 95.0, 60.0, 20.0, 10.0, 90.0, 80.0, 20.0, 10.0, 110.0, 80.0, 90.0, 20.0, 190.0, None],
        ]

        expected_df = pd.DataFrame(data=data, columns=input_cols)

        return expected_df

    def test_remove_all_nulls_rows(self, caplog, create_equality_dict):
        """Test for remove_all_nulls_rows function."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()
        equality_dict = create_equality_dict

        with caplog.at_level(logging.INFO):
            result_df = remove_all_nulls_rows(input_df, equality_dict)
            assert "Removing rows with all null values from validation" in caplog.text
            pd.testing.assert_frame_equal(result_df, expected_df)


class TestRunBreakdownValidation():
    """Unit tests for run_breakdown_validation function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = ["reference", "instance", "202", "203", "222", "223", "204","205", "206", "207","219","220", "209",
        "221", "210", "211", '212','214','216','242','250','243','244','245','246','247','248', '249', '218',
        '225','226','227','228','229','237','302', '303','304','305','501','503','505','507','502','504','506',
        '508','405', '407','409', '411', '406', '408', '410', '412', "871"]

        data = [
             ['A', 1, 10, 30, 15, 15, 40, 10, 10, 20, 10, 15, 20, 10, 45, 85, 10, 10, 10, 10, 10, 10, 10, 10, 1, 1, 2, 1, 85, 20, 20, 20, 20, 2, 3, 50, 20, 10, 80, 50, 20, 25, 95, 60, 20, 10, 90, 80, 20, 10, 110, 80, 90, 20, 190, None],
             ['B', 1, 1, 30, 15, 15, 40, 10, 10, 20, 10, 15, 20, 10, 45, 85, 10, 10, 10, 10, 10, 10, 10, 10, 1, 1, 2, 1, 85, 20, 20, 20, 20, 2, 3, 50, 20, 10, 80, 50, 20, 25, 95, 60, 20, 10, 90, 80, 20, 10, 110, 80, 90, 20, 190, None],
             ['C', 1, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,	None, None,	None, None, None, None, None, None, None, None, None, None, None],
             ['D', 1, None, 0, None, None, 0, None, None, None, None, None, None, None, 0, 0, None, None, None, None, None, None, None, None, None, None, None, None, 0, None,	None, None,	None, None, None, None,	None, None, 0, None, None, None, 0,	None, None,	None, 0, None, None, None, 0, None,	None, None,	0, None],
             ]

        input_df = pd.DataFrame(data=data, columns=input_cols)
        input_df["is_constructed"] = True

        return input_df

    def test_breakdown_validation_success(self, caplog, create_config):
        """Test for run_breakdown_validation function where the values match."""
        input_df = self.create_input_df()
        input_df = input_df.loc[(input_df['reference'] == 'A')]
        config = create_config
        msg = 'All breakdown values are valid.\n'
        with caplog.at_level(logging.INFO):
            run_breakdown_validation(input_df, config, "constructed")
            assert msg in caplog.text

    #TODO: we're currently not raising an error but will later put this back in
    # def test_breakdown_validation_msg(self, create_config):
    #     """Test for run_breakdown_validation function to check the returned message."""
    #     input_df = self.create_input_df()
    #     input_df = input_df.loc[(input_df['reference'] == 'B')]
    #     config = create_config
    #     msg = "Columns ['202', '203'] do not equal column 204 for reference: B, instance 1.\n "
    #     with pytest.raises(ValueError) as e:
    #         run_breakdown_validation(input_df, config, "constructed")
    #     assert str(e.value) == msg

    def test_breakdown_validation_fail_all_null(self, caplog, create_config):
        """Test for run_breakdown_validation function where there are no values."""
        input_df = self.create_input_df()
        input_df = input_df.loc[(input_df['reference'] == 'C')]
        config = create_config
        msg = 'All breakdown values are valid.\n'
        with caplog.at_level(logging.INFO):
            run_breakdown_validation(input_df, config, "constructed")
            assert msg in caplog.text

    def test_breakdown_validation_fail_totals_zero(self, caplog, create_config):
        """Test for run_breakdown_validation function where there are zeros."""
        input_df = self.create_input_df()
        input_df = input_df.loc[(input_df['reference'] == 'D')]
        config = create_config
        msg = 'All breakdown values are valid.\n'
        with caplog.at_level(logging.INFO):
            run_breakdown_validation(input_df, config, "constructed")
            assert msg in caplog.text

class TestReplaceNullsWithZero:
    """Unit tests for replace_nulls_with_zero function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = ["reference", "instance", "202", "203", "222", "223", "204", "205", "206", "207", "219", "220", "209",
                        "221", "210", "211", '212', '214', '216', '242', '250', '243', '244', '245', '246', '247', '248', '249', '218',
                        '225', '226', '227', '228', '229', '237', '302', '303', '304', '305', '501', '503', '505', '507', '502', '504', '506',
                        '508', '405', '407', '409', '411', '406', '408', '410', '412', "999"]

        data = [
            ['A', 1, 10, 30, 15, 15, 40, 10, 10, 20, 10, 15, 20, 10, 45, 85, 10, 10, 10, 10, 10, 10, 10, 10, 1, 1, 2, 1, 85, 20, 20, 20, 20, 2, 3, 50, 20, 10, 80, 50, 20, 25, 95, 60, 20, 10, 90, 80, 20, 10, 110, 80, 90, 20, 190, None],
            ['B', 1, 1, 30, 15, 15, 40, 10, 10, 20, 10, 15, 20, 10, 45, 85, 10, 10, 10, 10, 10, 10, 10, 10, 1, 1, 2, 1, 85, 20, 20, 20, 20, 2, 3, 50, 20, 10, 80, 50, 20, 25, 95, 60, 20, 10, 90, 80, 20, 10, 110, 80, 90, 20, 190, None],
            ['C', 1, None, 30, None, None, 0, None, None, None, None, None, None, None, 0, 0, None, None, None, None, None, None, None, None, None, None, None, None, 0, None, None, None, None, None, None, None, None, None, 0, None, None, None, 0, None, None, None, 0, None, None, None, 0, None, None, None, 0, None],
        ]

        input_df = pd.DataFrame(data=data, columns=input_cols)

        return input_df

    def test_replace_nulls_with_zero(self, caplog, create_equality_dict):
        """Test for replace_nulls_with_zero function where nulls are replaced with zeros."""
        input_df = self.create_input_df()
        expected_df = input_df.copy()
        equals_dict = create_equality_dict
        expected_df.loc[2, ["202", "205", "206", "207", "219", "220", "209", "210", "211", '212', '214', '216', '242', '250', '243', '244', '245', '246', '247', '248', '249', '218', '225', '226', '227', '228', '229', '237', '302', '303', '304', '305', '501', '503', '505', '507', '502', '504', '506', '508', '405', '407', '409', '411', '406', '408', '410', '412']] = 0

        with caplog.at_level(logging.INFO):
            result_df = replace_nulls_with_zero(input_df, equals_dict)
            assert "Replacing nulls with zeros where total zero" in caplog.text
            pd.testing.assert_frame_equal(result_df, expected_df)


class TestRemoveAllNullRows:
    """Unit tests for replace_nulls_with_zero function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = ["reference", "instance", "202", "203", "222", "223", "204", "205", "206", "207", "219", "220", "209",
                        "221", "210", "211", '212', '214', '216', '242', '250', '243', '244', '245', '246', '247', '248', '249', '218',
                        '225', '226', '227', '228', '229', '237', '302', '303', '304', '305', '501', '503', '505', '507', '502', '504', '506',
                        '508', '405', '407', '409', '411', '406', '408', '410', '412', "999"]

        data = [
            ['A', 1, 10, 30, 15, 15, 40, 10, 10, 20, 10, 15, 20, 10, 45, 85, 10, 10, 10, 10, 10, 10, 10, 10, 1, 1, 2, 1, 85, 20, 20, 20, 20, 2, 3, 50, 20, 10, 80, 50, 20, 25, 95, 60, 20, 10, 90, 80, 20, 10, 110, 80, 90, 20, 190, None],
            ['B', 1, 1, 30, 15, 15, None, 10, 10, 20, 10, 15, 20, 10, 45, None, 10, 10, None, 10, 10, 10, 10, 10, 1, 1, 2, 1, 85, 20, 20, 20, 20, 2, 3, 50, 20, 10, 80, 50, 20, 25, 95, 60, 20, 10, 90, 80, 20, 10, 110, 80, 90, 20, 190, None],
            ['C', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
        ]

        input_df = pd.DataFrame(data=data, columns=input_cols)

        return input_df

    def create_expected_df(self):
        """Create an expected dataframe for the test."""
        input_cols = ["reference", "instance", "202", "203", "222", "223", "204", "205", "206", "207", "219", "220", "209",
                        "221", "210", "211", '212', '214', '216', '242', '250', '243', '244', '245', '246', '247', '248', '249', '218',
                        '225', '226', '227', '228', '229', '237', '302', '303', '304', '305', '501', '503', '505', '507', '502', '504', '506',
                        '508', '405', '407', '409', '411', '406', '408', '410', '412', "999"]

        data = [
            ['A', 1.0, 10.0, 30.0, 15.0, 15.0, 40.0, 10.0, 10.0, 20.0, 10.0, 15.0, 20.0, 10.0, 45.0, 85.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 1.0, 1.0, 2.0, 1.0, 85.0, 20.0, 20.0, 20.0, 20.0, 2.0, 3.0, 50.0, 20.0, 10.0, 80.0, 50.0, 20.0, 25.0, 95.0, 60.0, 20.0, 10.0, 90.0, 80.0, 20.0, 10.0, 110.0, 80.0, 90.0, 20.0, 190.0, None],
            ['B', 1.0, 1.0, 30.0, 15.0, 15.0, None, 10.0, 10.0, 20.0, 10.0, 15.0, 20.0, 10.0, 45.0, None, 10.0, 10.0, None, 10.0, 10.0, 10.0, 10.0, 10.0, 1.0, 1.0, 2.0, 1.0, 85.0, 20.0, 20.0, 20.0, 20.0, 2.0, 3.0, 50.0, 20.0, 10.0, 80.0, 50.0, 20.0, 25.0, 95.0, 60.0, 20.0, 10.0, 90.0, 80.0, 20.0, 10.0, 110.0, 80.0, 90.0, 20.0, 190.0, None],
        ]

        expected_df = pd.DataFrame(data=data, columns=input_cols)

        return expected_df

    def test_remove_all_nulls_rows(self, caplog, create_config):
        """Test for remove_all_nulls_rows function."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()
        config = create_config

        with caplog.at_level(logging.INFO):
            result_df = remove_all_nulls_rows(input_df, config)
            assert "Removing rows with all null values from validation" in caplog.text
            pd.testing.assert_frame_equal(result_df, expected_df)


class TestEqualValidation:
    """Unit tests for equal_validation function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = ["reference", "instance", "202", "203", "222", "223", "204", "205", "206", "207", "219", "220", "209",
                        "221", "210", "211", '212', '214', '216', '242', '250', '243', '244', '245', '246', '247', '248', '249', '218',
                        '225', '226', '227', '228', '229', '237', '302', '303', '304', '305', '501', '503', '505', '507', '502', '504', '506',
                        '508', '405', '407', '409', '411', '406', '408', '410', '412', "871"]

        data = [
            ['A', 1, 10, 30, 15, 15, 40, 10, 10, 20, 10, 15, 20, 10, 45, 85, 10, 10, 10, 10, 10, 10, 10, 10, 1, 1, 2, 1, 85, 20, 20, 20, 20, 2, 3, 50, 20, 10, 80, 50, 20, 25, 95, 60, 20, 10, 90, 80, 20, 10, 110, 80, 90, 20, 190, None],
            ['B', 1, 1, 30, 15, 15, 40, 10, 10, 20, 10, 15, 20, 10, 45, 85, 10, 10, 10, 10, 10, 10, 10, 10, 1, 1, 2, 1, 85, 20, 20, 20, 20, 2, 3, 50, 20, 10, 80, 50, 20, 25, 95, 60, 20, 10, 90, 80, 20, 10, 110, 80, 90, 20, 190, None],
            ['C', 1, None, 0, None, None, 0, None, None, None, None, None, None, None, 0, 0, None, None, None, None, None, None, None, None, None, None, None, None, 0, None, None, None, None, None, None, None, None, None, 0, None, None, None, 0, None, None, None, 0, None, None, None, 0, None, None, None, 0, None],
        ]

        input_df = pd.DataFrame(data=data, columns=input_cols)

        return input_df


    def test_equal_validation_success(self, caplog, create_equality_dict):
        """Test for equal_validation function where the values match."""
        input_df = self.create_input_df()
        input_df = input_df.loc[(input_df['reference'] == 'A')]
        equality_dict = create_equality_dict
        msg = ""
        count = 0
        with caplog.at_level(logging.INFO):
            result_msg, result_count = equal_validation(input_df, equality_dict)
            assert "Doing breakdown total checks..." in caplog.text
            assert result_msg == msg
            assert result_count == count

    def test_equal_validation_fail(self, create_equality_dict):
        """Test for equal_validation function where the values do not meet the criteria."""
        input_df = self.create_input_df()
        input_df = input_df.loc[(input_df['reference'] == 'B')].reset_index(drop=True)
        equality_dict = create_equality_dict
        msg = "Columns ['202', '203'] do not equal column 204 for reference: B, instance 1.\n "
        count = 1
        result_msg, result_count = equal_validation(input_df, equality_dict)
        assert result_msg == msg
        assert result_count == count

    def test_equal_validation_all_null(self, caplog, create_equality_dict):
        """Test for equal_validation function where all values are zero or null."""
        input_df = self.create_input_df()
        input_df = input_df.loc[(input_df['reference'] == 'C')].reset_index(drop=True)
        equals_dict = create_equality_dict
        msg = ""
        count = 0
        with caplog.at_level(logging.INFO):
            result_msg, result_count = equal_validation(input_df, equals_dict)
            assert "Doing breakdown total checks..." in caplog.text
            assert result_msg == msg
            assert result_count == count

class TestGreaterThanValidation:
    """Unit tests for greater_than_validation function."""

    def create_input_df(self):
        """Create an input dataframe for the test."""
        input_cols = ["reference", "instance", "202", "203", "222", "223", "204", "205", "206", "207", "219", "220", "209",
                        "221", "210", "211", '212', '214', '216', '242', '250', '243', '244', '245', '246', '247', '248', '249', '218',
                        '225', '226', '227', '228', '229', '237', '302', '303', '304', '305', '501', '503', '505', '507', '502', '504', '506',
                        '508', '405', '407', '409', '411', '406', '408', '410', '412', "871"]

        data = [
            ['A', 1, 10, 30, 15, 15, 40, 10, 10, 20, 10, 15, 20, 10, 45, 85, 10, 10, 10, 10, 10, 10, 10, 10, 1, 1, 2, 1, 85, 20, 20, 20, 20, 2, 3, 50, 20, 10, 80, 50, 20, 25, 95, 60, 20, 10, 90, 80, 20, 10, 110, 80, 90, 20, 190, None],
            ['B', 1, 10, 20, 30, 40, 100, 10, 20, 30, 40, 50, 60, 70, 80, 90, 10, 20, 30, 40, 50, 60, 70, 80, 1, 2, 3, 4, 100, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230, 240, 250, 260, 270, None],
            ['C', 1, None, 0, None, None, 0, None, None, None, None, None, None, None, 0, 0, None, None, None, None, None, None, None, None, None, None, None, None, 0, None, None, None, None, None, None, None, None, None, 0, None, None, None, 0, None, None, None, 0, None, None, None, 0, None, None, None, 0, None],
        ]

        input_df = pd.DataFrame(data=data, columns=input_cols)

        return input_df

    def test_greater_than_validation_success(self, caplog):
        """Test for greater_than_validation function where the values meet the criteria."""
        input_df = self.create_input_df()
        input_df = input_df.loc[(input_df['reference'] == 'A')]
        msg = ""
        count = 0
        with caplog.at_level(logging.INFO):
            result_msg, result_count = greater_than_validation(input_df, msg, count)
            assert "Doing checks for values that should be greater than..." in caplog.text
            assert result_msg == msg
            assert result_count == count

    def test_greater_than_validation_partial_match(self):
        """Test for greater_than_validation function where some values match and some do not."""
        input_df = self.create_input_df()
        input_df = input_df.loc[(input_df['reference'] == 'B')].reset_index(drop=True)
        msg = "Column 221 is greater than 209 for reference: B, instance 1.\n "
        count = 1
        result_msg, result_count = greater_than_validation(input_df, "", 0)
        assert result_msg == msg
        assert result_count == count

    def test_greater_than_validation_all_null(self, caplog):
        """Test for greater_than_validation function where all values are null."""
        input_df = self.create_input_df()
        input_df = input_df.loc[(input_df['reference'] == 'C')]
        msg = ""
        count = 0
        with caplog.at_level(logging.INFO):
            result_msg, result_count = greater_than_validation(input_df, msg, count)
            assert "Doing checks for values that should be greater than..." in caplog.text
            assert result_msg == msg
            assert result_count == count
