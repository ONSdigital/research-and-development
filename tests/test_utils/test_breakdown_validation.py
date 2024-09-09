from src.utils.breakdown_validation import (
    run_breakdown_validation,
    replace_nulls_with_zero,
    remove_all_nulls_rows,
    equal_validation,
    greater_than_validation,
)
import pandas as pd
import pytest
import logging

class TestRunBreakdownValidation:
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
             ['C', 1,],
             ['D', 1, None, 0, None, None, 0, None, None, None, None, None, None, None, 0, 0, None, None, None, None, None, None, None, None, None, None, None, None, 0, None,	None, None,	None, None, None, None,	None, None, 0, None, None, None, 0,	None, None,	None, 0, None, None, None, 0, None,	None, None,	0, None],
             ]

        input_df = pd.DataFrame(data=data, columns=input_cols)

        return input_df


    def test_breakdown_validation_success(self, caplog):
        """Test for run_breakdown_validation function where the values match."""
        input_df = self.create_input_df()
        input_df = input_df.loc[(input_df['reference'] == 'A')]
        msg = 'All breakdown values are valid.\n'
        with caplog.at_level(logging.INFO):
            run_breakdown_validation(input_df)
            assert msg in caplog.text

    def test_breakdown_validation_fail(self):
        """Test for run_breakdown_validation function where the values do not meet the criteria."""
        input_df = self.create_input_df()
        input_df = input_df.loc[(input_df['reference'] == 'B')]
        with pytest.raises(ValueError):
            run_breakdown_validation(input_df)

    def test_breakdown_validation_fail_all_null(self, caplog):
        """Test for run_breakdown_validation function where the values do not meet the criteria."""
        input_df = self.create_input_df()
        input_df = input_df.loc[(input_df['reference'] == 'C')]
        msg = 'All breakdown values are valid.\n'
        with caplog.at_level(logging.INFO):
            run_breakdown_validation(input_df)
            assert msg in caplog.text

    def test_breakdown_validation_fail_totals_zero(self, caplog):
        """Test for run_breakdown_validation function where the values do not meet the criteria."""
        input_df = self.create_input_df()
        input_df = input_df.loc[(input_df['reference'] == 'D')]
        msg = 'All breakdown values are valid.\n'
        with caplog.at_level(logging.INFO):
            run_breakdown_validation(input_df)
            assert msg in caplog.text

    def test_breakdown_validation_msg(self):
        """Test for run_breakdown_validation function to check the returned message."""
        input_df = self.create_input_df()
        input_df = input_df.loc[(input_df['reference'] == 'B')]
        msg = "Columns ['202', '203'] do not equal column 204 for reference: B, instance 1.\n "
        with pytest.raises(ValueError) as e:
            run_breakdown_validation(input_df)
        assert str(e.value) == msg


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

    def test_replace_nulls_with_zero(self, caplog):
        """Test for replace_nulls_with_zero function where nulls are replaced with zeros."""
        input_df = self.create_input_df()
        expected_df = input_df.copy()
        expected_df.loc[2, ["202", "205", "206", "207", "219", "220", "209", "210", "211", '212', '214', '216', '242', '250', '243', '244', '245', '246', '247', '248', '249', '218', '225', '226', '227', '228', '229', '237', '302', '303', '304', '305', '501', '503', '505', '507', '502', '504', '506', '508', '405', '407', '409', '411', '406', '408', '410', '412']] = 0

        with caplog.at_level(logging.INFO):
            result_df = replace_nulls_with_zero(input_df)
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

    def test_remove_all_nulls_rows(self, caplog):
        """Test for remove_all_nulls_rows function."""
        input_df = self.create_input_df()
        expected_df = self.create_expected_df()

        with caplog.at_level(logging.INFO):
            result_df = remove_all_nulls_rows(input_df)
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
            ['C', 1, 10, 20, 30, 40, 100, 10, 20, 30, 40, 50, 60, 70, 80, 90, 10, 20, 30, 40, 50, 60, 70, 80, 1, 2, 3, 4, 100, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230, 240, 250, 260, 270, None],
            ['D', 1, None, 0, None, None, 0, None, None, None, None, None, None, None, 0, 0, None, None, None, None, None, None, None, None, None, None, None, None, 0, None, None, None, None, None, None, None, None, None, 0, None, None, None, 0, None, None, None, 0, None, None, None, 0, None, None, None, 0, None],
        ]

        input_df = pd.DataFrame(data=data, columns=input_cols)

        return input_df

    def test_equal_validation_success(self, caplog):
        """Test for equal_validation function where the values match."""
        input_df = self.create_input_df()
        input_df = input_df.loc[(input_df['reference'] == 'A')]
        msg = ""
        count = 0
        with caplog.at_level(logging.INFO):
            result_msg, result_count = equal_validation(input_df)
            assert "Doing breakdown total checks..." in caplog.text
            assert result_msg == msg
            assert result_count == count

    def test_equal_validation_fail(self):
        """Test for equal_validation function where the values do not meet the criteria."""
        input_df = self.create_input_df()
        input_df = input_df.loc[(input_df['reference'] == 'B')].reset_index(drop=True)
        msg = "Columns ['202', '203'] do not equal column 204 for reference: B, instance 1.\n "
        count = 1
        result_msg, result_count = equal_validation(input_df)
        assert result_msg == msg
        assert result_count == count

    def test_equal_validation_all_null(self, caplog):
        """Test for equal_validation function where all values are null."""
        input_df = self.create_input_df()
        input_df = input_df.loc[(input_df['reference'] == 'D')].reset_index(drop=True)
        msg = ""
        count = 0
        with caplog.at_level(logging.INFO):
            result_msg, result_count = equal_validation(input_df)
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
            ['B', 1, 1, 30, 15, 15, 40, 10, 10, 20, 10, 15, 20, 10, 45, 85, 10, 10, 10, 10, 10, 10, 10, 10, 1, 1, 2, 1, 85, 20, 20, 20, 20, 2, 3, 50, 20, 10, 80, 50, 20, 25, 95, 60, 20, 10, 90, 80, 20, 10, 110, 80, 90, 20, 190, None],
            ['C', 1, 10, 20, 30, 40, 100, 10, 20, 30, 40, 50, 60, 70, 80, 90, 10, 20, 30, 40, 50, 60, 70, 80, 1, 2, 3, 4, 100, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230, 240, 250, 260, 270, None],
            ['D', 1, None, 0, None, None, 0, None, None, None, None, None, None, None, 0, 0, None, None, None, None, None, None, None, None, None, None, None, None, 0, None, None, None, None, None, None, None, None, None, 0, None, None, None, 0, None, None, None, 0, None, None, None, 0, None, None, None, 0, None],
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

    # def test_greater_than_validation_fail(self):
    #     """Test for greater_than_validation function where the values do not meet the criteria."""
    #     input_df = self.create_input_df()
    #     input_df = input_df.loc[(input_df['reference'] == 'B')]
    #     msg = "Column 221 is greater than 209 for reference: B, instance 1.\n "
    #     count = 1
    #     result_msg, result_count = greater_than_validation(input_df, "", 0)
    #     print(result_msg)
    #     assert result_msg == msg
    #     assert result_count == count

    def test_greater_than_validation_all_null(self, caplog):
        """Test for greater_than_validation function where all values are null."""
        input_df = self.create_input_df()
        input_df = input_df.loc[(input_df['reference'] == 'D')]
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
        input_df = input_df.loc[(input_df['reference'] == 'C')].reset_index(drop=True)
        msg = "Column 221 is greater than 209 for reference: C, instance 1.\n "
        count = 1
        result_msg, result_count = greater_than_validation(input_df, "", 0)
        assert result_msg == msg
        assert result_count == count



