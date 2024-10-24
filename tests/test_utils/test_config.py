"""Tests for config.py."""

import pytest
import os
import pathlib
from typing import Union

import yaml

from src.utils.config import (
    safeload_yaml,
    merge_configs,
    validate_config,
    _validate_numeric,
    _check_has_schema,
    _validate_path,
    _nulltype_conversion,
    validate_freezing_config_settings,
    validate_construction_config_settings,
    validate_freezing_run_config
)


def write_dict_to_yaml(_dict: dict, path: Union[str, pathlib.Path]) -> None:
    """Write a dictionary as a yaml file

    Args:
        _dict (dict): The data to save to the yaml file.
        path (Union[str, pathlib.Path]): The save path (including file ext).

    Returns:
        None
    """
    with open(path, "w") as f:
        yaml.dump(_dict, f, default_flow_style=False)


class TestCheckHasSchema(object):
    """Tests for _check_has_schema."""

    def test__check_has_schema_true(self):
        """Positive tests for _check_has_schema"""
        test_dict = {"singular": False, "dtype": "str", "accept_nonetype": False}
        assert _check_has_schema(
            test_dict
        ), "Expected results to be true. Schema not recognised."

    def test__check_has_schema_false(self):
        """Negative tests for _check_has_schema"""
        test_dict = {"test": "no_schema"}
        assert not _check_has_schema(
            test_dict
        ), "Expected results to be False. Schema incorrectly recognised."


class TestValidateNumeric(object):
    """Tests for _validate_numeric."""

    @pytest.fixture(scope="function")
    def numeric_config(self) -> dict:
        """A dummy config for numeric ranges."""
        config = dict(min=5, max=15)
        return config

    def test__validate_numeric(self, numeric_config):
        """General tests for _validate_numeric."""
        # central value
        _validate_numeric(10, "test", numeric_config)
        # on max
        _validate_numeric(15, "test", numeric_config)
        # on min
        _validate_numeric(5, "test", numeric_config)

    @pytest.mark.parametrize(
        "value, msg",
        [
            (4, r"Config value for test .*4.* less than min .*5.*"),
            (16, r"Config value for test .*16.* greater than max .*15.*"),
        ],
    )
    def test__validate_numeric_raises(self, value, msg, numeric_config):
        """General tests for _validate_numeric."""
        with pytest.raises(ValueError, match=msg):
            _validate_numeric(value, "test", numeric_config)


class TestNulltypeConversion(object):
    """Tests for _nulltype_conversion."""

    @pytest.mark.parametrize(
        "value, return_value",
        [
            ("", None),
            ("null", None),
            ("NULL", None),
            ("none", None),
            ("None", None),
            ("Not null", "Not null"),
        ],
    )
    def test__nulltype_conversion(self, value, return_value):
        """General tests for _nulltype_conversion."""
        assert (
            _nulltype_conversion(value) == return_value
        ), f"_nulltype_conversion expected to return {return_value}. Got {value}"


class TestValidatePath(object):
    """Tests for _validate_path."""

    @pytest.fixture(scope="function")
    def path_config(self) -> dict:
        config = {"filetype": "txt"}
        return config

    def test__validate_path_raises(self, path_config):
        """Test _validate_path raises."""
        msg = r"Expected file extension .txt for test/path.test. Got .test"
        with pytest.raises(TypeError, match=msg):
            _validate_path("test/path.test", path_config)


class TestMergeConfigs(object):
    """Tests for merge_configs."""

    def test_merge_configs(self):
        """General tests for merge_configs."""
        conf1 = {"test": 1, "test2": {"sub1": 1}}
        conf2 = {"tester": 1, "test2": {"sub2": 2}}
        expected = {"test": 1, "test2": {"sub1": 1, "sub2": 2}, "tester": 1}
        resultant = merge_configs(conf1, conf2)
        assert expected == resultant, "Merged configs not as expected."

    def test_merge_configs_raises_top_level(self):
        """Test that merge_configs raises overlapping keys."""
        test1 = {"test": 1}
        test2 = {"test": 2}
        msg = r"Overlapping keys in configs: test"
        with pytest.raises(ValueError, match=msg):
            merge_configs(test1, test2)

    def test_merge_configs_raises_second_level(self):
        """Test that merge_configs raises overlapping keys."""
        test1 = {"test": {"test_again": 1}}
        test2 = {"test": {"test_again": 2}}
        msg = r"Overlapping keys in configs: test:test_again"
        with pytest.raises(ValueError, match=msg):
            merge_configs(test1, test2)


class TestValidateConfig(object):
    """Tests for validate_config.

    This will also act as a space to test _check_items.
    """

    @pytest.fixture(scope="function")
    def dummy_validation(self):
        """A dummy config validation for use in tests."""
        validation = {
            "tester": {
                "singular": True,
                "dtype": "int",
                "accept_nonetype": False,
                "max": 15,
                "min": 5,
            }
        }
        return validation

    def test_validate_config(self, dummy_validation, tmp_path):
        """General testss for validate_config."""
        validation_path = os.path.join(tmp_path, "conf_val.yaml")
        write_dict_to_yaml(dummy_validation, validation_path)
        dummy_config = {
            "config_validation": {"validate": True, "path": validation_path},
            "tester": 14,
        }
        validate_config(dummy_config)

    def test_validate_config_no_validate(self, dummy_validation, tmp_path):
        """Test that None is returned when validate=False."""
        validation_path = os.path.join(tmp_path, "conf_val.yaml")
        write_dict_to_yaml(dummy_validation, validation_path)
        dummy_config = {
            "config_validation": {"validate": False, "path": validation_path},
            "tester": 14,
        }
        assert isinstance(
            validate_config(dummy_config), type(None)
        ), "validate_config expected to return None."

    @pytest.mark.parametrize(
        "val, error_type, msg",
        (
            [
                16,
                ValueError,
                (r"Config value for tester:tester .*16.* greater than " r"max .*15.*"),
            ],
            [
                4,
                ValueError,
                (r"Config value for tester:tester .*4.* less than min " r".*5.*."),
            ],
            ["tester", TypeError, r".*tester:tester.* expected .*int.* Got .*str.*"],
        ),
    )
    def test_validate_config_ints(
        self, dummy_validation, tmp_path, val, error_type, msg
    ):
        """Tests for validating an integer."""
        validation_path = os.path.join(tmp_path, "conf_val.yaml")
        write_dict_to_yaml(dummy_validation, validation_path)
        dummy_config = {
            "config_validation": {"validate": True, "path": validation_path},
            "tester": val,
        }
        with pytest.raises(error_type, match=msg):
            validate_config(dummy_config)

    @pytest.mark.parametrize("value", (None, "None", ""))
    def test_validate_config_accept_nonetypes(self, dummy_validation, tmp_path, value):
        """Test that nonetypes are accepted."""
        dummy_validation["tester"]["accept_nonetype"] = True
        validation_path = os.path.join(tmp_path, "conf_val.yaml")
        write_dict_to_yaml(dummy_validation, validation_path)
        dummy_config = {
            "config_validation": {"validate": True, "path": validation_path},
            "tester": value,
        }
        validate_config(dummy_config)

    def test_validate_config_multiple_layers(self, dummy_validation, tmp_path):
        """Test that a config with multiple layers is parsed."""
        validation_path = os.path.join(tmp_path, "conf_val.yaml")
        validation = {"layer1": dummy_validation}
        write_dict_to_yaml(validation, validation_path)
        dummy_config = {
            "config_validation": {"validate": True, "path": validation_path},
            "layer1": {"tester": 4},
        }
        msg = r"Config value for tester:tester .*4.* less than min .*5.*."
        with pytest.raises(ValueError, match=msg):
            validate_config(dummy_config)

    @pytest.mark.parametrize(
        "path, ext, error, msg",
        (
            [
                "tester/test.txt",
                ".yaml",
                TypeError,
                r"Expected file extension .*yaml.*Got.*txt.*",
            ],
            # test with no '.' in extension
            [
                "tester/test.bat",
                "toml",
                TypeError,
                r"Expected file extension .*toml.*Got.*bat.*",
            ],
        ),
    )
    def test_validate_config_path(self, tmp_path, path, ext, error, msg):
        """Tests for validating a path."""
        validation_path = os.path.join(tmp_path, "conf_val.yaml")
        validation = {
            "test_path": {
                "singular": True,
                "dtype": "path",
                "accept_nonetype": False,
                "filetype": ext,
            }
        }
        write_dict_to_yaml(validation, validation_path)
        config = {
            "config_validation": {"validate": True, "path": validation_path},
            "test_path": path,
        }
        with pytest.raises(error, match=msg):
            validate_config(config)

    @pytest.mark.parametrize(
        "val, error_type, msg",
        (
            [
                16.5,
                ValueError,
                (
                    r"Config value for tester:tester .*16.*5.* greater than "
                    r"max .*15.*"
                ),
            ],
            [
                4.7,
                ValueError,
                (r"Config value for tester:tester .*4.*7.* less than min " r".*5.*."),
            ],
            [
                "tester",
                TypeError,
                r".*tester:tester.* expected .*float.* Got .*str.*",
            ],
        ),
    )
    def test_validate_config_floats(
        self, dummy_validation, tmp_path, val, error_type, msg
    ):
        """Tests for validating an integer."""
        validation_path = os.path.join(tmp_path, "conf_val.yaml")
        dummy_validation["tester"]["dtype"] = "float"
        write_dict_to_yaml(dummy_validation, validation_path)
        dummy_config = {
            "config_validation": {"validate": True, "path": validation_path},
            "tester": val,
        }
        with pytest.raises(error_type, match=msg):
            validate_config(dummy_config)


class TestValidateFreezingConfigSettings(object):
    """Tests for validate_freezing_config_settings."""

    def test_validate_freezing_config_settings_no_errors(self):
        """Test validate_freezing_config_settings with no errors."""
        user_config = {
            "global": {
                "run_with_snapshot": False,
                "run_with_snapshot_and_freeze" : False,
                "run_with_frozen_data": True,
                "load_updated_snapshot_for_comparison": False,
                "run_updates_and_freeze": False,
            },
            "staging_paths": {
                "snapshot_path": "/path/to/frozen_snapshot",
                "updated_snapshot_path": "/path/to/updated_snapshot",
            },
            "freezing_paths": {
                "frozen_data_staged_path": "/path/to/frozen_data_staged",
                "freezing_additions_path": "/path/to/freezing_adds",
                "freezing_amendments_path": "/path/to/freezing_amends",
            },
        }
        validate_freezing_config_settings(user_config)

    def test_validate_freezing_config_settings_run_first_snapshot_without_snapshot_path(self):
        """Test validate_freezing_config_settings with run_with_snapshot True but no snapshot_path."""
        user_config = {
            "global": {
                "run_with_snapshot": True,
                "run_with_snapshot_and_freeze" : False,
                "run_with_frozen_data": False,
                "load_updated_snapshot_for_comparison": False,
                "run_updates_and_freeze": False,
            },
            "staging_paths": {
                "snapshot_path": None,
                "updated_snapshot_path": "/path/to/updated_snapshot",
            },
            "freezing_paths": {
                "frozen_data_staged_path": "/path/to/frozen_data_staged",
                "freezing_additions_path": "/path/to/freezing_adds",
                "freezing_amendments_path": "/path/to/freezing_amends",
            },
        }
        with pytest.raises(ValueError,
                           match="If running first snapshot of results,"
                           " a frozen snapshot path must be provided."):
            validate_freezing_config_settings(user_config)

    def test_validate_freezing_config_settings_run_with_frozen_data_without_frozen_data_staged_path(self):
        """Test validate_freezing_config_settings with run_with_frozen_data True but no frozen_data_staged_path."""
        user_config = {
            "global": {
                "run_with_snapshot": False,
                "run_with_snapshot_and_freeze" : False,
                "run_with_frozen_data": True,
                "load_updated_snapshot_for_comparison": False,
                "run_updates_and_freeze": False,
            },
            "staging_paths": {
                "snapshot_path": "/path/to/frozen_snapshot",
                "updated_snapshot_path": "/path/to/updated_snapshot",
            },
            "freezing_paths": {
                "frozen_data_staged_path": None,
                "freezing_additions_path": "/path/to/freezing_adds",
                "freezing_amendments_path": "/path/to/freezing_amends",
            },
        }
        with pytest.raises(ValueError,
                           match="If running frozen data, a frozen data staged path must be provided."):
            validate_freezing_config_settings(user_config)

    def test_validate_freezing_config_settings_load_updated_snapshot_without_updated_snapshot_path(self):
        """Test validate_freezing_config_settings with load_updated_snapshot_for_comparison True but no updated_snapshot_path."""
        user_config = {
            "global": {
                "run_with_snapshot": False,
                "run_with_snapshot_and_freeze" : False,
                "load_updated_snapshot_for_comparison": True,
                "run_updates_and_freeze": False,
                "run_with_frozen_data": False,
            },
            "staging_paths": {
                "snapshot_path": "/path/to/frozen_snapshot",
                "updated_snapshot_path": None,
            },
            "freezing_paths": {
                "frozen_data_staged_path": "/path/to/frozen_data_staged",
                "freezing_additions_path": "/path/to/freezing_adds",
                "freezing_amendments_path": "/path/to/freezing_amends",
            },
        }
        with pytest.raises(ValueError,
                           match="If loading an updated snapshot for comparison,"
                           " a secondary snapshot path and frozen data staged path must be provided."):
            validate_freezing_config_settings(user_config)

    def test_validate_freezing_config_settings_load_updated_snapshot_without_frozen_data_staged_path(self):
        """Test validate_freezing_config_settings with load_updated_snapshot_for_comparison True but no frozen_data_staged_path."""
        user_config = {
            "global": {
                "run_with_snapshot": False,
                "run_with_snapshot_and_freeze" : False,
                "load_updated_snapshot_for_comparison": True,
                "run_updates_and_freeze": False,
                "run_with_frozen_data": False,
            },
            "staging_paths": {
                "snapshot_path": "/path/to/frozen_snapshot",
                "updated_snapshot_path": "/path/to/updated_snapshot",
            },
            "freezing_paths": {
                "frozen_data_staged_path": None,
                "freezing_additions_path": "/path/to/freezing_adds",
                "freezing_amendments_path": "/path/to/freezing_amends",
            },
        }
        with pytest.raises(ValueError,
                           match="If loading an updated snapshot for comparison,"
                           " a secondary snapshot path and frozen data staged path must be provided."):
            validate_freezing_config_settings(user_config)

    def test_validate_freezing_config_settings_run_updates_and_freeze_without_frozen_data_staged_path(self):
        """Test validate_freezing_config_settings with run_updates_and_freeze True but no frozen_data_staged_path."""
        user_config = {
            "global": {
                "run_with_snapshot": False,
                "run_with_snapshot_and_freeze" : False,
                "load_updated_snapshot_for_comparison": False,
                "run_with_frozen_data": False,
                "run_updates_and_freeze": True,
            },
            "staging_paths": {
                "snapshot_path": "/path/to/frozen_snapshot",
                "updated_snapshot_path": "/path/to/updated_snapshot",
            },
            "freezing_paths": {
                "frozen_data_staged_path": None,
                "freezing_additions_path": "/path/to/freezing_adds",
                "freezing_amendments_path": "/path/to/freezing_amends",
            },
        }
        with pytest.raises(ValueError,
                           match="If running updates and freezing,"
                           " a frozen data staged path and a freezing adds or amends path must be provided."):
            validate_freezing_config_settings(user_config)

    def test_validate_freezing_config_settings_run_updates_and_freeze_without_freezing_adds_and_amends_path(self):
        """Test validate_freezing_config_settings with run_updates_and_freeze True but no freezing_adds_and_amends_path."""
        user_config = {
            "global": {
                "run_with_snapshot": False,
                "run_with_snapshot_and_freeze" : False,
                "run_with_frozen_data": False,
                "load_updated_snapshot_for_comparison": False,
                "run_updates_and_freeze": True,
            },
            "staging_paths": {
                "snapshot_path": "/path/to/frozen_snapshot",
                "updated_snapshot_path": "/path/to/updated_snapshot",
            },
            "freezing_paths": {
                "frozen_data_staged_path": "/path/to/frozen_data_staged",
                "freezing_additions_path": None,
                "freezing_amendments_path": None,
            },
        }
        with pytest.raises(ValueError,
                           match="If running updates and freezing,"
                           " a frozen data staged path and a freezing adds or amends path must be provided."):
            validate_freezing_config_settings(user_config)


class TestValidateConstructionConfigSettings(object):
    """Tests for validate_freezing_config_settings."""

    def test_validate_construction_config_settings_no_errors(self):
        """Test validate_construction_config_settings with no errors."""
        user_config = {
            "global": {
                "run_all_data_construction": True,
                "run_postcode_construction": True,
                "run_ni_construction": False,
            },
            "construction_paths": {
                "all_data_construction_file_path": "/path/to/all_data",
                "postcode_construction_file_path": "/path/to/postcodes",
                "construction_file_path_ni": None,
            },
        }
        validate_construction_config_settings(user_config)

    def test_validate_construction_config_settings_run_all_data_no_file(self):
        """Test validate_construction_config_settings with no all data file."""
        user_config = {
            "global": {
                "run_all_data_construction": True,
                "run_postcode_construction": False,
                "run_ni_construction": False,
            },
            "construction_paths": {
                "all_data_construction_file_path": None,
                "postcode_construction_file_path": "/path/to/postcodes",
                "construction_file_path_ni": "/path/to/ni",
            },
        }
        with pytest.raises(ValueError,
                           match="If running all data construction,"
                           " an all data construction file path must be provided."):
            validate_construction_config_settings(user_config)

    def test_validate_construction_config_settings_run_postcodes_no_file(self):
        """Test validate_construction_config_settings with no postcode file."""
        user_config = {
            "global": {
                "run_all_data_construction": False,
                "run_postcode_construction": True,
                "run_ni_construction": False,
            },
            "construction_paths": {
                "all_data_construction_file_path": "/path/to/all_data",
                "postcode_construction_file_path": None,
                "construction_file_path_ni": "/path/to/ni",
            },
        }
        with pytest.raises(ValueError,
                           match="If running postcode construction,"
                           " a postcode construction file path must be provided."):
            validate_construction_config_settings(user_config)

    def test_validate_construction_config_settings_run_ni_no_file(self):
        """Test validate_construction_config_settings with no ni file."""
        user_config = {
            "global": {
                "run_all_data_construction": False,
                "run_postcode_construction": False,
                "run_ni_construction": True,
            },
            "construction_paths": {
                "all_data_construction_file_path": "/path/to/all_data",
                "postcode_construction_file_path": "/path/to/postcodes",
                "construction_file_path_ni": None,
            },
        }
        with pytest.raises(ValueError,
                           match="If running NI construction,"
                           " a NI construction file path must be provided."):
            validate_construction_config_settings(user_config)


class TestValidateFreezingRunConfig(object):
    """Tests for validate_main_config."""

    def create_input_config(self, values: Union[tuple, list]) -> dict:
        """Create a dummy input config.

        Args:
            values (Union[tuple, list]): The values to apply to the config.

        Returns:
            dict: The dummy config dictionary.
        """
        config = {
            "global": {
                "run_with_snapshot": values[0],
                "run_with_snapshot_and_freeze": values[1],
                "load_updated_snapshot_for_comparison": values[2],
                "run_updates_and_freeze": values[3],
                "run_with_frozen_data": values[4]
            }
        }
        return config

    def test_validate_freezing_run_config(self):
        """General tests for validate_main_config."""
        config = self.create_input_config([True, False, False, False, False])
        results = validate_freezing_run_config(config=config)
        assert results == (True, False, False, False, False), (
            "Freezing config validation did not return the expected config values."
        )


    def test_validate_freezing_run_config_raises(self):
        """Failing tests for validate_main_config."""
        config = self.create_input_config(values=[True, True, True, True, False])
        msg = "Only one type of pipeline run is allowed.*"
        with pytest.raises(ValueError, match=msg):
            validate_freezing_run_config(config)
