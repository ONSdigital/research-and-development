"""Tests for freezing_utils.py."""

from datetime import datetime
from typing import Union

import pytest
import pandas as pd

from src.freezing.freezing_utils import _add_last_frozen_column, validate_main_config

class TestValidateMainConfig(object):
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
                "run_with_snapshot_until_freezing": values[0],
                "load_updated_snapshot_for_comparison": values[1],
                "run_updates_and_freeze": values[2],
                "run_frozen_data": values[3]
            }
        }
        return config

    def test_validate_main_config(self):
        """General tests for validate_main_config."""
        config = self.create_input_config([True, False, False,  False])
        results = validate_main_config(config=config)
        assert results == (True, False, False, False), (
            "Freezing config validation did not return the expected config values."
        )


    def test_validate_main_config_raises(self):
        """Failing tests for validate_main_config."""
        config = self.create_input_config(values=[True, True, True, False])
        msg = "Only one type of pipeline run is allowed.*"
        with pytest.raises(ValueError, match=msg):
            validate_main_config(config)
