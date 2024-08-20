"""Tests for freezing_utils.py."""

from datetime import datetime

import pandas as pd

from src.freezing.freezing_utils import _add_last_frozen_column

class TestAddLastFrozenColumn(object):
    """Tests for _add_last_frozen_column."""

    def create_expected_last_frozen(self, run_id: int) -> str:
        """Create an expected output from _add_last_frozen_column."""
        today = datetime.today().strftime("%y-%m-%d")
        expected_frozen = f"{today}_v{str(run_id)}"
        return expected_frozen

    def test__add_last_frozen_column(self):
        """General tests for _add_last_frozen_column."""
        # create dummy df with one row
        dummy_df = pd.DataFrame(
            {"test": [0]}
        )
        # add last_frozen_column
        last_frozen_df = _add_last_frozen_column(dummy_df, 7000)
        exp_last_frozen = self.create_expected_last_frozen(7000)
        assert len(last_frozen_df.last_frozen.unique()) == 1, (
            "_add_last_frozen_column has added multiple different values."
        )
        assert last_frozen_df.last_frozen.unique()[0] == exp_last_frozen, (
            "_add_last_frozen_column not behaving as expected."
        )