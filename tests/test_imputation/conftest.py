"""Shared configuration for imputation tests."""

# Third Party Imports
import pytest

@pytest.fixture(scope="module")
def imputation_config() -> dict:
    """A dummy imputation config for running imputation tetsts."""
    config = {
        "imputation": {
            "mot_threshold": [],
            "lf_target_vars": [],
            "trim_threshold": [],
            "lower_trim_perc": [],
            "upper_trim_perc": [],
            "sf_expansion_threshold": []
                },
        "breakdowns": []
    }
    return config

