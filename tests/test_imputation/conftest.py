"""Shared configuration for imputation tests."""

# Third Party Imports
import pytest

@pytest.fixture(scope="module")
def imputation_config() -> dict:
    """A dummy imputation config for running imputation tetsts."""
    config = {
        "imputation": {
            "mor_threshold": 3,
            "trim_threshold": 10,
            "lower_trim_perc": 15,
            "upper_trim_perc": 15,
            "sf_expansion_threshold": 3,
            "lf_target_vars": ["211", "305", "emp_researcher", "emp_technician"]
        },
        "breakdowns": {
            "211": ["212", "214"],
            "305": ["302", "303", "304"],
            "emp_total": ["emp_researcher", "emp_technician"]
        }
    }
    return config
