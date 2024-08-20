"""Shared configuration for imputation tests."""

# Third Party Imports
import pytest

@pytest.fixture(scope="module")
def imputation_config() -> dict:
    """A dummy imputation config for running imputation tetsts."""
    config = {
        "test": []
    }
    return config