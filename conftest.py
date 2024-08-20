"""conftest.py.

`pytest` configuration file. Currently used to flag tests for set-up only.
Reworked example from pytest docs:
https://docs.pytest.org/en/latest/example/simple.html.
"""

import pytest


def pytest_addoption(parser):
    """Adapt pytest cli args, and give more info when -h flag is used."""
    parser.addoption(
        "--runhdfs",
        action="store_true",
        default=False,
        help="Run HDFS tests.",
    )
    parser.addoption(
        "--runwip",
        action="store_true",
        default=False,
        help="Run WIP tests."
    )


def pytest_configure(config):
    """Add ini value line."""
    config.addinivalue_line("markers", "runhdfs: Run HDFS related tests.")
    config.addinivalue_line("markers", "runwip: Run work in progress tests.")



def pytest_collection_modifyitems(config, items):  # noqa:C901
    """Handle switching based on cli args."""

    # do full test suite when all flags are given
    if (
        config.getoption("--runhdfs") &
        config.getoption("--runwip")
    ):
        return

    # do not add marks when the markers flags are passed
    if not config.getoption("--runhdfs"):
        skip_hdfs = pytest.mark.skip(reason="Need --runhdfs option to run.")
        for item in items:
            if "runhdfs" in item.keywords:
                item.add_marker(skip_hdfs)
    
    if not config.getoption("--runwip"):
        skip_hdfs = pytest.mark.skip(reason="Need --runwip option to run.")
        for item in items:
            if "runwip" in item.keywords:
                item.add_marker(skip_hdfs)
