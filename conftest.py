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


def pytest_configure(config):
    """Add ini value line."""
    config.addinivalue_line("markers", "runhdfs: Run HDFS related tests.")


def pytest_collection_modifyitems(config, items):  # noqa:C901
    """Handle switching based on cli args."""

    # do full test suite when all flags are given
    if config.getoption("--runhdfs"):
        return

    # do not add runhdfs marks when the --runhdfs flag is passed
    if not config.getoption("--runhdfs"):
        skip_hdfs = pytest.mark.skip(reason="Need --runhdfs option to run.")
        for item in items:
            if "runhdfs" in item.keywords:
                item.add_marker(skip_hdfs)
