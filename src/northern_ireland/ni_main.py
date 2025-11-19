"""The main file for the Northern Ireland  module."""

import logging
import pandas as pd
import warnings
from collections.abc import Callable
from src.northern_ireland.ni_staging import run_ni_staging
from src.construction.construction_main import run_construction
from src.northern_ireland.ni_headcount_fte import run_ni_headcount_fte

NIModuleLogger = logging.getLogger(__name__)


def run_ni(
    config: dict,
    check_file_exists: Callable,
    read_csv: Callable,
    write_csv: Callable,
) -> pd.DataFrame:
    """Stage NI data and apply construction to it.

    Args:
        config (dict): The pipeline configuration
        check_file_exists (Callable): Function to check if file exists
            This will be the s3, hdfs or network version depending on settings.
        read_csv (Callable): Function to read a csv file.
            This will be the s3, hdfs or network version depending on settings.
        write_csv (Callable): Function to write to a csv file.
            This will be the s3, hdfs or network version depending on settings.
    Returns:
        DataFrame: A dataframe containing staged and validated Northern Ireland
            data with any constructed records amended.
    """
    NIModuleLogger.info("Starting Northern Ireland data staging and validation...")

    # Check survey type and terminate if it is "PNP"
    if config["survey"]["survey_type"] == "PNP":
        warnings.warn("Survey type 'PNP' and Northern Ireland are mutually exclusive.")
        raise SystemExit(
            "survey_type='PNP' not compaiable with load_ni_data=True. "
            "Either 1. Change the survey type to BERD, or 2. Change "
            "load_ni_data to False."
        )

    ni_full_responses_df = run_ni_staging(
        config,
        check_file_exists,
        read_csv,
        write_csv,
    )

    if config["global"]["run_ni_construction"]:
        NIModuleLogger.info("Running NI construction")
        ni_full_responses_df = run_construction(
            ni_full_responses_df,
            config,
            check_file_exists,
            read_csv,
            is_northern_ireland=True,
        )
    else:
        NIModuleLogger.info("NI construction is not enabled")

    NIModuleLogger.info("Running NI headcount and fte")
    full_ni_df = run_ni_headcount_fte(ni_full_responses_df)

    return full_ni_df
