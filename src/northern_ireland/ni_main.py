"""The main file for the Northern Ireland  module."""

import logging
import pandas as pd
from typing import Callable
from src.northern_ireland.ni_staging import run_ni_staging
from src.construction.construction import run_construction
from src.northern_ireland.ni_headcount_fte import run_ni_headcount_fte

NIModuleLogger = logging.getLogger(__name__)


def run_ni(
    config: dict,
    check_file_exists: Callable,
    read_csv: Callable,
    write_csv: Callable,
    run_id: int,
) -> pd.DataFrame:
    """Stage NI data and apply construction to it.

    Args:
        config (dict): The pipeline configuration
        check_file_exists (Callable): Function to check if file exists
            This will be the hdfs or network version depending on settings.
        read_csv (Callable): Function to read a csv file.
            This will be the hdfs or network version depending on settings.
        write_csv (Callable): Function to write to a csv file.
            This will be the hdfs or network version depending on settings.
        run_id (int): The run id for this run.
    Returns:
        DataFrame: A dataframe containing staged and validated Northern Ireland
            data with any constructed records amended.
    """
    NIModuleLogger.info("Starting Northern Ireland data staging and validation...")
    ni_full_responses_df = run_ni_staging(
        config,
        check_file_exists,
        read_csv,
        write_csv,
        run_id,
    )

    NIModuleLogger.info("Running NI construction")
    ni_df = run_construction(
        ni_full_responses_df,
        config,
        check_file_exists,
        read_csv,
        is_northern_ireland=True,
    )

    NIModuleLogger.info("Running NI headcount and fte")
    full_ni_df = run_ni_headcount_fte(ni_df)

    return full_ni_df
