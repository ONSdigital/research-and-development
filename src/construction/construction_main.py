"""The main file for the construction module."""
import logging
from typing import Callable

import pandas as pd
import numpy as np

from src.construction.construction_read_validate import (
    read_validate_all_construction_files,
    read_validate_postcode_construction_file,
)
from src.construction.all_data_construction import all_data_construction
from src.construction.postcode_construction import postcode_data_construction


construction_logger = logging.getLogger(__name__)


def run_construction(  # noqa: C901
    snapshot_df: pd.DataFrame,
    config: dict,
    check_file_exists: Callable,
    read_csv: Callable,
    is_run_all_data_construction: bool = False,
    is_run_postcode_construction: bool = False,
    is_northern_ireland: bool = False,
) -> pd.DataFrame:
    """Run the construction module.

    The construction module reads the construction file provided by users and
    any non-null values are used to overwrite the snapshot dataframe. This is
    intended for users to do ad hoc updates of data without having to provide
    a new snapshot.

    Args:
        snapshot_df (pd.DataFrame): The staged and vaildated snapshot data.
        config (dict): The pipeline configuration
        check_file_exists (callable): Function to check if file exists. This
            will be the hdfs or network version depending on settings.
        read_csv (callable): Function to read a csv file. This will be the hdfs
            or network version depending on settings.
        is_run_all_data_construction (bool): A logical parameter to perform all
            construction. If this flag is True, and there is a construction
            file, all construction steps will be done before the imputation.
        is_run_postcode_construction (bool): A logical parameter to perform
            postcode construction. If this flag is True, and there is a postcode
            construction file, the postcode constructions will be done after the
            imputation.
        is_northern_ireland (bool): If true, do construction on Northern Ireland
            data instead of England, Wales and Scotland data.
    Returns:
        updated_snapshot_df (pd.DataFrame): As main_snapshot but with records
            amended and flags added to mark whether a record was constructed.
    """
    if is_northern_ireland:
        run_construction = config["global"]["run_ni_construction"]
        run_postcode_construction = False
        df = read_validate_all_construction_files(
            config,
            check_file_exists,
            read_csv,
            construction_logger,
            is_northern_ireland=True,
        )
        updated_snapshot_df = all_data_construction(
            df, snapshot_df, construction_logger, is_northern_ireland=True
        )

    elif is_run_all_data_construction:
        run_construction = config["global"]["run_all_data_construction"]
        run_postcode_construction = False
        df = read_validate_all_construction_files(
            config, check_file_exists, read_csv, construction_logger
        )
        updated_snapshot_df = all_data_construction(
            df, snapshot_df, construction_logger
        )

    elif is_run_postcode_construction:
        run_postcode_construction = config["global"]["run_postcode_construction"]
        run_construction = False
        df = read_validate_postcode_construction_file(
            config, check_file_exists, read_csv, construction_logger
        )
        updated_snapshot_df = postcode_data_construction(
            df, snapshot_df, construction_logger
        )

    # Skip this module if not needed
    if not run_construction and not run_postcode_construction:
        construction_logger.info("Skipping Construction...")
        return snapshot_df

    return updated_snapshot_df
