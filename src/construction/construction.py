"""The main file for the construction module."""
import logging
import pandas as pd
from typing import Callable

from src.staging.validation import validate_data_with_schema

construction_logger = logging.getLogger(__name__)


def run_construction(
    snapshot_df: pd.DataFrame,
    config: dict,
    check_file_exists: Callable,
    read_csv: Callable,
    run_id: int,
) -> pd.DataFrame:
    """Run the construction module.

    The construction module reads the construction file provided by users and
    any non-null values are used to overwrite the snapshot dataframe. This is
    intended for users to do ad hoc updates of data without having to provide
    a new snapshot.

    Args:
        snapshot_df (pd.DataFrame): The staged and vaildated snapshot data.
        config (dict): The pipeline configuration
        check_file_exists (callable): Function to check if file exists. This will
            be the hdfs or network version depending on settings.
        read_csv (callable): Function to read a csv file. This will be the hdfs or
            network version depending on settings.
        run_id (int): The run id for this run.
    Returns:
        updated_snapshot_df (pd.DataFrame): As main_snapshot but with records
            amended and flags added to mark whether a record was constructed.
    """
    # Skip this module if not needed
    run_construction = config["global"]["run_construction"]
    if run_construction is False:
        construction_logger.info("Skipping Construction...")
        return snapshot_df

    # Check the construction file exists and has records, then read it
    construction_file_path = config["network_paths"]["construction_file_path"]
    construction_file_exists = check_file_exists(construction_file_path)
    if construction_file_exists:
        try:
            construction_df = read_csv(construction_file_path)
        except pd.errors.EmptyDataError:
            construction_logger.warning(
                f"Construction file {construction_file_path} is empty, skipping..."
            )
            return snapshot_df
    else:
        construction_logger.info(
            "Construction file not found, skipping construction..."
        )
        return snapshot_df

    # Make a copy of the snapshot
    updated_snapshot_df = snapshot_df.copy()

    # Validate construction file and drop columns without constructed values
    validate_data_with_schema(construction_df,
                              "./config/construction_schema.toml")
    construction_df = construction_df.dropna(axis="columns", how="all")

    # Add flags to indicate whether a row was constructed
    updated_snapshot_df["is_constructed"] = False
    construction_df["is_constructed"] = True

    # ! TEMPORARY Fix the year since the snapshot doesn't seem to have one?
    updated_snapshot_df["year"] = 2022

    # Update the values with the constructed ones
    construction_df.set_index(["reference", "year", "instance"], inplace=True)
    updated_snapshot_df.set_index(["reference", "year", "instance"], inplace=True)
    updated_snapshot_df.update(construction_df)
    updated_snapshot_df.reset_index(inplace=True)

    return updated_snapshot_df
