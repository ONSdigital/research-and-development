import logging
from datetime import datetime
import os
import pandas as pd
from typing import Callable

FreezingLogger = logging.getLogger(__name__)

def get_amendments(
    frozen_csv: pd.DataFrame,
    updated_snapshot: pd.DataFrame,
    ) -> pd.DataFrame:
    """Get amended records from updated snapshot.

    Get all records that are present in both the frozen_csv and the updated
    snapshot, and have matching keys.

    Args:
        frozen_csv (pd.DataFrame): The staged and validated frozen data.
        updated_snapshot (pd.DataFrame): The staged and validated updated
            snapshot data.

    Returns:
        amendments_df (pd.DataFrame): The records that have changed.
    """
    FreezingLogger.info("Looking for records that have changed in the updated snapshot.")
    key_cols = ["reference", "period", "instance"]
    numeric_cols = [
        "219",
        "220",
        "242",
        "243",
        "244",
        "245",
        "246",
        "247",
        "248",
        "249",
        "250",
    ]
    numeric_cols_new = [f"{i}_updated" for i in numeric_cols]
    numeric_cols_diff = [f"{i}_diff" for i in numeric_cols]

    # Inner join on keys to select only records present in both snapshots
    amendments_df = pd.merge(
        frozen_csv,
        updated_snapshot,
        on=key_cols,
        how="inner",
        suffixes=("_original", "_updated"),
    )

    # If there are any records to amend, calculate differences
    if amendments_df.shape[0] > 0:

        for each in numeric_cols:
            amendments_df[f"{each}_diff"] = (
                amendments_df[f"{each}_updated"] - amendments_df[f"{each}_original"]
            )
            amendments_df.loc[
                amendments_df[f"{each}_diff"] > 0.00001, f"is_{each}_diff_nonzero"
            ] = True

        # Take a slice of the df which is just the cols ending with _diff_nonzero
        # Do a column-wise any() on this slice, which returns a series where the
        # value is True if any of the *_diff_nonzero cols in that row were True
        # Add that series as a column to the original df
        # Remove any rows from the df where is_any_diff_nonzero is False
        amendments_df["is_any_diff_nonzero"] = amendments_df[
            amendments_df.columns[amendments_df.columns.str.endswith("_diff_nonzero")]
        ].any(axis="columns")
        amendments_df = amendments_df.loc[amendments_df["is_any_diff_nonzero"]]

        # Select the row from the updated snapshot, and differences in key variables
        select_cols = [
            "reference",
            "period",
            "instance",
            *[col for col in amendments_df.columns if col.endswith("_updated")],
            *numeric_cols_diff,
        ]
        amendments_df = amendments_df[select_cols]
        amendments_df.columns = [col.replace("_updated", "") for col in amendments_df.columns]

        # Add markers
        amendments_df["accept_changes"] = False

        return amendments_df
    else:
        freezing_logger.info("No amendments found.")
        return None


def get_additions(
    frozen_csv: pd.DataFrame,
    updated_snapshot: pd.DataFrame,
    ) -> pd.DataFrame:
    """Get added records from the updated snapshot.

    Get all records that are present in the updated snapshot but not the main

    Args:
        frozen_csv (pd.DataFrame): The staged and validated frozen data.
        updated_snapshot (pd.DataFrame): The staged and validated updated snapshot data.

    Returns:
        additions_df (pd.DataFrame): The new records identified in the updated snapshot data.
    """
    FreezingLogger.info("Looking for new records in the updated snapshot.")
    key_cols = ["reference", "period", "instance"]

    # To do a right anti-join, we need to do a full outer join first, then
    # take only rows that were marked as right-only by the indicator. After
    # that, there will be copies of every column in both, but for the
    # right-only rows the columns from the left df will be null, so they're
    # all dropped afterwards.
    outer_join = pd.merge(
        frozen_csv,
        updated_snapshot,
        on=key_cols,
        how="outer",
        suffixes=("_old", ""),
        indicator=True,
    )
    additions_df = outer_join[(outer_join._merge == "right_only")].drop(
        "_merge", axis=1
    )
    additions_df = additions_df[
        additions_df.columns[~additions_df.columns.str.endswith("_old")]
    ]

    if additions_df.shape[0] > 0:
        additions_df["accept_changes"] = False
        return additions_df
    else:
        freezing_logger.info("No additions found.")
        return None


def output_freezing_files(
    amendments_df: pd.DataFrame,
    additions_df: pd.DataFrame,
    config: dict,
    write_csv: Callable,
    run_id: int,
    ) -> bool:
    """Save CSVs of amendments and additions for user approval.

    Args:
        amendments_df (pd.DataFrame): The records that have changed.
        additions_df (pd.DataFrame): The records that have been added.
        config (dict): The pipeline configuration
        write_csv (callable): Function to write to a csv file. This will be the
            hdfs or network version depending on settings.
        run_id (int): The run id for this run.

    Returns:
        bool: True if the files were written successfully.
    """

    freezing_changes_to_review_path = config["freezing_paths"]["freezing_changes_to_review_path"]
    FreezingLogger.info("Outputting changes to review file(s).")
    tdate = datetime.now().strftime("%y-%m-%d")
    survey_year = config["years"]["survey_year"]

    # Check if the dataframes are empty before writing
    if amendments_df is not None:
        filename = (
            f"{survey_year}_freezing_amendments_to_review_{tdate}_v{run_id}.csv"
        )
        write_csv(os.path.join(freezing_changes_to_review_path, filename), amendments_df)

    if additions_df is not None:
        filename = (
            f"{survey_year}_freezing_additions_to_review_{tdate}_v{run_id}.csv"
        )
        write_csv(os.path.join(freezing_changes_to_review_path, filename), additions_df)

    if amendments_df is None and additions_df is None:
        FreezingLogger.info("No changes to review found.")
        return False
    else:
        FreezingLogger.info("File(s) to review output sucessfully.")
        return True
