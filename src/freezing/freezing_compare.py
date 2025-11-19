from collections.abc import Callable
import logging
import os
import pandas as pd

from src.staging.staging_helpers import filter_pnp_data
from src.utils.breakdown_validation import get_equality_dicts
from src.utils.helpers import filename_amender, order_dataframe_for_output

FreezingLogger = logging.getLogger(__name__)


def get_amendments(
    frozen_csv: pd.DataFrame,
    updated_snapshot: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """Get amended records from updated snapshot.

    Get all records that are present in both the frozen_csv and the updated
    snapshot, and have matching keys.

    Args:
        frozen_csv (pd.DataFrame): The staged and validated frozen data.
        updated_snapshot (pd.DataFrame): The staged and validated updated
            snapshot data.
        config (dict): The pipeline configuration.

    Returns:
        amendments_df (pd.DataFrame): The records that have changed.
    """
    FreezingLogger.info(
        "Looking for records that have changed in the updated snapshot."
    )
    key_cols = ["reference", "period", "instance"]

    # Get the dictionary of equality lists
    equality_dict = get_equality_dicts(config, sublist="freezing")

    # Extract the values (lists) from the dictionary
    equality_lists = list(equality_dict.values())

    # Concatenate all lists into a single list
    concatenated_list = sum(equality_lists, [])

    # Remove duplicates and sort the list to create numeric_cols
    numeric_cols = sorted(set(concatenated_list))

    non_numeric_cols = ["200", "201", "601", "604", "status"]

    # numeric_cols_new = [f"{i}_updated" for i in numeric_cols]
    numeric_cols_diff = [f"{i}_diff" for i in numeric_cols]
    # non_numeric_cols_new = [f"{i}_updated" for i in non_numeric_cols]
    non_numeric_cols_diff = [f"{i}_diff" for i in non_numeric_cols]

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
            amendments_df[f"{each}_abs_diff"] = (
                amendments_df[f"{each}_updated"] - amendments_df[f"{each}_original"]
            ).abs()
            amendments_df.loc[
                amendments_df[f"{each}_abs_diff"] > 0.00001,
                f"is_{each}_abs_diff_nonzero_or_true",
            ] = True

        for each in non_numeric_cols:
            amendments_df[f"is_{each}_abs_diff_nonzero_or_true"] = (
                amendments_df[f"{each}_updated"] != amendments_df[f"{each}_original"]
            )
            amendments_df.loc[
                amendments_df[f"is_{each}_abs_diff_nonzero_or_true"], f"{each}_diff"
            ] = amendments_df[f"{each}_updated"]

        # Take a slice of the df which is just the cols ending with
        # _diff_nonzero_or_true.
        # Do a column-wise any() on this slice, which returns a series where the
        # value is True if any of the *_diff_nonzero_or_true cols in that row were True
        # Add that series as a column to the original df
        # Remove any rows from the df where is_any_diff_nonzero_or_true is False
        amendments_df["is_any_diff_nonzero_or_true"] = amendments_df[
            amendments_df.columns[
                amendments_df.columns.str.endswith("_abs_diff_nonzero_or_true")
            ]
        ].any(axis="columns")
        amendments_df = amendments_df.loc[amendments_df["is_any_diff_nonzero_or_true"]]

        # Select the row from the updated snapshot, and differences in key variables
        select_cols = [
            "reference",
            "period",
            "instance",
            *[col for col in amendments_df.columns if col.endswith("_updated")],
            *numeric_cols_diff,
            *non_numeric_cols_diff,
        ]
        amendments_df = amendments_df[select_cols]
        amendments_df.columns = [
            col.replace("_updated", "") for col in amendments_df.columns
        ]

        # Add markers
        amendments_df["change_type"] = "amendment"
        amendments_df["accept_changes"] = False
        amendments_df["frozen_data_file"] = config["freezing_paths"][
            "frozen_data_staged_path"
        ].rsplit("/", 1)[-1]

        return amendments_df
    else:
        # If there are no amendments, return an empty dataframe
        FreezingLogger.info("No amendments found.")
        return pd.DataFrame()


def process_additions_deletions(
    merge_df: pd.DataFrame, config: dict, add_or_del: str
) -> pd.DataFrame:
    """Process additions and deletions.

    This function processes the additions and deletions dataframes by removing
    the columns that are not needed for the final output.

    Args:
        df (pd.DataFrame): The merged frozen and updated snapshot dataframes
        config (dict): The pipeline configuration
        add_or_del (str): Whether to process additions or deletions
            ("additions" or "deletions")

    Returns:
        pd.DataFrame: The processed dataframe.
    """
    if add_or_del == "additions":
        merge_type = "right_only"
        suffix = "_old"
    elif add_or_del == "deletions":
        merge_type = "left_only"
        suffix = "_new"
    else:
        raise ValueError("add_or_del must be either 'additions' or 'deletions'")

    df = merge_df[merge_df._merge == merge_type].copy()
    df = df.drop(columns=["_merge"])
    # Remove the old columns that are not needed for the final output
    df = df[df.columns[~df.columns.str.endswith(suffix)]]
    # Rename the remaining columns to remove any further suffixes
    df.columns = df.columns.str.replace("_old", "").str.replace("_new", "")

    # Filter for either BERD or PNP data
    df = filter_pnp_data(df, config)

    # Add a column for indicating if the changes are accepted or not
    # and a column for the frozen data file name
    frozen_data_file = config["freezing_paths"]["frozen_data_staged_path"]
    if len(df) > 0:
        df["change_type"] = add_or_del.rstrip("s")
        df["accept_changes"] = False
        df["frozen_data_file"] = frozen_data_file.rsplit("/", 1)[-1]
        return df
    else:
        FreezingLogger.info("No additions found.")
        return pd.DataFrame()


def get_additions_deletions(
    frozen_csv: pd.DataFrame,
    updated_snapshot: pd.DataFrame,
    config: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Get added records from the updated snapshot.

    Get all records that are present in the updated snapshot but not the main

    Args:
        frozen_csv (pd.DataFrame): The staged and validated frozen data.
        updated_snapshot (pd.DataFrame): The staged and validated updated snapshot data.
        config (dict): The pipeline configuration.

    Returns:
        additions_df (pd.DataFrame): The new records identified in
            the updated snapshot data.
    """
    FreezingLogger.info("Looking for new or deleted records in the updated snapshot.")
    key_cols = ["reference", "period", "instance"]

    outer_join = pd.merge(
        frozen_csv,
        updated_snapshot,
        on=key_cols,
        how="outer",
        suffixes=("_old", "_new"),
        indicator=True,
    )
    additions_df = process_additions_deletions(outer_join, config, "additions")
    deletions_df = process_additions_deletions(outer_join, config, "deletions")

    return additions_df, deletions_df


def bring_together_split_cases(
    amendments_df: pd.DataFrame,
    additions_df: pd.DataFrame,
    deletions_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Checks for references in both the additions and amendments.
    If a reference is found in both: move all the relevant rows into
    amendments and remove from additions.

    Args:
        amendments_df (pd.DataFrame): The records that have changed.
        additions_df (pd.DataFrame): The records that have been added.
        deletions_df (pd.DataFrame): The records that have been deleted.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: The amendments,
            additions and deletions dataframes.
    """
    if not additions_df.empty:
        addition_split_cases = additions_df[
            additions_df["reference"].isin(amendments_df["reference"])
        ]
        if not addition_split_cases.empty:
            # remove the split cases from the additions df
            additions_df = additions_df[
                ~additions_df.reference.isin(addition_split_cases.reference)
            ]
            # concatenate the split cases to the amendments dataframe
            amendments_df = pd.concat(
                [amendments_df, addition_split_cases], ignore_index=True
            )
    if not deletions_df.empty:
        deletion_split_cases = deletions_df[
            deletions_df["reference"].isin(amendments_df["reference"])
        ]
        if not deletion_split_cases.empty:
            # remove the split cases from the deletions df
            deletions_df = deletions_df[
                ~deletions_df.reference.isin(deletion_split_cases.reference)
            ]
            # concatenate the split cases to the amendments dataframe
            amendments_df = pd.concat(
                [amendments_df, deletion_split_cases], ignore_index=True
            )
    return amendments_df, additions_df, deletions_df


def output_freezing_files(
    amendments_df: pd.DataFrame,
    additions_df: pd.DataFrame,
    deletions_df: pd.DataFrame,
    config: dict,
    write_csv: Callable,
) -> bool:
    """Save CSVs of amendments, additions and deletions for user approval.

    Args:
        amendments_df (pd.DataFrame): The records that have changed.
        additions_df (pd.DataFrame): The records that have been added.
        deletions_df (pd.DataFrame): The records that have been deleted.
        config (dict): The pipeline configuration
        write_csv (callable): Function to write to a csv file. This will be the
            hdfs or network version depending on settings.

    Returns:
        bool: True if the files were written successfully.
    """

    freezing_changes_to_review_path = config["freezing_paths"][
        "freezing_changes_to_review_path"
    ]
    FreezingLogger.info("Outputting changes to review file(s).")

    # Check if the dataframes are empty before writing
    if not amendments_df.empty:
        # Order the dataframe for output by reference and instance
        amendments_df = order_dataframe_for_output(amendments_df)
        # Create the filename using the filename_amender function and write csv file
        filename = filename_amender("freezing_amendments_to_review", config)
        write_csv(
            os.path.join(freezing_changes_to_review_path, filename), amendments_df
        )

    if not additions_df.empty:
        additions_df = order_dataframe_for_output(additions_df)
        filename = filename_amender("freezing_additions_to_review", config)
        write_csv(os.path.join(freezing_changes_to_review_path, filename), additions_df)

    if not deletions_df.empty:
        deletions_df = order_dataframe_for_output(deletions_df)
        filename = filename_amender("freezing_deletions_to_review", config)
        write_csv(os.path.join(freezing_changes_to_review_path, filename), deletions_df)

    # If all three dataframes are empty, log that there are no changes to review
    if amendments_df.empty and additions_df.empty and deletions_df.empty:
        FreezingLogger.info("No changes to review found.")
        return False
    else:
        FreezingLogger.info("File(s) to review output sucessfully.")
        return True


def run_comparison(
    frozen_data_for_comparison: pd.DataFrame,
    updated_snapshot: pd.DataFrame,
    config: dict,
    write_csv: Callable,
) -> None:
    """Main function to run comparison of frozen data and updated snapshot.
    Function outputs two csv files, one for additions and one for amendments.

    Args:
        frozen_data_for_comparison (pd.DataFrame): The staged and validated frozen data.
        updated_snapshot (pd.DataFrame): The staged and validated updated snapshot data.
        config (dict): The pipeline configuration
        write_csv (callable): Function to write to a csv file.

    Returns:
        None
    """
    additions_df, deletions_df = get_additions_deletions(
        frozen_data_for_comparison, updated_snapshot, config
    )
    amendments_df = get_amendments(frozen_data_for_comparison, updated_snapshot, config)
    if not amendments_df.empty:
        amendments_df, additions_df, deletions_df = bring_together_split_cases(
            amendments_df, additions_df, deletions_df
        )
    output_freezing_files(amendments_df, additions_df, deletions_df, config, write_csv)
