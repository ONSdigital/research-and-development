"""DEPRECATED MODULE DO NOT USE."""

import os
import logging
import pandas as pd
from typing import Callable
from datetime import datetime


construction_logger = logging.getLogger(__name__)


def run_construction(
    main_snapshot: pd.DataFrame,
    secondary_snapshot: pd.DataFrame,
    config: dict,
    check_file_exists: Callable,
    write_csv: Callable,
    read_csv: Callable,
    run_id: int,
) -> pd.DataFrame:
    """Run the construction module.

    Args:
        main_snapshot (pd.DataFrame): The staged and vaildated snapshot data.
        secondary_snapshot (pd.DataFrame): The staged and validated updated
            snapshot data.
        config (dict): The pipeline configuration
        check_file_exists (callable): Function to check if file exists. This will
            be the hdfs or network version depending on settings.
        write_csv (callable): Function to write to a csv file. This will be the
            hdfs or network version depending on settings.
        read_csv (callable): Function to read a csv file. This will be the hdfs or
            network version depending on settings.
        run_id (int): The run id for this run.
    Returns:
        constructed_df (pd.DataFrame): As main_snapshot but with records amended
            and added from the construction files.
    """
    # Skip this module if the secondary snapshot isn't loaded
    load_updated_snapshot = config["global"]["load_updated_snapshot"]
    load_manual_construction = config["global"]["load_manual_construction"]
    if load_manual_construction is False:
        construction_logger.info("Skipping Construction...")
        return main_snapshot

    # ! For now, we add the year column since neither file has it
    main_snapshot["year"] = 2022
    if load_updated_snapshot is True:
        secondary_snapshot["year"] = 2022

        # Use the secondary snapshot to generate construction files for the next run
        additions_df = get_additions(main_snapshot, secondary_snapshot)
        amendments_df = get_amendments(main_snapshot, secondary_snapshot)
        output_construction_files(
            amendments_df, additions_df, config, write_csv, run_id
        )

    # Read the construction files from the last run and apply them
    constructed_df = apply_construction(
        main_snapshot, config, check_file_exists, read_csv, write_csv, run_id
    )
    constructed_df.reset_index(drop=True, inplace=True)

    return constructed_df


def get_amendments(main_snapshot, secondary_snapshot):
    """Get amended records from secondary snapshot.

    Get all records that are present in both the main snapshot and the updated
    snapshot, and have matching keys.
    """
    key_cols = ["reference", "year", "instance"]
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
        main_snapshot,
        secondary_snapshot,
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

        # ? I think this is the way to do it:
        # ?     Take a slice of the df which is just the cols ending with _diff_nonzero
        # ?     Do a column-wise any() on this slice, which returns a series where the
        #       value is True if any of the *_diff_nonzero cols in that row were True
        # ?     Add that series as a column to the original df
        # ?     Remove any rows from the df where is_any_diff_nonzero is False
        # ! Can't test this without a real secondary snapshot file
        amendments_df["is_any_diff_nonzero"] = amendments_df[
            amendments_df.columns[amendments_df.columns.str.endswith("_diff_nonzero")]
        ].any(axis="columns")
        amendments_df = amendments_df.loc[amendments_df["is_any_diff_nonzero"]]

        # Select only the keys, updated value, difference, and postcode
        # TODO Would be easier for users if the numberic cols alternated
        select_cols = [
            "reference",
            "year",
            "instance",
            *numeric_cols_new,
            *numeric_cols_diff,
            "postcodes_harmonised",
        ]
        amendments_df = amendments_df[select_cols]

        # Add markers
        amendments_df["is_constructed"] = True
        amendments_df["accept_changes"] = False

        return amendments_df
    else:
        construction_logger.info("No amendments found.")
        return None


def get_additions(main_snapshot, secondary_snapshot):
    """Get added records from secondary snapshot.

    Get all records that are present in the updated snapshot but not the main snapshot
    """
    key_cols = ["reference", "year", "instance"]

    # To do a right anti-join, we need to do a full outer join first, then
    # take only rows that were marked as right-only by the indicator. After
    # that, there will be copies of every column in both, but for the
    # right-only rows the columns from the left df will be null, so they're
    # all dropped afterwards.
    outer_join = pd.merge(
        main_snapshot,
        secondary_snapshot,
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
        additions_df["is_constructed"] = True
        additions_df["accept_changes"] = False
        return additions_df
    else:
        construction_logger.info("No additions found.")
        return None


def output_construction_files(amendments_df, additions_df, config, write_csv, run_id):
    """Save CSVs of amendments and additions for user approval."""
    # Prepare output paths
    network_or_hdfs = config["global"]["network_or_hdfs"]
    paths = config[f"{network_or_hdfs}_paths"]
    tdate = datetime.now().strftime("%y-%m-%d")
    survey_year = config["years"]["current_year"]
    construction_folder = paths["construction_path"]
    amendments_filename = os.path.join(
        construction_folder,
        "auto_construction",
        f"{survey_year}_construction_amendments_{tdate}_v{run_id}.csv",
    )
    additions_filename = os.path.join(
        construction_folder,
        "auto_construction",
        f"{survey_year}_construction_additions_{tdate}_v{run_id}.csv",
    )

    # Check if the dataframes are empty before writing
    if amendments_df is not None:
        write_csv(f"{amendments_filename}", amendments_df)
    if additions_df is not None:
        write_csv(f"{additions_filename}", additions_df)

    return True


def apply_construction(main_df, config, check_file_exists, read_csv, write_csv, run_id):
    """Read user-edited construction files and apply them to the main snapshot."""
    # Prepare filepaths to read from
    network_or_hdfs = config["global"]["network_or_hdfs"]
    paths = config[f"{network_or_hdfs}_paths"]
    amendments_filepath = paths["construction_amend_path"]
    additions_filepath = paths["construction_add_path"]

    # Check if the construction files exist
    amendments_exist = check_file_exists(amendments_filepath)
    additions_exist = check_file_exists(additions_filepath)

    # If each file exists, read it and call the function to apply them
    if (not amendments_exist) and (not additions_exist):
        construction_logger.info("No amendments or additions to apply, skipping...")
        return main_df

    if amendments_exist:
        try:
            amendments_df = read_csv(amendments_filepath)
            constructed_df = apply_amendments(main_df, amendments_df)
        except pd.errors.EmptyDataError:
            construction_logger.warning(
                f"Amendments file {amendments_filepath} is empty, skipping..."
            )

    if additions_exist:
        try:
            additions_df = read_csv(additions_filepath)
            additions_df["instance"] = additions_df["instance"].astype("Int64")
            constructed_df = apply_additions(main_df, additions_df)
        except pd.errors.EmptyDataError:
            construction_logger.warning(
                f"Additions file {additions_filepath} is empty, skipping..."
            )

    # Save the constructed dataframe as a CSV
    tdate = datetime.now().strftime("%y-%m-%d")
    survey_year = config["years"]["current_year"]
    construction_output_filepath = os.path.join(
        paths["root"], "construction", f"{survey_year}_constructed_snapshot_{tdate}_v{run_id}.csv"
    )
    write_csv(construction_output_filepath, constructed_df)
    return constructed_df


def apply_amendments(main_df, amendments_df):
    """Apply amendments to the main snapshot."""
    key_cols = ["reference", "year", "instance"]
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

    accepted_amendments_df = amendments_df.drop(
        amendments_df[~amendments_df.accept_changes].index
    )

    if accepted_amendments_df.shape[0] == 0:
        construction_logger.info(
            "Amendments file contained no records marked for inclusion"
        )
        return main_df

    # Drop the diff columns
    accepted_amendments_df = accepted_amendments_df.drop(
        columns=[col for col in accepted_amendments_df.columns if col.endswith("_diff")]
    )

    # Join the amendments onto the main snapshot
    amended_df = pd.merge(main_df, accepted_amendments_df, how="left", on=key_cols)

    # Drop the old numeric cols and rename the amended cols
    amended_df = amended_df.drop(columns=numeric_cols)
    cols_to_rename = dict(zip(numeric_cols_new, numeric_cols))
    amended_df = amended_df.rename(columns=cols_to_rename, errors="raise")

    construction_logger.info(
        f"{accepted_amendments_df.shape[0]} records amended during construction"
    )

    return amended_df


def apply_additions(main_df, additions_df):
    """Apply additions to the main snapshot."""
    # Drop records where accept_changes is False and if any remain, add them to main df
    accepted_additions_df = additions_df.drop(
        additions_df[~additions_df["accept_changes"]].index
    )
    if accepted_additions_df.shape[0] > 0:
        added_df = pd.concat([main_df, accepted_additions_df], ignore_index=True)
        construction_logger.info(
            f"{accepted_additions_df.shape[0]} records added during construction"
        )
    else:
        construction_logger.info(
            "Additions file contained no records marked for inclusion"
        )
        return main_df

    return added_df
