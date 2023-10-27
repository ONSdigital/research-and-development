"""TODO."""

import logging
import pandas as pd

from src.utils.local_file_mods import read_local_csv, write_local_csv, local_file_exists
from src.utils.hdfs_mods import read_hdfs_csv, write_hdfs_csv, hdfs_isfile


construction_logger = logging.getLogger(__name__)


def run_construction(main_snapshot, secondary_snapshot, config, write_csv, run_id):
    """Define placeholder construction main function."""
    # ! For now, we add the year column since neither file has it
    main_snapshot["year"] = 2022
    secondary_snapshot["year"] = 2022

    print ("=== INSIDE CONSTRUCTION MODULE ===")

    additions_df = get_additions(main_snapshot, secondary_snapshot)
    additions_df.info(verbose=True)

    raise BaseException("=== TEST END ===")
    pass


def get_amendments(main_snapshot, secondary_snapshot):
    """Create a dataframe of rows in the main snapshot that are updated in the updated snapshot."""
    key_cols = ["reference", "year", "instance"]
    numeric_cols = ["219", "220", "242", "243", "244", "245", "246", "247", "248", "249", "250"]

    # Inner join on the keys to select only records present in both
    amendments_df = main_snapshot.join(secondary_snapshot,
                                       on = key_cols,
                                       how = "inner",
                                       lsuffix = "_original",
                                       rsuffix = "_updated")

    # Filter for where abs(old_value - new_value) > 0.0001 to ignore floating point noise
    # ? How should this final df be strucutred? One row per row, or melted so there's one row per numeric col changed?
    for each in numeric_cols:
        amendments_df[f"{each}_diff"] = amendments_df[f"{each}_original"] - amendments_df[f"{each}_updated"]
        amendments_df = amendments_df.loc[abs(amendments_df[f"{each}_diff"]) > 0.0001] # ! This won't work, since then we're filtering each loop?

    # Select only the keys, updated and diffs of numeric columns, and postcode
    numeric_cols_new = [f"{i}_updated" for i in numeric_cols]
    numeric_cols_diff = [f"{i}_diff" for i in numeric_cols]
    select_cols = ["reference", "year", "instance", *numeric_cols_new, *numeric_cols_diff, "harmonised_postcode"]
    amendments_df = amendments_df[select_cols]

    # Add markers
    amendments_df["is_constructed"] = True
    amendments_df["accept_changes"] = False

    return amendments_df


def get_additions(main_snapshot, secondary_snapshot):
    """Create a dataframe of rows present in the updated snapshot that are not in the main snapshot."""
    key_cols = ["reference", "year", "instance"]

    # To do a right anti-join, we need to do a full outer join first, then
    # take only rows that were marked as right-only by the indicator. After
    # that, there will be copies of every column in both, but for the
    # right-only rows the columns from the left df will be null, so they're
    # all dropped afterwards.
    outer_join = pd.merge(main_snapshot,
                          secondary_snapshot,
                          on=key_cols,
                          how="outer",
                          suffixes=("_old", ""),
                          indicator=True)
    additions_df = outer_join[(outer_join._merge=="right_only")].drop("_merge", axis=1)
    additions_df = additions_df[additions_df.columns[~additions_df.columns.str.endswith('_old')]]

    # Add markers
    additions_df["is_constructed"] = True
    additions_df["accept_changes"] = False

    return additions_df


def output_construction_files(amendments_df, additions_df, config, write_csv, run_id):
    """TODO"""
    network_or_hdfs = config["global"]["network_or_hdfs"]
    if network_or_hdfs == "network":
        # TODO Prepare filepaths
        write_local_csv(filepath=amendments_filepath, data=amendments_df)
        write_local_csv(filepath=additions_filepath, data=additions_df)
    elif network_or_hdfs == "hdfs":
        # TODO Prepare filepaths
        write_hdfs_csv(filepath=amendments_filepath, data=amendments_df)
        write_hdfs_csv(filepath=additions_filepath, data=additions_df)

    return True


def apply_construction(main_df, config):
    """TODO"""
    network_or_hdfs = config["global"]["network_or_hdfs"]
    if network_or_hdfs == "network":

        amendments_filepath = None # TODO
        local_file_exists(amendments_filepath)
        amendments_df = read_local_csv(amendments_filepath)

        additions_filepath = None # TODO
        local_file_exists(additions_filepath)
        additions_df = read_local_csv(additions_filepath)

        construction_filepath = None # TODO

    elif network_or_hdfs == "hdfs":
        amendments_filepath = None # TODO
        hdfs_isfile(amendments_filepath)
        amendments_df = read_hdfs_csv(amendments_filepath)

        additions_filepath = None # TODO
        hdfs_isfile(additions_filepath)
        additions_df = read_hdfs_csv(additions_filepath)

        construction_filepath = None # TODO

    # Apply the amendments and additions
    constructed_df = apply_amendments(main_df, amendments_df)
    constructed_df = apply_additions(main_df, additions_df)
    
    # Save the constructed dataframe as a CSV
    # TODO construction_filepath needs to be different from above
    if network_or_hdfs == "network":
        write_hdfs_csv(construction_filepath, constructed_df)
    elif network_or_hdfs == "hdfs":
        write_local_csv(construction_filepath, constructed_df)

    return True


def apply_amendments(main_df, amendments_df):
    """TODO"""
    accepted_amendments_df = amendments_df.drop(amendments_df[amendments_df.accept_changes == False])
    if accepted_amendments_df.shape[0] > 0:
        # TODO Left join them to the main df on the keys
        # TODO For records with is_constructed = True, overwrite their old values with the constructed values
        construction_logger.info(f"{accepted_amendments_df.shape[0]} records amended during construction")
        pass
    else:
        construction_logger.info("No amended records found during construction")
    
    return amended_df


def apply_additions(main_df, additions_df):
    """TODO"""
    # Drop any records where accept_changes = False and if any records remain, union them to the main df
    accepted_additions_df = additions_df.drop(additions_df[additions_df.accept_changes == False])
    if accepted_additions_df.shape[0] > 0:
        added_df = pd.concat([main_df, accepted_additions_df])
        construction_logger.info(f"{accepted_additions_df.shape[0]} records added during construction")
    else:
        construction_logger.info("No additional records found during construction")

    return added_df
