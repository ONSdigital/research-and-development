import logging
from collections.abc import Callable

import pandas as pd
import numpy as np

from src.freezing.freezing_utils import (
    _add_last_frozen_column,
    validate_additions_df,
    drop_cols,
)
from src.utils.breakdown_validation import get_all_wanted_columns

FreezingApplyLogger = logging.getLogger(__name__)


def apply_freezing(
    main_df: pd.DataFrame,
    config: dict,
    check_file_exists: Callable,
    read_csv: Callable,
) -> pd.DataFrame:
    """Read user-edited freezing files and apply them to the main snapshot.
    Args:
        main_df (pd.DataFrame): The main snapshot.
        config (dict): The pipeline configuration.
        check_file_exists (callable): Function to check if file exists. This will
            be the hdfs or network version depending on settings.
        read_csv (callable): Function to read a csv file. This will be the hdfs or
            network version depending on settings.

    Returns:
        constructed_df (pd.DataFrame): As main_df but with records amended and added
            from the freezing files.
    """
    # Prepare filepaths to read from
    freezing_paths = config["freezing_paths"]
    amendments_filepath = freezing_paths["freezing_amendments_path"]
    additions_filepath = freezing_paths["freezing_additions_path"]
    deletions_filepath = freezing_paths["freezing_deletions_path"]

    # Check if the freezing files exist
    amendments_exist = check_file_exists(amendments_filepath)
    additions_exist = check_file_exists(additions_filepath)
    deletions_exist = check_file_exists(deletions_filepath)

    # If each file exists, read it and call the function to apply them
    if not amendments_exist and not additions_exist and not deletions_exist:
        FreezingApplyLogger.info(
            "No amendments, additions or deletions to apply, skipping..."
        )
        return main_df

    # apply amendments
    if amendments_exist:
        amendments_df = read_csv(amendments_filepath)
        if amendments_df.empty:
            FreezingApplyLogger.warning(
                f"Amendments file ({amendments_filepath}) is empty, skipping..."
            )
        else:
            main_df = apply_amendments(main_df, amendments_df, config)

    # apply additions
    if additions_exist:
        deletions_df = read_csv(additions_filepath)
        if deletions_df.empty:
            FreezingApplyLogger.warning(
                f"Additions file {additions_filepath} is empty, skipping..."
            )
        else:
            main_df = apply_additions(main_df, deletions_df, config)

    # apply deletions
    if deletions_exist:
        deletions_df = read_csv(deletions_filepath)
        if deletions_df.empty:
            FreezingApplyLogger.warning(
                f"Deletions file {deletions_filepath} is empty, skipping..."
            )
        else:
            main_df = apply_deletions(main_df, deletions_df, config)
    # retore nulls in the instance column
    main_df["instance"] = main_df["instance"].replace(-1, np.nan)
    return main_df


def apply_amendments(
    main_df: pd.DataFrame,
    amendments_df: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """Apply amendments to the main snapshot.

    Args:
        main_df (pd.DataFrame): The main snapshot.
        amendments_df (pd.DataFrame): The amendments to apply.
        config (dict): The pipeline configuration.

    Returns:
        amended_df (pd.DataFrame): The main snapshot with amendments applied.
    """
    # Get references where accept_changes is True
    changes_refs = amendments_df[
        amendments_df.accept_changes.isin([True])
    ].reference.unique()

    # Filter amendments to only include those marked for inclusion
    accepted_amendments_df = amendments_df[amendments_df.reference.isin(changes_refs)]

    if accepted_amendments_df.shape[0] == 0:
        FreezingApplyLogger.info(
            "Amendments file contained no records marked for inclusion"
        )
        return main_df

    # Separate out the rows to be deleted
    # fill nulls in the boolean column with "amendment" as a safeguard
    accepted_amendments_df = accepted_amendments_df.copy()
    # Avoid changing the original df
    accepted_amendments_df["change_type"] = (
        accepted_amendments_df["change_type"].fillna("amendment").astype(str)
    )
    deletion_cond = accepted_amendments_df["change_type"].isin(["deletion"])
    # Separate the dataframe into deletion rows and non-deletion rows
    delete_rows_df = accepted_amendments_df.copy()[deletion_cond]
    accepted_amendments_df = accepted_amendments_df.copy()[~deletion_cond]

    # apply the deletions to the main df
    main_df = apply_deletions(main_df, delete_rows_df, config)

    # Drop the diff columns and accept_changes col
    accepted_amendments_df = accepted_amendments_df.drop(
        columns=[col for col in accepted_amendments_df.columns if col.endswith("_diff")]
    )
    # drop other unwanted columns
    accepted_amendments_df = drop_cols(accepted_amendments_df)

    # rename columns
    accepted_amendments_df.columns = [
        col.replace("_updated", "") for col in accepted_amendments_df.columns
    ]

    # update last_frozen column
    accepted_amendments_df = _add_last_frozen_column(accepted_amendments_df, config)

    # list of tuples with values to filter
    values_to_filter = (
        accepted_amendments_df[["reference", "instance"]].apply(tuple, axis=1).tolist()
    )

    # drop records to be amended from main df bassed on values_to_filter
    main_df = main_df[
        ~main_df[["reference", "instance"]].apply(tuple, axis=1).isin(values_to_filter)
    ]

    # add amended records to main df
    amended_df = pd.concat([main_df, accepted_amendments_df])

    FreezingApplyLogger.info(
        f"{accepted_amendments_df.shape[0]} record(s) amended during freezing"
    )

    # Apply deletions for 604
    amended_df = apply_deletions_604(amended_df, accepted_amendments_df, config)

    return amended_df


def apply_additions(
    main_df: pd.DataFrame,
    additions_df: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """Apply additions to the main snapshot.

    Args:
        main_df (pd.DataFrame): The main snapshot.
        additions_df (pd.DataFrame): The additions to apply.
        config (dict): The pipeline configuration.

    Returns:
        added_df (pd.DataFrame): The main snapshot with additions applied.
    """
    if not validate_additions_df(main_df, additions_df, FreezingApplyLogger):
        FreezingApplyLogger.info(
            "Skipping additions since the additions csv is invalid..."
        )
        return main_df
    # References in the additions data frame do not exist in the frozen data
    # The exception to this is status "Form sent out", where no return was given.
    # If any row is marked True, then all rows with that reference are included.
    changes_refs = additions_df[
        additions_df.accept_changes.isin([True])
    ].reference.unique()

    accepted_additions_df = additions_df[additions_df.reference.isin(changes_refs)]

    # removes the old form sent out where we have a new clear response
    remove_status = [
        "Form sent out",
        "Ceased trading (NIL4)",
        "Out of scope (NIL3)",
        "Dormant (NIL5)",
    ]
    main_df = main_df[
        ~(
            (main_df.reference.isin(accepted_additions_df.reference))
            & (main_df.status.isin(remove_status))
        )
    ]

    accepted_additions_df = drop_cols(accepted_additions_df)
    if accepted_additions_df.shape[0] > 0:
        accepted_additions_df = _add_last_frozen_column(accepted_additions_df, config)
        added_df = pd.concat([main_df, accepted_additions_df], ignore_index=True)
        FreezingApplyLogger.info(
            f"{accepted_additions_df.shape[0]} record(s) added during freezing"
        )
    else:
        FreezingApplyLogger.info(
            "Additions file contained no records marked for inclusion"
        )
        return main_df
    return added_df


def apply_deletions(
    main_df: pd.DataFrame,
    deletions_df: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """Apply deletions to the main snapshot.

    Args:
        main_df (pd.DataFrame): The main snapshot.
        deletions_df (pd.DataFrame): The deletions to apply.
        config (dict): The pipeline configuration.

    Returns:
        deleted_df (pd.DataFrame): The main snapshot with deletions applied.
    """
    # Fill nulls in the boolean column with False as a safeguard
    deletions_df["accept_changes"] = (
        deletions_df["accept_changes"].fillna(False).astype(bool)
    )

    # For long forms, if the instance 0 is in the deletions data, then all instances
    # must be deleted. Get a list of references this refers to
    deletions_cond = deletions_df.accept_changes.isin([True])
    delete_all_df = deletions_df[deletions_cond & (deletions_df.instance == 0)]
    refs_to_delete = delete_all_df.reference.unique()
    # Ensure all rows in the references to delete have accept_changes = True
    deletions_df.loc[deletions_df.reference.isin(refs_to_delete), "accept_changes"] = (
        True
    )
    # prepare the deletions_df for merging by filtering for the rows to delete
    accepted_deletions_df = deletions_df.copy()[deletions_cond]
    rows_deleted = accepted_deletions_df.shape[0]
    if rows_deleted == 0:
        FreezingApplyLogger.info(
            "Deletions file contained no records marked for inclusion"
        )
        return main_df

    # Replace nulls in the 'instance' column with a placeholder value (-1)
    main_df["instance"] = main_df["instance"].fillna(-1)
    accepted_deletions_df["instance"] = accepted_deletions_df["instance"].fillna(-1)

    # join the deletions data to the main (frozen) dataframe
    merged_df = main_df.merge(
        accepted_deletions_df[["reference", "instance", "accept_changes"]],
        how="left",
        on=["reference", "instance"],
    )
    # Filter the merged dataframe to remove rows with accept_changes = True
    merged_df["accept_changes"] = merged_df["accept_changes"].fillna(False)
    reduced_df = merged_df.copy()[merged_df["accept_changes"].isin([False])]

    reduced_df = _add_last_frozen_column(reduced_df, config)
    reduced_df = drop_cols(reduced_df)

    FreezingApplyLogger.info(f"{rows_deleted} record(s) removed during freezing")

    return reduced_df


def apply_deletions_604(main_df, accepted_amendments_df, config):
    """Apply deletions for 604.

    Checks if accepted_amendments_df contains any rows where
    instance = 0 and 604 = No. If one or more rows meet this condition,
    accepted_amendments_df is filtered for conditions where instance = 0,
    and 604 = No, creating flagged_df. The flagged references and periods
    are then used to filter main_df for rows where instance is greater than 0.

    Args:
        main_df (pd.DataFrame): The main DataFrame.
        accepted_amendments_df (pd.DataFrame): The accepted amendments DataFrame.

    Returns:
        pd.DataFrame: The main DataFrame with deletions applied
    """

    # Check if any row meets the condition of instance = 0 and 604 = No
    condition = (accepted_amendments_df["instance"] == 0) & (
        accepted_amendments_df["604"] == "No"
    )

    if condition.any():

        # Filter the accepted_amendments_df Dataframe based on for
        # instance =  0 and 604 = No
        flagged_df = accepted_amendments_df[condition]

        # Get pairs of values for columns "reference" and "period"
        # where instance = 0 and 604 = No.
        flagged_references = list(flagged_df["reference"])

        # Create a mask to identify rows where reference is in flagged_references
        # and instance is greater than 0
        reference_greater_0_mask = (main_df["reference"].isin(flagged_references)) & (
            main_df["instance"] > 0
        )

        # Filter the DataFrame using the mask
        main_df = main_df[~reference_greater_0_mask]

        # Create a mask to identify rows where reference is in flagged_references
        # and instance is  equal to 0
        reference_equal_0_mask = (main_df["reference"].isin(flagged_references)) & (
            main_df["instance"] == 0
        )

        # Replace the value in column '604' with 'No' when reference is in
        # flagged_references and instance is 0
        main_df.loc[
            reference_equal_0_mask,
            "604",
        ] = "No"

        # For rows where instance = 0 and 604 was changed from "Yes" to "No", ensure
        # columns 4xx's, 5xx's, and 7xx's are set to 0.0.

        # get columns to check
        check_list_604 = get_all_wanted_columns(config, "instance_0_cols")

        # for columns that are instance 0, and were identified as having 604 set from
        # Yes to No, set the value in columns in check_list_604 to 0.0.
        main_df.loc[reference_equal_0_mask, check_list_604] = 0.0

    return main_df
