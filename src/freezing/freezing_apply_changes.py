import logging
import os
from datetime import datetime
from typing import Union, Callable

import pandas as pd

from src.utils.helpers import values_in_column
from src.freezing.freezing_utils import _add_last_frozen_column


def apply_freezing(
    main_df: pd.DataFrame,
    config: dict,
    check_file_exists: Callable,
    read_csv: Callable,
    run_id: int,
    FreezingLogger: logging.Logger,
) -> pd.DataFrame:
    """Read user-edited freezing files and apply them to the main snapshot.
    Args:
        main_df (pd.DataFrame): The main snapshot.
        config (dict): The pipeline configuration.
        check_file_exists (callable): Function to check if file exists. This will
            be the hdfs or network version depending on settings.
        read_csv (callable): Function to read a csv file. This will be the hdfs or
            network version depending on settings.
        run_id (int): The run id for this run.
        FreezingLogger (logging.Logger): The logger to log to.

    Returns:
        constructed_df (pd.DataFrame): As main_df but with records amended and added
            from the freezing files.
    """
    # Prepare filepaths to read from
    platform = config["global"]["platform"]
    paths = config[f"{platform}_paths"]
    amendments_filepath = paths["freezing_amendments_path"]
    additions_filepath = paths["freezing_additions_path"]

    # Check if the freezing files exist
    amendments_exist = check_file_exists(amendments_filepath)
    additions_exist = check_file_exists(additions_filepath)

    # If each file exists, read it and call the function to apply them
    if not (amendments_exist or additions_exist):
        FreezingLogger.info("No amendments or additions to apply, skipping...")
        return main_df

    # apply amendments
    if amendments_exist:
        amendments_df = read_csv(amendments_filepath)
        if amendments_df.empty:
            FreezingLogger.warning(
                f"Amendments file ({amendments_filepath}) is empty, skipping..."
            )
        else:
            main_df = apply_amendments(
                main_df,
                amendments_df,
                run_id,
                FreezingLogger,
            )

    # apply additions
    if additions_exist:
        additions_df = read_csv(additions_filepath)
        if additions_df.empty:
            FreezingLogger.warning(
                f"Additions file {additions_filepath} is empty, skipping..."
            )
        else:
            additions_df["instance"] = additions_df["instance"].astype("Int64")
            main_df = apply_additions(main_df, additions_df, run_id, FreezingLogger)

    return main_df


def validate_any_refinst_in_frozen(
    frozen_df: pd.DataFrame,
    df2: pd.DataFrame,
) -> bool:
    """Validate that any of the ref/inst combinations from df2 are in the frozen df.

    Args:
        frozen_df (pd.DataFrame): The frozen csv df
        df2 (pd.DataFrame): A second dataframe.

    Returns:
        bool: Whether any ref/inst combs from df2 are in frozen_df.
    """
    frozen_copy = frozen_df.copy()
    df2_copy = df2.copy()
    frozen_copy["refinst"] = frozen_copy["reference"].astype(str) + frozen_copy[
        "instance"
    ].astype(str)
    df2_copy["refinst"] = df2_copy["reference"].astype(str) + df2_copy[
        "instance"
    ].astype(str)
    result = any([x in list(frozen_copy["refinst"]) for x in list(df2_copy["refinst"])])
    return result


def validate_additions_df(
    frozen_df: pd.DataFrame,
    additions_df: pd.DataFrame,
    FreezingLogger: logging.Logger,
) -> None:
    """Validate the additions df.

    Args:
        frozen_df (pd.DataFrame): The frozen csv df.
        additions_df (pd.DataFrame): The additions df.
        FreezingLogger (logging.Logger): The logger to log to.

    Returns:
        bool: Whether or not the additions df is valid.
    """
    # check that the ref/inst combos are not staged frozen data
    FreezingLogger.info(
        "Checking if any ref/inst in the additions df are in the frozen data..."
    )

    any_present = validate_any_refinst_in_frozen(frozen_df, additions_df)
    if any_present:
        FreezingLogger.info(
            "Some reference/instance combinations from the additions file are "
            "present in the frozen data."
        )
        return False
    return True


def apply_amendments(
    main_df: pd.DataFrame,
    amendments_df: pd.DataFrame,
    run_id: int,
    FreezingLogger: logging.Logger,
) -> pd.DataFrame:
    """Apply amendments to the main snapshot.

    Args:
        main_df (pd.DataFrame): The main snapshot.
        amendments_df (pd.DataFrame): The amendments to apply.
        run_id (int): The current run id.
        FreezingLogger (logging.Logger): The logger.

    Returns:
        amended_df (pd.DataFrame): The main snapshot with amendments applied.
    """
    changes_refs = amendments_df[
        amendments_df.accept_changes == True
    ].reference.unique()

    accepted_amendments_df = amendments_df[amendments_df.reference.isin(changes_refs)]

    if accepted_amendments_df.shape[0] == 0:
        FreezingLogger.info("Amendments file contained no records marked for inclusion")
        return main_df

    # Drop the diff columns and accept_changes col
    accepted_amendments_df = accepted_amendments_df.drop(
        columns=[col for col in accepted_amendments_df.columns if col.endswith("_diff")]
    )
    accepted_amendments_df = accepted_amendments_df.drop("accept_changes", axis=1)

    # rename columns
    accepted_amendments_df.columns = [
        col.replace("_updated", "") for col in accepted_amendments_df.columns
    ]
    # update last_frozen column
    accepted_amendments_df = _add_last_frozen_column(accepted_amendments_df, run_id)

    # drop records to be amended from main df
    main_df = main_df[~main_df.reference.isin(changes_refs)]
    # add amended records to main df
    amended_df = pd.concat([main_df, accepted_amendments_df])
    FreezingLogger.info(
        f"{accepted_amendments_df.shape[0]} record(s) amended during freezing"
    )
    return amended_df


def apply_additions(
    main_df: pd.DataFrame,
    additions_df: pd.DataFrame,
    run_id: int,
    FreezingLogger: logging.Logger,
) -> pd.DataFrame:
    """Apply additions to the main snapshot.

    Args:
        main_df (pd.DataFrame): The main snapshot.
        additions_df (pd.DataFrame): The additions to apply.
        run_id (int): The current run id.
        FreezingLogger (logging.Logger): The logger.

    Returns:
        added_df (pd.DataFrame): The main snapshot with additions applied.
    """
    if not validate_additions_df(main_df, additions_df, FreezingLogger):
        FreezingLogger.info("Skipping additions since the additions csv is invalid...")
        return main_df
    # Drop records where accept_changes is False and if any remain, add them to main df
    changes_refs = additions_df[additions_df.accept_changes == True].reference.unique()

    accepted_additions_df = additions_df[additions_df.reference.isin(changes_refs)]

    # removes the old form sent out where we have a new clear response
    main_df = main_df[~main_df.reference.isin(accepted_additions_df.reference)]

    accepted_additions_df = accepted_additions_df.drop("accept_changes", axis=1)
    if accepted_additions_df.shape[0] > 0:
        accepted_additions_df = _add_last_frozen_column(accepted_additions_df, run_id)
        added_df = pd.concat([main_df, accepted_additions_df], ignore_index=True)
        FreezingLogger.info(
            f"{accepted_additions_df.shape[0]} record(s) added during freezing"
        )
    else:
        FreezingLogger.info("Additions file contained no records marked for inclusion")
        return main_df
    return added_df
