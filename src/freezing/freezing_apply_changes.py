
import logging
import os
from datetime import datetime
from typing import Union, Callable

import pandas as pd

# TODO: Create a utils function to replicate this, for any column given a set of values
from src.construction.construction_validation import _references_in_snapshot

def apply_freezing(
    main_df: pd.DataFrame,
    config: dict,
    check_file_exists: Callable,
    read_csv: Callable,
    write_csv: Callable,
    run_id: int,
    freezing_logger: logging.Logger,
    ) -> pd.DataFrame:
    """Read user-edited freezing files and apply them to the main snapshot.
    Args:
        main_df (pd.DataFrame): The main snapshot.
        config (dict): The pipeline configuration.
        check_file_exists (callable): Function to check if file exists. This will
            be the hdfs or network version depending on settings.
        read_csv (callable): Function to read a csv file. This will be the hdfs or
            network version depending on settings.
        write_csv (callable): Function to write to a csv file. This will be the
            hdfs or network version depending on settings.
        run_id (int): The run id for this run.

    Returns:
        constructed_df (pd.DataFrame): As main_df but with records amended and added
            from the freezing files.
    """
    # Prepare filepaths to read from
    network_or_hdfs = config["global"]["network_or_hdfs"]
    paths = config[f"{network_or_hdfs}_paths"]
    amendments_filepath = paths["freezing_amendments_path"]
    additions_filepath = paths["freezing_add_path"]

    # Check if the freezing files exist
    amendments_exist = check_file_exists(amendments_filepath)
    additions_exist = check_file_exists(additions_filepath)

    # If each file exists, read it and call the function to apply them
    if not (amendments_exist and additions_exist):
        freezing_logger.info("No amendments or additions to apply, skipping...")
        return main_df

    # apply amendments
    if amendments_exist:
        amendments_df = read_csv(amendments_filepath)
        if amendments_df.empty:
            freezing_logger.warning(
                f"Amendments file ({amendments_filepath}) is empty, skipping..."
            )
        else:
            constructed_df = apply_amendments(main_df, amendments_df)

    # apply additions
    if additions_exist:
        additions_df = read_csv(additions_filepath)
        if additions_df.empty:
            freezing_logger.warning(
            f"Additions file {additions_filepath} is empty, skipping..."
        )
        else:
            additions_df["instance"] = additions_df["instance"].astype("Int64")
            constructed_df = apply_additions(main_df, additions_df)


    # Save the constructed dataframe as a CSV
    tdate = datetime.now().strftime("%y-%m-%d")
    survey_year = config["years"]["survey_year"]
    freezing_output_filepath = os.path.join(
        paths["root"],
        "freezing",
        f"{survey_year}_constructed_snapshot_{tdate}_v{run_id}.csv",
    )
    write_csv(freezing_output_filepath, constructed_df)
    return constructed_df


def validate_amendments_df(
        df: pd.DataFrame,
        freezing_logger: logging.Logger,
    ) -> None:
    # check all columns are present (in staged frozen data)
    # check that all ref/inst combs are in staged frozen data
    pass


def validate_amendments_df(
        df: pd.DataFrame,
        freezing_logger: logging.Logger,
    ) -> None:
    # check all columns are present (in staged frozen data)
    # check that all ref/inst combs are not in staged frozen data
    pass
    

def apply_amendments(
    main_df: pd.DataFrame,
    amendments_df: pd.DataFrame,
    freezing_logger: logging.Logger
    ) -> pd.DataFrame:
    """Apply amendments to the main snapshot.

    Args:
        main_df (pd.DataFrame): The main snapshot.
        amendments_df (pd.DataFrame): The amendments to apply.

    Returns:
        amended_df (pd.DataFrame): The main snapshot with amendments applied.
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

    accepted_amendments_df = amendments_df.drop(
        amendments_df[~amendments_df.accept_changes].index
    )

    if accepted_amendments_df.shape[0] == 0:
        freezing_logger.info(
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

    freezing_logger.info(
        f"{accepted_amendments_df.shape[0]} records amended during freezing"
    )
    # update last_frozen columns
    return amended_df


def apply_additions(
    main_df: pd.DataFrame,
    additions_df: pd.DataFrame,
    freezing_logger: logging.Logger
    ) -> pd.DataFrame:
    """Apply additions to the main snapshot.

    Args:
        main_df (pd.DataFrame): The main snapshot.
        additions_df (pd.DataFrame): The additions to apply.

    Returns:
        added_df (pd.DataFrame): The main snapshot with additions applied.
    """
    # Drop records where accept_changes is False and if any remain, add them to main df
    accepted_additions_df = additions_df.drop(
        additions_df[~additions_df["accept_changes"]].index
    )
    if accepted_additions_df.shape[0] > 0:
        added_df = pd.concat([main_df, accepted_additions_df], ignore_index=True)
        freezing_logger.info(
            f"{accepted_additions_df.shape[0]} records added during freezing"
        )
    else:
        freezing_logger.info("Additions file contained no records marked for inclusion")
        return main_df
    # update last_frozen column
    return added_df
