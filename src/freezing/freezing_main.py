import logging
from datetime import datetime
from typing import Callable

import pandas as pd

from src.staging.validation import validate_data_with_schema
from src.utils.defence import type_defence


FreezingLogger = logging.getLogger(__name__)


def run_freezing(
    main_snapshot: pd.DataFrame,
    # secondary_snapshot: pd.DataFrame,
    config: dict,
    # check_file_exists: Callable,
    write_csv: Callable,
    read_csv: Callable,
    run_id: int,
) -> pd.DataFrame:
    """Run the freezing module.

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
            and added from the freezing files.
    """
    # return frozen snapshot if config allows
    run_first_snapshot_of_results = config["global"]["run_first_snapshot_of_results"]
    frozen_data_staged_path = config["freezing_paths"]["frozen_data_staged_path"]
    run_updates_and_freeze = config["global"]["run_updates_and_freeze"]

    if not run_first_snapshot_of_results:
        FreezingLogger.info("Loading frozen data...")
        frozen_data_staged_csv = read_csv(frozen_data_staged_path)
        validate_data_with_schema(
            frozen_data_staged_path, "./config/frozen_data_staged_schema.toml"
        )
        FreezingLogger.info(
            "Frozen data successfully read from {frozen_data_staged_path}"
        )
        return frozen_data_staged_csv

    if run_first_snapshot_of_results:
        updated_snapshot = main_snapshot.copy()
        updated_snapshot = _add_last_frozen_column(updated_snapshot)

    # # Skip this module if the secondary snapshot isn't loaded
    # load_updated_snapshot = config["global"]["load_updated_snapshot"]
    # load_manual_freezing = config["global"]["load_manual_freezing"]
    # if load_manual_freezing is False:
    #     freezing_logger.info("Skipping freezing...")
    #     return main_snapshot

    # # ! For now, we add the year column since neither file has it
    # main_snapshot["year"] = 2022
    # if load_updated_snapshot is True:
    #     secondary_snapshot["year"] = 2022

    #     # Use the secondary snapshot to generate freezing files for the next run
    #     additions_df = get_additions(main_snapshot, secondary_snapshot)
    #     amendments_df = get_amendments(main_snapshot, secondary_snapshot)
    #     output_freezing_files(
    #         amendments_df, additions_df, config, write_csv, run_id
    #     )

    # # Read the freezing files from the last run and apply them
    # constructed_df = apply_freezing(
    #     main_snapshot, config, check_file_exists, read_csv, write_csv, run_id
    # )
    # constructed_df.reset_index(drop=True, inplace=True)

    if run_first_snapshot_of_results or run_updates_and_freeze:
        FreezingLogger.info("Outputting frozen data file.")
        tdate = datetime.now().strftime("%y-%m-%d")
        survey_year = config["years"]["survey_year"]
        filename = (
            f"{survey_year}_FROZEN_staged_BERD_full_responses_{tdate}_v{run_id}.csv"
        )
        write_csv(f"{frozen_data_staged_path}/{filename}", updated_snapshot)

    return updated_snapshot


def read_frozen_csv():
    # new functionality
    pass

# function ready for use
def _add_last_frozen_column(
        frozen_df: pd.DataFrame, 
        run_id: int
    ) -> pd.DataFrame:
    """Add the last_frozen column to staged data.

    Args:
        frozen_df (pd.DataFrame): The frozen data.
        run_id (int): The current run id.

    Returns:
        pd.DataFrame: A dataframe containing the updated last_frozen column.
    """
    type_defence(frozen_df, "frozen_df", pd.DataFrame)
    type_defence(run_id, "run_id", int)
    todays_date = datetime.today().strftime("%d_%m_%Y")
    last_frozen = f"{todays_date}_{str(run_id)}"
    frozen_df["last_frozen"] = last_frozen
    return frozen_df
