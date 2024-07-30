# import os
import logging
import pandas as pd
from typing import Callable
from datetime import datetime


FreezingLogger = logging.getLogger(__name__)


def run_freezing(
    main_snapshot: pd.DataFrame,
    # secondary_snapshot: pd.DataFrame,
    config: dict,
    # check_file_exists: Callable,
    write_csv: Callable,
    # read_csv: Callable,
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

    frozen_data_staged_path = config["freezing_paths"]["frozen_data_staged_path"]
    run_first_snapshot_of_results = config["global"]["run_first_snapshot_of_results"]
    run_updates_and_freeze = config["global"]["run_updates_and_freeze"]

    if run_first_snapshot_of_results:
        updated_snapshot = main_snapshot.copy()

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
