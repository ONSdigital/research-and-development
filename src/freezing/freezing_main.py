import logging
import os
from datetime import datetime
from typing import Callable

import pandas as pd

from src.staging.validation import validate_data_with_schema
from src.utils.defence import type_defence
from src.utils.helpers import convert_formtype
from src.freezing.freezing_compare import get_additions, get_amendments, output_freezing_files


FreezingLogger = logging.getLogger(__name__)


def run_freezing(
    snapshot_df: pd.DataFrame,
    config: dict,
    write_csv: Callable,
    read_csv: Callable,
    run_id: int,
) -> pd.DataFrame:
    """Run the freezing module.

    Args:
        snapshot_df (pd.DataFrame): The staged and vaildated snapshot data.
        config (dict): The pipeline configuration
        write_csv (callable): Function to write to a csv file. This will be the
            hdfs or network version depending on settings.
        read_csv (callable): Function to read a csv file. This will be the hdfs or
            network version depending on settings.
        run_id (int): The run id for this run.
    Returns:
        prepared_frozen_data (pd.DataFrame): As snapshot_df but with records amended
            and added from the freezing files.
    """
    # Determine freezing settings
    run_with_snapshot_until_freezing = config["global"]["run_with_snapshot_until_freezing"]
    load_updated_snapshot_for_comparison = config["global"]["load_updated_snapshot_for_comparison"]
    run_updates_and_freeze = config["global"]["run_updates_and_freeze"]
    run_frozen_data = config["global"]["run_frozen_data"]


    if load_updated_snapshot_for_comparison:
        FreezingLogger.info("Comparing the updated snapshot with the frozen data.")
        updated_snapshot = snapshot_df.copy()
        frozen_data_for_comparison = read_frozen_csv(config, read_csv)
        frozen_data_for_comparison = frozen_data_for_comparison.convert_dtypes()

        # Use the updated snapshot to generate freezing files for the next run
        additions_df = get_additions(frozen_data_for_comparison, updated_snapshot)
        amendments_df = get_amendments(frozen_data_for_comparison, updated_snapshot)
        output_freezing_files(
            amendments_df, additions_df, config, write_csv, run_id
        )
        prepared_frozen_data = snapshot_df.copy()

    elif run_frozen_data:
        prepared_frozen_data = read_frozen_csv(config, read_csv)

    else:
        prepared_frozen_data = main_snapshot.copy()
        prepared_frozen_data = _add_last_frozen_column(prepared_frozen_data, run_id)


    # # Read the freezing files from the last run and apply them
    # constructed_df = apply_freezing(
    #     main_snapshot, config, check_file_exists, read_csv, write_csv, run_id
    # )
    # constructed_df.reset_index(drop=True, inplace=True)

    if run_with_snapshot_until_freezing or run_updates_and_freeze:
        frozen_data_staged_output_path = config["freezing_paths"]["frozen_data_staged_output_path"]
        FreezingLogger.info("Outputting frozen data file.")
        tdate = datetime.now().strftime("%y-%m-%d")
        survey_year = config["years"]["survey_year"]
        filename = (
            f"{survey_year}_FROZEN_staged_BERD_full_responses_{tdate}_v{run_id}.csv"
        )
        write_csv(os.path.join(frozen_data_staged_output_path, filename), prepared_frozen_data)

    return prepared_frozen_data


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
    todays_date = datetime.today().strftime("%y-%m-%d")
    last_frozen = f"{todays_date}_v{str(run_id)}"
    frozen_df["last_frozen"] = last_frozen
    return frozen_df


def read_frozen_csv(config: dict,
                    read_csv: Callable) -> pd.DataFrame:
    """Read the frozen data csv in.

    Args:
        config (dict): The pipeline configuration.
        read_csv (callable): Function to read a csv file. This will be the
            hdfs or network version depending on settings.

    Returns:
        pd.DataFrame: The frozen data csv.
    """
    frozen_data_staged_path = config["freezing_paths"]["frozen_data_staged_path"]
    FreezingLogger.info("Loading frozen data...")
    frozen_csv = read_csv(frozen_data_staged_path)
    validate_data_with_schema(
        frozen_csv, "./config/frozen_data_staged_schema.toml"
    )

    frozen_csv["formtype"] = frozen_csv["formtype"].apply(
        convert_formtype
    )
    FreezingLogger.info(
        "Frozen data successfully read from {frozen_data_staged_path}"
    )
    return frozen_csv
