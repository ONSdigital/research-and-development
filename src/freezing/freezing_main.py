import logging
import os
from datetime import datetime
from typing import Callable

import pandas as pd

from src.freezing.freezing_utils import  _add_last_frozen_column
from src.freezing.freezing_apply_changes import apply_freezing
from src.staging.validation import validate_data_with_schema
from src.utils.helpers import convert_formtype
from src.freezing.freezing_compare import get_additions, get_amendments, output_freezing_files


FreezingLogger = logging.getLogger(__name__)


def run_freezing(
    snapshot_df: pd.DataFrame,
    config: dict,
    write_csv: Callable,
    read_csv: Callable,
    check_file_exists: Callable,
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
        check_file_exists (callable): Function to check if a file exists.
        run_id (int): The run id for this run.
    Returns:
        prepared_frozen_data (pd.DataFrame): As snapshot_df but with records amended
            and added from the freezing files.
    """
    
    # read in validated config settings
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

    # Read the freezing files and apply them
    elif run_updates_and_freeze:
        frozen_data = read_frozen_csv(config, read_csv)
        prepared_frozen_data = apply_freezing(
            frozen_data, config, check_file_exists, read_csv, run_id, FreezingLogger
        )
        prepared_frozen_data.reset_index(drop=True, inplace=True)

    elif run_frozen_data:
        prepared_frozen_data = read_frozen_csv(config, read_csv)

    else:
        prepared_frozen_data = snapshot_df.copy()
        prepared_frozen_data = _add_last_frozen_column(prepared_frozen_data, run_id)


    if run_with_snapshot_until_freezing or run_updates_and_freeze:
        frozen_data_staged_output_path = config["freezing_paths"]["frozen_data_staged_output_path"]
        FreezingLogger.info("Outputting frozen data file.")
        tdate = datetime.now().strftime("%y-%m-%d")
        survey_year = config["years"]["survey_year"]
        filename = (
            f"{survey_year}_FROZEN_staged_BERD_full_responses_{tdate}_v{run_id}.csv"
        )
        write_csv(
            os.path.join(frozen_data_staged_output_path, filename), prepared_frozen_data
        )

    return prepared_frozen_data


def read_frozen_csv(config: dict, read_csv: Callable) -> pd.DataFrame:
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
    validate_data_with_schema(frozen_csv, "./config/frozen_data_staged_schema.toml")

    frozen_csv["formtype"] = frozen_csv["formtype"].apply(convert_formtype)
    FreezingLogger.info(f"Frozen data successfully read from {frozen_data_staged_path}")
    return frozen_csv
