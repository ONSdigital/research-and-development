"""The main file for the staging and validation module."""
import logging
import pandas as pd
from typing import Callable

from src.staging import spp_parser
from src.staging import spp_snapshot_processing as processing
from src.staging import validation as val

StagingMainLogger = logging.getLogger(__name__)


def run_staging(
    config: dict, check_file_exists: Callable, load_json: Callable
) -> pd.DataFrame:
    """Run the staging and validation module.

    The snapshot data is ingested from a json file, and parsed into dataframes,
    one for survey contributers and another for their responses. These are merged
    and transmuted so each question has its own column. The resulting dataframe
    undergoes validation and then is returned to the pipeline.

    Args:
        config (dict): The pipeline configuration
        check_file_exists (Callable): Function to check if file exists
            This will be the hdfs or network version depending on settings.
        load_json (Callable): Function to load a json file.
            This will be the hdfs or network version depending on settings.
    Returns:
        pd.DataFrame: The staged and vaildated snapshot data.
    """

    network_or_hdfs = config["global"]["network_or_hdfs"]
    snapshot_path = config[f"{network_or_hdfs}_paths"]["snapshot_path"]

    # Check data file exists, raise an error if it does not.
    check_file_exists(snapshot_path)

    # load and parse the snapshot data json file
    snapdata = load_json(snapshot_path)
    contributors_df, responses_df = spp_parser.parse_snap_data(snapdata)
    StagingMainLogger.info("Finished Data Ingest...")

    # Data Transmutation
    StagingMainLogger.info("Starting Data Transmutation...")
    full_responses = processing.full_responses(contributors_df, responses_df)

    processing.response_rate(contributors_df, responses_df)
    StagingMainLogger.info("Finished Data Transmutation...")

    # Data validation
    val.check_data_shape(full_responses)

    # Check the postcode column
    postcode_masterlist = config["hdfs_paths"]["postcode_masterlist"]
    val.validate_post_col(contributors_df, postcode_masterlist)

    return full_responses
