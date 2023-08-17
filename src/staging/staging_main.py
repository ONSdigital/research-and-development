"""The main file for the staging and validation module."""
import logging
import pandas as pd
from typing import Callable, Tuple
from datetime import datetime

from src.staging import spp_parser, history_loader
from src.staging import spp_snapshot_processing as processing
from src.staging import validation as val

StagingMainLogger = logging.getLogger(__name__)


def run_staging(
    config: dict,
    check_file_exists: Callable,
    load_json: Callable,
    read_csv: Callable,
    write_csv: Callable,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Run the staging and validation module.

    The snapshot data is ingested from a json file, and parsed into dataframes,
    one for survey contributers and another for their responses. These are merged
    and transmuted so each question has its own column. The resulting dataframe
    undergoes validation.

    When running on the local network,

    Args:
        config (dict): The pipeline configuration
        check_file_exists (Callable): Function to check if file exists
            This will be the hdfs or network version depending on settings.
        load_json (Callable): Function to load a json file.
            This will be the hdfs or network version depending on settings.
        read_csv (Callable): Function to read a csv file.
            This will be the hdfs or network version depending on settings.
        write_csv (Callable): Function to write to a csv file.
            This will be the hdfs or network version depending on settings.
    Returns:
        pd.DataFrame: The staged and vaildated snapshot data.
    """
    # Check the environment switch
    network_or_hdfs = config["global"]["network_or_hdfs"]

    # Conditionally load paths
    paths = config[f"{network_or_hdfs}_paths"]
    snapshot_path = paths["snapshot_path"]

    # Load historic data
    curent_year = config["years"]["current_year"]
    years_to_load = config["years"]["previous_years_to_load"]
    years_gen = history_loader.history_years(curent_year, years_to_load)

    if years_gen is None:
        StagingMainLogger.info("No historic data to load for this run.")
    else:
        StagingMainLogger.info("Loading historic data...")
        history_path = paths["history_path"]
        dict_of_hist_dfs = history_loader.load_history(
            years_gen, history_path, read_csv
        )
        # Check if it has loaded and is not empty
        if isinstance(dict_of_hist_dfs, dict) and bool(dict_of_hist_dfs):
            StagingMainLogger.info(
                "Dictionary of history data: %s loaded into pipeline",
                ", ".join(dict_of_hist_dfs),
            )
            StagingMainLogger.info("Historic data loaded.")
        else:
            StagingMainLogger.warning(
                "Problem loading historic data. Dict may be empty or not present"
            )
            raise Exception("The historic data did not load")

    # Check data file exists, raise an error if it does not.
    StagingMainLogger.info("Loading SPP snapshot data...")
    check_file_exists(snapshot_path)

    # load and parse the snapshot data json file
    snapdata = load_json(snapshot_path)
    contributors_df, responses_df = spp_parser.parse_snap_data(snapdata)

    # Load the PG mapper
    mapper_path = paths["mapper_path"]
    mapper = read_csv(mapper_path)

    # the anonymised snapshot data we use in hdfs
    # does not include the instance column. This fix should be removed
    # when new anonymised data is given.
    if network_or_hdfs == "hdfs":
        responses_df["instance"] = 0
    StagingMainLogger.info("Finished Data Ingest...")

    val.validate_data_with_schema(contributors_df, "./config/contributors_schema.toml")
    val.validate_data_with_schema(responses_df, "./config/long_response.toml")

    # Data Transmutation
    StagingMainLogger.info("Starting Data Transmutation...")
    full_responses = processing.full_responses(contributors_df, responses_df)

    # Validate and force data types for the full responses df
    val.validate_data_with_both_schema(
        full_responses,
        "config/contributors_schema.toml",
        "config/wide_responses.toml",
    )

    # Data validation
    val.check_data_shape(full_responses)

    # Validate the postcode column
    postcode_masterlist = config["hdfs_paths"]["postcode_masterlist"]
    val.validate_post_col(contributors_df, postcode_masterlist)

    processing.response_rate(contributors_df, responses_df)
    StagingMainLogger.info(
        "Finished Data Transmutation and validation of full responses dataframe"
    )

    # Output the staged BERD data for BaU testing when on local network.
    if network_or_hdfs == "network":
        StagingMainLogger.info("Starting output of staged BERD data...")
        test_folder = config["network_paths"]["staging_test_foldername"]
        tdate = datetime.now().strftime("%Y-%m-%d")
        staged_filename = f"staged_BERD_full_responses_{tdate}.csv"
        write_csv(f"{test_folder}/{staged_filename}", full_responses)
        StagingMainLogger.info("Finished output of staged BERD data.")

    return full_responses, mapper
