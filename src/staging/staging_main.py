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
    run_id: int,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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
        run_id (int): The run id for this run.
    Returns:
        pd.DataFrame: The staged and vaildated snapshot data.
    """
    # Check the environment switch
    network_or_hdfs = config["global"]["network_or_hdfs"]

    # Conditionally load paths
    paths = config[f"{network_or_hdfs}_paths"]
    snapshot_path = paths["snapshot_path"]

    # Load historic data
    if config["global"]["load_historic_data"]:
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

    else:
        StagingMainLogger.info("Skipping loading historic data...")

    # Check data file exists, raise an error if it does not.
    StagingMainLogger.info("Loading SPP snapshot data...")
    check_file_exists(snapshot_path)

    # load and parse the snapshot data json file
    snapdata = load_json(snapshot_path)
    contributors_df, responses_df = spp_parser.parse_snap_data(snapdata)

    # the anonymised snapshot data we use in the DevTest environment
    # does not include the instance column. This fix should be removed
    # when new anonymised data is given.
    if network_or_hdfs == "hdfs" and config["global"]["dev_test"]:
        responses_df["instance"] = 0
    StagingMainLogger.info("Finished Data Ingest...")

    val.validate_data_with_schema(contributors_df, "./config/contributors_schema.toml")
    val.validate_data_with_schema(responses_df, "./config/long_response.toml")

    # Data Transmutation
    StagingMainLogger.info("Starting Data Transmutation...")
    full_responses = processing.full_responses(contributors_df, responses_df)

    # Validate and force data types for the full responses df
    # TODO Find a fix for the datatype casting before uncommenting
    val.combine_schemas_validate_full_df(
        full_responses,
        "config/contributors_schema.toml",
        "config/wide_responses.toml",
    )

    # Data validation
    val.check_data_shape(full_responses)

    processing.response_rate(contributors_df, responses_df)
    StagingMainLogger.info(
        "Finished Data Transmutation and validation of full responses dataframe"
    )

    # Stage and validate the postcode column
    if config["global"]["validate_postcodes"]:
        StagingMainLogger.info("Starting PostCode Validation")
        postcode_masterlist = paths["postcode_masterlist"]
        check_file_exists(postcode_masterlist)
        postcode_masterlist = read_csv(postcode_masterlist, ["pcd"])
        invalid_df, unreal_df = val.validate_post_col(
            full_responses, postcode_masterlist, config
        )
        pcodes_folder = paths["postcode_path"]
        tdate = datetime.now().strftime("%Y-%m-%d")
        invalid_filename = f"invalid_pattern_postcodes_{tdate}_v{run_id}.csv"
        unreal_filename = f"missing_postcodes_{tdate}_v{run_id}.csv"
        write_csv(f"{pcodes_folder}/{invalid_filename}", invalid_df)
        write_csv(f"{pcodes_folder}/{unreal_filename}", unreal_df)
    else:
        StagingMainLogger.info("PostCode Validation skipped")

    # Stage the manual outliers file
    StagingMainLogger.info("Loading Manual Outlier File")
    manual_path = paths["manual_outliers_path"]
    check_file_exists(manual_path)
    wanted_cols = ["reference", "instance", "manual_outlier"]
    manual_outliers = read_csv(manual_path, wanted_cols)
    val.validate_data_with_schema(
        manual_outliers, "./config/manual_outliers_schema.toml"
    )
    StagingMainLogger.info("Manual Outlier File Loaded Successfully...")

    # Load the PG mapper
    mapper_path = paths["mapper_path"]
    check_file_exists(mapper_path)
    mapper = read_csv(
        mapper_path,
        cols=[
            "SIC 2003",
            "SIC 2003_Description",
            "SIC 2007_CODE",
            "SIC 2007_Description",
            "Table 23 SIC Group",
            "2010 Pub PG",
            "2016 > Pub PG",
            "2009 Form PG",
            "2010 Form PG",
            "2016 > Form PG",
        ],
    )

    # Load ultfoc (Foreign Ownership) mapper
    StagingMainLogger.info("Loading Foreign Ownership File")
    ultfoc_mapper_path = paths["ultfoc_mapper_path"]
    check_file_exists(ultfoc_mapper_path)
    ultfoc_mapper = read_csv(ultfoc_mapper_path)
    val.validate_data_with_schema(ultfoc_mapper, "./config/ultfoc_schema.toml")
    val.validate_ultfoc_df(ultfoc_mapper)
    StagingMainLogger.info("Foreign Ownership File Loaded Successfully...")

    # Loading cell number covarege
    StagingMainLogger.info("Loading Cell Covarage File...")
    cellno_path = paths["cellno_path"]
    check_file_exists(cellno_path)
    cellno_df = read_csv(cellno_path)
    StagingMainLogger.info("Covarage File Loaded Successfully...")

    # Output the staged BERD data for BaU testing when on local network.
    if (network_or_hdfs == "network") & (config["global"]["output_full_responses"]):
        StagingMainLogger.info("Starting output of staged BERD data...")
        test_folder = config["network_paths"]["staging_test_foldername"]
        tdate = datetime.now().strftime("%Y-%m-%d")
        staged_filename = f"staged_BERD_full_responses_{tdate}_v{run_id}.csv"
        write_csv(f"{test_folder}/{staged_filename}", full_responses)
        StagingMainLogger.info("Finished output of staged BERD data.")
    else:
        StagingMainLogger.info("Skipping output of staged BERD data...")

    return full_responses, manual_outliers, mapper, ultfoc_mapper, cellno_df
