"""The main file for the staging and validation module."""
import logging
from numpy import random
from typing import Callable, Tuple
from datetime import datetime
import os

from src.staging import spp_parser, history_loader
from src.staging import spp_snapshot_processing as processing
from src.staging import validation as val
from src.staging import pg_conversion as pg

StagingMainLogger = logging.getLogger(__name__)


def run_staging(
    config: dict,
    check_file_exists: Callable,
    load_json: Callable,
    read_csv: Callable,
    write_csv: Callable,
    read_feather: Callable,
    write_feather: Callable,
    run_id: int,
) -> Tuple:
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
        tuple
            full_responses (pd.DataFrame): The staged and vaildated snapshot data,
            manual_outliers (pd.DataFrame): Data with column for manual outliers,
            pg_mapper (pd.DataFrame): Product grouo mapper,
            ultfoc_mapper (pd.DataFrame): Foreign ownership mapper,
            cora_mapper (pd.DataFrame): CORA status mapper,
            cellno_df (pd.DataFrame): Cell numbers mapper,
            postcode_df (pd.DataFrame): Postcodes to Regional Code mapper,
            pg_alpha_num (pd.DataFrame): Product group alpha to numeric mapper.
    """
    # Check the environment switch
    network_or_hdfs = config["global"]["network_or_hdfs"]

    # Conditionally load paths
    paths = config[f"{network_or_hdfs}_paths"]
    snapshot_path = paths["snapshot_path"]
    snapshot_name = os.path.basename(snapshot_path).split(".", 1)[0]

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

    check_file_exists(snapshot_path)

    # load and parse the snapshot data json file
    # Check if feather file exists in snapshot path
    feather_path = paths["feather_path"]
    load_from_feather = config["global"]["load_from_feather"]
    feather_file = os.path.join(feather_path, f"{snapshot_name}.feather")
    feather_files_exist = check_file_exists(feather_file)

    is_network = network_or_hdfs == "network"
    # Only read from feather if feather files exist and we are on network
    if is_network & feather_files_exist & load_from_feather:
        # Load data from first feather file found
        StagingMainLogger.info("Skipping data validation. Loading from feather")
        snapdata = read_feather(feather_file)
        StagingMainLogger.info(f"{feather_file} loaded")
        READ_FROM_FEATHER = True
    else:
        StagingMainLogger.info("Loading SPP snapshot data from json file")
        # Load data from JSON file
        snapdata = load_json(snapshot_path)

        contributors_df, responses_df = spp_parser.parse_snap_data(snapdata)

        # the anonymised snapshot data we use in the DevTest environment
        # does not include the instance column. This fix should be removed
        # when new anonymised data is given.
        if network_or_hdfs == "hdfs" and config["global"]["dev_test"]:
            responses_df["instance"] = 0
            # In the anonymised data the selectiontype is always 'L'
            col_size = contributors_df.shape[0]
            random.seed(seed=42)
            contributors_df["selectiontype"] = random.choice(
                ["P", "C", "L"], size=col_size
            )
            cellno_list = config["devtest"]["seltype_list"]
            contributors_df["cellnumber"] = random.choice(cellno_list, size=col_size)
        StagingMainLogger.info("Finished Data Ingest...")

        val.validate_data_with_schema(
            contributors_df, "./config/contributors_schema.toml"
        )
        val.validate_data_with_schema(responses_df, "./config/long_response.toml")

        # Data Transmutation
        StagingMainLogger.info("Starting Data Transmutation...")
        full_responses = processing.full_responses(contributors_df, responses_df)
        StagingMainLogger.info(
            "Finished Data Transmutation and validation of full responses dataframe"
        )

        # Validate and force data types for the full responses df
        # TODO Find a fix for the datatype casting before uncommenting
        val.combine_schemas_validate_full_df(
            full_responses,
            "./config/contributors_schema.toml",
            "./config/wide_responses.toml",
        )

        # Write feather file to snapshot path
        if is_network:
            feather_file = os.path.join(feather_path, f"{snapshot_name}.feather")
            write_feather(feather_file, full_responses)
        READ_FROM_FEATHER = False

    if READ_FROM_FEATHER:
        contributors_df = read_feather(
            os.path.join(feather_path, f"{snapshot_name}_contributors.feather")
        )
        responses_df = read_feather(
            os.path.join(feather_path, f"{snapshot_name}_responses.feather")
        )
        full_responses = snapdata

    # Get response rate
    processing.response_rate(contributors_df, responses_df)

    # Data validation
    val.check_data_shape(full_responses)

    # Stage, validate and harmonise the postcode column
    StagingMainLogger.info("Starting PostCode Validation")
    postcode_masterlist = paths["postcode_masterlist"]
    check_file_exists(postcode_masterlist)
    postcode_df = read_csv(postcode_masterlist)
    postcode_masterlist = postcode_df["pcd2"]
    invalid_df, unreal_df = val.validate_post_col(
        full_responses, postcode_masterlist, config
    )
    StagingMainLogger.info("Saving Invalid Postcodes to File")
    pcodes_folder = paths["postcode_path"]
    tdate = datetime.now().strftime("%Y-%m-%d")
    invalid_filename = f"invalid_pattern_postcodes_{tdate}_v{run_id}.csv"
    unreal_filename = f"missing_postcodes_{tdate}_v{run_id}.csv"
    write_csv(f"{pcodes_folder}/{invalid_filename}", invalid_df)
    write_csv(f"{pcodes_folder}/{unreal_filename}", unreal_df)
    StagingMainLogger.info("Finished PostCode Validation")

    if config["global"]["load_manual_outliers"]:
        # Stage the manual outliers file
        StagingMainLogger.info("Loading Manual Outlier File")
        manual_path = paths["manual_outliers_path"]
        check_file_exists(manual_path)
        wanted_cols = ["reference", "manual_outlier"]
        manual_outliers = read_csv(manual_path, wanted_cols)
        val.validate_data_with_schema(
            manual_outliers, "./config/manual_outliers_schema.toml"
        )
        StagingMainLogger.info("Manual Outlier File Loaded Successfully...")
    else:
        manual_outliers = None
        StagingMainLogger.info("Loading of Manual Outlier File skipped")

    # Load the PG mapper
    pg_mapper = paths["pg_mapper_path"]
    check_file_exists(pg_mapper)
    pg_mapper = read_csv(pg_mapper)

    # Map PG from SIC/PG numbers to column '201'.
    full_responses = pg.run_pg_conversion(full_responses, pg_mapper, target_col="201")

    # Load cora mapper
    StagingMainLogger.info("Loading Cora status mapper file")
    cora_mapper_path = paths["cora_mapper_path"]
    check_file_exists(cora_mapper_path)
    cora_mapper = read_csv(cora_mapper_path)
    # validates and updates from int64 to string type
    val.validate_data_with_schema(cora_mapper, "./config/cora_schema.toml")
    cora_mapper = val.validate_cora_df(cora_mapper)
    StagingMainLogger.info("Cora status mapper file loaded successfully...")

    # Load ultfoc (Foreign Ownership) mapper
    StagingMainLogger.info("Loading Foreign Ownership File")
    ultfoc_mapper_path = paths["ultfoc_mapper_path"]
    check_file_exists(ultfoc_mapper_path)
    ultfoc_mapper = read_csv(ultfoc_mapper_path)
    val.validate_data_with_schema(ultfoc_mapper, "./config/ultfoc_schema.toml")
    val.validate_ultfoc_df(ultfoc_mapper)
    StagingMainLogger.info("Foreign Ownership mapper file loaded successfully...")

    # Load itl mapper
    StagingMainLogger.info("Loading ITL File")
    itl_mapper_path = paths["itl_path"]
    check_file_exists(itl_mapper_path)
    itl_mapper = read_csv(itl_mapper_path)
    val.validate_data_with_schema(itl_mapper, "./config/itl_schema.toml")
    StagingMainLogger.info("ITL File Loaded Successfully...")

    # Loading cell number covarege
    StagingMainLogger.info("Loading Cell Covarage File...")
    cellno_path = paths["cellno_path"]
    check_file_exists(cellno_path)
    cellno_df = read_csv(cellno_path)
    StagingMainLogger.info("Covarage File Loaded Successfully...")

    # Loading PG numeric to alpha mapper
    StagingMainLogger.info("Loading PG numeric to alpha File...")
    pg_alpha_num_path = paths["pg_alpha_num_path"]
    check_file_exists(pg_alpha_num_path)
    pg_alpha_num = read_csv(pg_alpha_num_path)
    val.validate_data_with_schema(pg_alpha_num, "./config/pg_alpha_num_schema.toml")
    StagingMainLogger.info("PG numeric to alpha File Loaded Successfully...")

    # Loading PG detailed mapper
    StagingMainLogger.info("Loading PG detailed mapper File...")
    pg_detailed_path = paths["pg_detailed_path"]
    check_file_exists(pg_detailed_path)
    pg_detailed = read_csv(pg_detailed_path)
    val.validate_data_with_schema(pg_detailed, "./config/pg_detailed_schema.toml")
    StagingMainLogger.info("PG detailed mapper File Loaded Successfully...")

    # Loading ITL1 detailed mapper
    StagingMainLogger.info("Loading ITL1 detailed mapper File...")
    itl1_detailed_path = paths["itl1_detailed_path"]
    check_file_exists(itl1_detailed_path)
    itl1_detailed = read_csv(itl1_detailed_path)
    val.validate_data_with_schema(itl1_detailed, "./config/itl1_detailed_schema.toml")
    StagingMainLogger.info("ITL1 detailed mapper File Loaded Successfully...")

    # Output the staged BERD data for BaU testing when on local network.
    if config["global"]["output_full_responses"]:
        StagingMainLogger.info("Starting output of staged BERD data...")
        staging_folder = paths["staging_output_path"]
        tdate = datetime.now().strftime("%Y-%m-%d")
        staged_filename = f"staged_BERD_full_responses_{tdate}_v{run_id}.csv"
        write_csv(f"{staging_folder}/{staged_filename}", full_responses)
        StagingMainLogger.info("Finished output of staged BERD data.")
    else:
        StagingMainLogger.info("Skipping output of staged BERD data...")

    return (
        full_responses,
        manual_outliers,
        pg_mapper,
        ultfoc_mapper,
        itl_mapper,
        cora_mapper,
        cellno_df,
        postcode_df,
        pg_alpha_num,
        pg_detailed,
        itl1_detailed,
    )
