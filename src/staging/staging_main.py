"""The main file for the staging and validation module."""
import logging
from numpy import random
from typing import Callable, Tuple
from datetime import datetime
import pandas as pd
import os

from src.staging import spp_parser, history_loader
from src.staging import spp_snapshot_processing as processing
from src.staging import validation as val
from src.staging import pg_conversion as pg
from src.utils.wrappers import time_logger_wrap

StagingMainLogger = logging.getLogger(__name__)


def load_historic_data(config: dict, paths: dict, read_csv: Callable) -> dict:
    """Load historic data into the pipeline.

    Args:
        config (dict): The pipeline configuration
        paths (dict): The paths to the data files
        read_csv (Callable): Function to read a csv file.
            This will be the hdfs or network version depending on settings.

    Returns:
        dict: A dictionary of history data loaded into the pipeline.
    """
    curent_year = config["years"]["current_year"]
    years_to_load = config["years"]["previous_years_to_load"]
    years_gen = history_loader.history_years(curent_year, years_to_load)

    if years_gen is None:
        StagingMainLogger.info("No historic data to load for this run.")
        return {}
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

    return dict_of_hist_dfs if dict_of_hist_dfs else {}


def check_snapshot_feather_exists(
    config: dict,
    check_file_exists: Callable,
    feather_file_to_check,
    secondary_feather_file,
) -> bool:
    """Check if one or both of snapshot feather files exists.

    Conifg arguments decide whether to check for one or both.

    Args:
        config (dict): The pipeline configuration
        check_file_exists (Callable): Function to check if file exists
            This will be the hdfs or network version depending on settings.

    Returns:
        bool: True if the feather file exists, False otherwise.
    """

    if config["global"]["load_updated_snapshot"]:
        return check_file_exists(feather_file_to_check) and check_file_exists(
            secondary_feather_file
        )
    else:
        return check_file_exists(feather_file_to_check)


@time_logger_wrap
def load_snapshot_feather(feather_file, read_feather):
    snapdata = read_feather(feather_file)
    StagingMainLogger.info(f"{feather_file} loaded")
    return snapdata


def fix_anon_data(responses_df, config):
    """
    Fixes anonymised snapshot data for use in the DevTest environment.

    This function adds an "instance" column to the provided DataFrame, and populates
    it with zeros. It also adds a "selectiontype" column with random values of "P",
    "C", or "L", and a "cellnumber" column with random values from the "seltype_list"
    in the configuration.

    This fix is necessary because the anonymised snapshot data currently used in the
    DevTest environment does not include the "instance" column. This fix should be
    removed when new anonymised data is provided.

    Args:
        responses_df (pandas.DataFrame): The DataFrame containing the anonymised
        snapshot data.
        config (dict): A dictionary containing configuration details.

    Returns:
        pandas.DataFrame: The fixed DataFrame with the added "instance",
        "selectiontype", and "cellnumber" columns.
    """
    responses_df["instance"] = 0
    col_size = responses_df.shape[0]
    random.seed(seed=42)
    responses_df["selectiontype"] = random.choice(["P", "C", "L"], size=col_size)
    cellno_list = config["devtest"]["seltype_list"]
    responses_df["cellnumber"] = random.choice(cellno_list, size=col_size)
    return responses_df


def load_val_snapshot_json(snapshot_path, load_json, config, network_or_hdfs):

    StagingMainLogger.info("Loading SPP snapshot data from json file")

    # Load data from JSON file
    snapdata = load_json(snapshot_path)

    contributors_df, responses_df = spp_parser.parse_snap_data(snapdata)

    # Get response rate
    res_rate = "{:.2f}".format(processing.response_rate(contributors_df, responses_df))

    # the anonymised snapshot data we use in the DevTest environment
    # does not include the instance column. This fix should be removed
    # when new anonymised data is given.
    if network_or_hdfs == "hdfs" and config["global"]["dev_test"]:
        responses_df = fix_anon_data(responses_df, config)
    StagingMainLogger.info("Finished Data Ingest...")

    # Validate snapshot data
    val.validate_data_with_schema(contributors_df, "./config/contributors_schema.toml")
    val.validate_data_with_schema(responses_df, "./config/long_response.toml")

    # Data Transmutation
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

    return full_responses, res_rate


def load_validate_secondary_snapshot(load_json, secondary_snapshot_path):

    # Load secondary snapshot data
    StagingMainLogger.info("Loading secondary snapshot data from json file")
    secondary_snapdata = load_json(secondary_snapshot_path)

    # Parse secondary snapshot data
    secondary_contributors_df, secondary_responses_df = spp_parser.parse_snap_data(
        secondary_snapdata
    )

    # Validate secondary snapshot data
    StagingMainLogger.info("Validating secondary snapshot data...")
    val.validate_data_with_schema(
        secondary_contributors_df, "./config/contributors_schema.toml"
    )
    val.validate_data_with_schema(secondary_responses_df, "./config/long_response.toml")

    # Create secondary full responses dataframe
    secondary_full_responses = processing.full_responses(
        secondary_contributors_df, secondary_responses_df
    )
    # Validate and force data types for the secondary full responses df
    val.combine_schemas_validate_full_df(
        secondary_full_responses,
        "./config/contributors_schema.toml",
        "./config/wide_responses.toml",
    )

    # return secondary_full_responses
    return secondary_full_responses


def write_snapshot_to_feather(
    feather_dir_path: str,
    snapshot_name: str,
    full_responses: pd.DataFrame,
    write_feather,
    secondary_snapshot_name: str,
    secondary_full_responses: pd.DataFrame = None,
) -> None:
    """
    Writes the provided DataFrames to feather files.

    This function writes the `full_responses` DataFrame to a feather file named
    "{snapshot_name}_corrected.feather", and the `secondary_full_responses`
    DataFrame to a feather file named "{secondary_snapshot_name}.feather".
    Both files are written to the provided `feather_path`.

    Args:
        feather_path (str): The path where the feather files will be written.
        snapshot_name (str): The name of the snapshot for the `full_responses`
        DataFrame.
        full_responses (pd.DataFrame): The DataFrame to write to the
        "{snapshot_name}_corrected.feather" file.
        secondary_snapshot_name (str): The name of the snapshot for the
        `secondary_full_responses` DataFrame.
        secondary_full_responses (pd.DataFrame): The DataFrame to write to the
        "{secondary_snapshot_name}.feather" file.
    """

    feather_file = f"{snapshot_name}_corrected.feather"
    feather_file_path = os.path.join(feather_dir_path, feather_file)
    write_feather(feather_file_path, full_responses)
    StagingMainLogger.info(f"Written {feather_file} to {feather_dir_path}")

    if secondary_full_responses is not None:
        secondary_feather_file = os.path.join(
            feather_dir_path, f"{secondary_snapshot_name}.feather"
        )
        write_feather(secondary_feather_file, secondary_full_responses)
        StagingMainLogger.info(
            f"Written {secondary_snapshot_name}.feather to {feather_dir_path}"
        )


def stage_validate_harmonise_postcodes(
    config, paths, full_responses, run_id, check_file_exists, read_csv, write_csv
):
    """
    Stage, validate and harmonise the postcode column
    """
    StagingMainLogger.info("Starting PostCode Validation")
    postcode_masterlist = paths["postcode_masterlist"]
    check_file_exists(postcode_masterlist, raise_error=True)
    postcode_mapper = read_csv(postcode_masterlist)
    postcode_masterlist = postcode_mapper["pcd2"]
    invalid_df = val.validate_post_col(full_responses, postcode_masterlist, config)
    StagingMainLogger.info("Saving Invalid Postcodes to File")
    pcodes_folder = paths["postcode_path"]
    tdate = datetime.now().strftime("%Y-%m-%d")
    invalid_filename = f"invalid_unrecognised_postcodes_{tdate}_v{run_id}.csv"
    write_csv(f"{pcodes_folder}/{invalid_filename}", invalid_df)
    StagingMainLogger.info("Finished PostCode Validation")

    return full_responses, postcode_mapper


def run_staging(
    config: dict,
    check_file_exists: Callable,
    load_json: Callable,
    read_csv: Callable,
    write_csv: Callable,
    read_feather: Callable,
    write_feather: Callable,
    isfile: Callable,
    list_files: Callable,
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
            secondary_full_responses (pd.Dataframe): The staged and validated updated
            snapshot data
            manual_outliers (pd.DataFrame): Data with column for manual outliers,
            ultfoc_mapper (pd.DataFrame): Foreign ownership mapper,
            cora_mapper (pd.DataFrame): CORA status mapper,
            cellno_df (pd.DataFrame): Cell numbers mapper,
            postcode_mapper (pd.DataFrame): Postcodes to Regional Code mapper,
            pg_alpha_num (pd.DataFrame): Product group alpha to numeric mapper.
            pg_num_alpha (pd.DataFrame): Product group numeric to alpha mapper.
            sic_pg_alpha (pd.DataFrame): SIC code to product group alpha mapper.
    """
    # Check the environment switch
    network_or_hdfs = config["global"]["network_or_hdfs"]

    # Conditionally load paths
    paths = config[f"{network_or_hdfs}_paths"]
    snapshot_path = paths["snapshot_path"]
    snapshot_name = os.path.basename(snapshot_path).split(".", 1)[0]
    secondary_snapshot_path = paths["secondary_snapshot_path"]
    secondary_snapshot_name = os.path.basename(secondary_snapshot_path).split(".", 1)[0]
    feather_path = paths["feather_path"]
    feather_file = os.path.join(feather_path, f"{snapshot_name}_corrected.feather")
    secondary_feather_file = os.path.join(
        feather_path, f"{secondary_snapshot_name}.feather"
    )

    # Config settings for staging
    is_network = network_or_hdfs == "network"
    load_from_feather = config["global"]["load_from_feather"]
    load_updated_snapshot = config["global"]["load_updated_snapshot"]

    # Load historic data
    if config["global"]["load_historic_data"]:
        dict_of_hist_dfs = load_historic_data(config, paths, read_csv)
        print(dict_of_hist_dfs)

    # Check if the if the snapshot feather and optionally the secondary
    # snapshot feather exist
    feather_files_exist = check_snapshot_feather_exists(
        config, check_file_exists, feather_file, secondary_feather_file
    )

    # Only read from feather if feather files exist and we are on network
    READ_FROM_FEATHER = is_network & feather_files_exist & load_from_feather
    if READ_FROM_FEATHER:
        # Load data from first feather file found
        StagingMainLogger.info("Skipping data validation. Loading from feather")
        full_responses = load_snapshot_feather(feather_file, read_feather)
        if load_updated_snapshot:
            secondary_full_responses = load_snapshot_feather(
                secondary_feather_file, read_feather
            )
        else:
            secondary_full_responses = None

        # Read in postcode mapper (needed later in the pipeline)
        postcode_masterlist = paths["postcode_masterlist"]
        check_file_exists(postcode_masterlist, raise_error=True)
        postcode_mapper = read_csv(postcode_masterlist)

    else:  # Read from JSON

        # Check data file exists, raise an error if it does not.
        check_file_exists(snapshot_path, raise_error=True)
        full_responses, response_rate = load_val_snapshot_json(
            snapshot_path, load_json, config, network_or_hdfs
        )

        print(response_rate)  # TODO: We might want to use this in a QA output

        # Data validation of json or feather data
        val.check_data_shape(full_responses, raise_error=True)

        # Validate the postcodes in data loaded from JSON
        full_responses, postcode_mapper = stage_validate_harmonise_postcodes(
            config,
            paths,
            full_responses,
            run_id,
            check_file_exists,
            read_csv,
            write_csv,
        )

        if load_updated_snapshot:
            secondary_full_responses = load_validate_secondary_snapshot(
                load_json,
                secondary_snapshot_path,
            )
        else:
            secondary_full_responses = None

        # Write both snapshots to feather file at given path
        if is_network:
            write_snapshot_to_feather(
                feather_path,
                snapshot_name,
                full_responses,
                write_feather,
                secondary_snapshot_name,
                secondary_full_responses,
            )

    if config["global"]["load_manual_outliers"]:
        # Stage the manual outliers file
        StagingMainLogger.info("Loading Manual Outlier File")
        manual_path = paths["manual_outliers_path"]
        check_file_exists(manual_path, raise_error=True)
        wanted_cols = ["reference", "manual_outlier"]
        manual_outliers = read_csv(manual_path, wanted_cols)
        val.validate_data_with_schema(
            manual_outliers, "./config/manual_outliers_schema.toml"
        )
        StagingMainLogger.info("Manual Outlier File Loaded Successfully...")
    else:
        manual_outliers = None
        StagingMainLogger.info("Loading of Manual Outlier File skipped")

    # Get the latest manual trim file
    manual_trim_path = paths["manual_imp_trim_path"]

    if config["global"]["load_manual_imputation"] and isfile(manual_trim_path):
        StagingMainLogger.info("Loading Imputation Manual Trimming File")
        wanted_cols = ["reference", "instance", "manual_trim"]
        manual_trim_df = read_csv(manual_trim_path, wanted_cols)
        manual_trim_df["manual_trim"] = manual_trim_df["manual_trim"].fillna(False)
        val.validate_data_with_schema(
            manual_trim_df, "./config/manual_trimming_schema.toml"
        )
        # Fill empty values with False
    else:
        manual_trim_df = pd.DataFrame()  # Create and empty df
        manual_outliers = None
        StagingMainLogger.info("Loading of Imputation Manual Trimming File skipped")

    # Loading PG numeric to alpha mapper
    StagingMainLogger.info("Loading PG numeric to alpha File...")
    pg_num_alpha_path = paths["pg_num_alpha_path"]
    check_file_exists(pg_num_alpha_path, raise_error=True)
    pg_num_alpha = read_csv(pg_num_alpha_path)
    val.validate_data_with_schema(pg_num_alpha, "./config/pg_num_alpha_schema.toml")
    pg_num_alpha = val.validate_many_to_one(
        pg_num_alpha, col_many="pg_numeric", col_one="pg_alpha"
    )
    StagingMainLogger.info("PG numeric to alpha File Loaded Successfully...")

    if config["global"]["load_backdata"]:
        # Stage the manual outliers file
        StagingMainLogger.info("Loading Backdata File")
        backdata_path = paths["backdata_path"]
        check_file_exists(backdata_path, raise_error=True)
        backdata = read_csv(backdata_path)
        # To be added once schema is defined
        # val.validate_data_with_schema(
        #     backdata_path, "./config/backdata_schema.toml"
        # )

        # Fix for different column names on network vs hdfs
        if network_or_hdfs == "network":
            # Map PG numeric to alpha in column q201
            # This isn't done on HDFS as the column is already mapped
            backdata = pg.pg_to_pg_mapper(
                backdata,
                pg_num_alpha,
                target_col="q201",
                pg_column="q201",
            )
        StagingMainLogger.info("Backdata File Loaded Successfully...")
    else:
        backdata = None
        StagingMainLogger.info("Loading of Backdata File skipped")

    # Load cora mapper
    StagingMainLogger.info("Loading Cora status mapper file")
    cora_mapper_path = paths["cora_mapper_path"]
    check_file_exists(cora_mapper_path, raise_error=True)
    cora_mapper = read_csv(cora_mapper_path)
    # validates and updates from int64 to string type
    val.validate_data_with_schema(cora_mapper, "./config/cora_schema.toml")
    cora_mapper = val.validate_cora_df(cora_mapper)
    StagingMainLogger.info("Cora status mapper file loaded successfully...")

    # Load ultfoc (Foreign Ownership) mapper
    StagingMainLogger.info("Loading Foreign Ownership File")
    ultfoc_mapper_path = paths["ultfoc_mapper_path"]
    check_file_exists(ultfoc_mapper_path, raise_error=True)
    ultfoc_mapper = read_csv(ultfoc_mapper_path)
    val.validate_data_with_schema(ultfoc_mapper, "./config/ultfoc_schema.toml")
    val.validate_ultfoc_df(ultfoc_mapper)
    StagingMainLogger.info("Foreign Ownership mapper file loaded successfully...")

    # Load itl mapper
    StagingMainLogger.info("Loading ITL File")
    itl_mapper_path = paths["itl_path"]
    check_file_exists(itl_mapper_path, raise_error=True)
    itl_mapper = read_csv(itl_mapper_path)
    val.validate_data_with_schema(itl_mapper, "./config/itl_schema.toml")
    StagingMainLogger.info("ITL File Loaded Successfully...")

    # Loading cell number covarege
    StagingMainLogger.info("Loading Cell Covarage File...")
    cellno_path = paths["cellno_path"]
    check_file_exists(cellno_path, raise_error=True)
    cellno_df = read_csv(cellno_path)
    StagingMainLogger.info("Covarage File Loaded Successfully...")

    # # Loading PG alpha to numeric mapper - possibly, deprecated
    # StagingMainLogger.info("Loading PG alpha to numeric File...")
    # pg_alpha_num_path = paths["pg_alpha_num_path"]
    # check_file_exists(pg_alpha_num_path, raise_error=True)
    # pg_alpha_num = read_csv(pg_alpha_num_path)
    # val.validate_data_with_schema(pg_alpha_num, "./config/pg_alpha_num_schema.toml")
    # pg_alpha_num = val.validate_many_to_one(
    #     pg_alpha_num, col_many="pg_alpha", col_one="pg_numeric"
    # )
    # StagingMainLogger.info("PG numeric to alpha File Loaded Successfully...")

    # Loading SIC to PG to alpha mapper
    StagingMainLogger.info("Loading SIC to PG to alpha File...")
    sic_pg_alpha_path = paths["sic_pg_alpha_path"]
    check_file_exists(sic_pg_alpha_path, raise_error=True)
    sic_pg_alpha = read_csv(sic_pg_alpha_path)
    val.validate_data_with_schema(sic_pg_alpha, "./config/sic_pg_alpha_schema.toml")
    sic_pg_alpha = val.validate_many_to_one(
        sic_pg_alpha, col_many="sic", col_one="pg_alpha"
    )
    StagingMainLogger.info("PG numeric to alpha File Loaded Successfully...")

    # Loading SIC to PG UTF mapper
    StagingMainLogger.info("Loading SIC to PG UTF File...")
    sic_pg_utf_path = paths["sic_pg_utf_path"]
    check_file_exists(sic_pg_utf_path, raise_error=True)
    sic_pg_utf = read_csv(sic_pg_utf_path)
    cols_needed = ["SIC 2007_CODE", "2016 > Form PG"]
    sic_pg_num = sic_pg_utf[cols_needed]
    mapper_path = paths["mapper_path"]
    write_csv(f"{mapper_path}/sic_pg_num.csv", sic_pg_num)
    val.validate_data_with_schema(sic_pg_num, "./config/sic_pg_num_schema.toml")
    sic_pg_num = val.validate_many_to_one(
        sic_pg_num, col_many="SIC 2007_CODE", col_one="2016 > Form PG"
    )
    StagingMainLogger.info("SIC to PG UTF File Loaded Successfully...")

    # Map PG from SIC/PG numbers to column '201'.
    full_responses = pg.run_pg_conversion(
        full_responses, pg_num_alpha, sic_pg_alpha, target_col="201"
    )

    # Loading PG detailed mapper
    StagingMainLogger.info("Loading PG detailed mapper File...")
    pg_detailed_path = paths["pg_detailed_path"]
    check_file_exists(pg_detailed_path, raise_error=True)
    pg_detailed = read_csv(pg_detailed_path)
    val.validate_data_with_schema(pg_detailed, "./config/pg_detailed_schema.toml")
    StagingMainLogger.info("PG detailed mapper File Loaded Successfully...")

    # Loading ITL1 detailed mapper
    StagingMainLogger.info("Loading ITL1 detailed mapper File...")
    itl1_detailed_path = paths["itl1_detailed_path"]
    check_file_exists(itl1_detailed_path, raise_error=True)
    itl1_detailed = read_csv(itl1_detailed_path)
    val.validate_data_with_schema(itl1_detailed, "./config/itl1_detailed_schema.toml")
    StagingMainLogger.info("ITL1 detailed mapper File Loaded Successfully...")

    # Loading ru_817_list mapper
    load_ref_list_mapper = config["global"]["load_reference_list"]
    if load_ref_list_mapper:
        StagingMainLogger.info("Loading the reference list mapper file...")
        ref_list_817_path = paths["ref_list_817_path"]
        check_file_exists(ref_list_817_path, raise_error=True)
        ref_list_817 = read_csv(ref_list_817_path)
        schema_path = "./config/reference_list_schema.toml"
        # check_file_exists(schema_path, raise_error=True)
        val.validate_data_with_schema(ref_list_817, schema_path)
        StagingMainLogger.info("reference list mapper File Loaded Successfully...")
        # update longform references that should be on the reference list
        full_responses = val.update_ref_list(full_responses, ref_list_817)
    else:
        StagingMainLogger.info("Skipping loding the reference list mapper File.")

    # Loading Civil or Defence detailed mapper
    StagingMainLogger.info("Loading Civil/Defence detailed mapper File...")
    civil_defence_detailed_path = paths["civil_defence_detailed_path"]
    check_file_exists(civil_defence_detailed_path, raise_error=True)
    civil_defence_detailed = read_csv(civil_defence_detailed_path)
    StagingMainLogger.info("Civil/Defence detailed mapper File Loaded Successfully...")

    # Loading SIC division detailed mapper
    StagingMainLogger.info("Loading SIC division detailed mapper File...")
    sic_division_detailed_path = paths["sic_division_detailed_path"]
    check_file_exists(sic_division_detailed_path, raise_error=True)
    sic_division_detailed = read_csv(sic_division_detailed_path)
    StagingMainLogger.info("SIC division detailed mapper File Loaded Successfully...")

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

    # If we didn't load a snapshot, leave the df as null
    if not load_updated_snapshot:
        secondary_full_responses = None

    return (
        full_responses,
        secondary_full_responses,
        manual_outliers,
        ultfoc_mapper,
        itl_mapper,
        cora_mapper,
        cellno_df,
        postcode_mapper,
        pg_num_alpha,
        sic_pg_alpha,
        sic_pg_num,
        backdata,
        pg_detailed,
        itl1_detailed,
        civil_defence_detailed,
        sic_division_detailed,
        manual_trim_df,
    )
