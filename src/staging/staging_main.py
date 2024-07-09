"""The main file for the staging and validation module."""
# Core imports
import logging
from typing import Callable, Tuple
from datetime import datetime
import os

# Our own modules
from src.staging import validation as val
import src.staging.staging_helpers as helpers

StagingMainLogger = logging.getLogger(__name__)


def run_staging(  # noqa: C901
    config: dict,
    check_file_exists: Callable,
    load_json: Callable,
    read_csv: Callable,
    write_csv: Callable,
    read_feather: Callable,
    write_feather: Callable,
    isfile: Callable,
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
        dict_of_hist_dfs = helpers.load_historic_data(config, paths, read_csv)
        print(dict_of_hist_dfs)

    # Check if the if the snapshot feather and optionally the secondary
    # snapshot feather exist
    feather_files_exist = helpers.check_snapshot_feather_exists(
        config, check_file_exists, feather_file, secondary_feather_file
    )

    # Only read from feather if feather files exist and we are on network
    READ_FROM_FEATHER = is_network & feather_files_exist & load_from_feather
    if READ_FROM_FEATHER:
        # Load data from first feather file found
        StagingMainLogger.info("Skipping data validation. Loading from feather")
        full_responses = helpers.load_snapshot_feather(feather_file, read_feather)
        # filter out PNP data legalstatus=7
        full_responses = helpers.filter_pnp_data(full_responses)
        if load_updated_snapshot:
            secondary_full_responses = helpers.load_snapshot_feather(
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
        full_responses, response_rate = helpers.load_val_snapshot_json(
            snapshot_path, load_json, config, network_or_hdfs
        )

        StagingMainLogger.info(
            f"Response rate: {response_rate}"
        )  # TODO: We might want to use this in a QA output

        # Data validation of json or feather data
        val.check_data_shape(full_responses, raise_error=True)

        # Validate the postcodes in data loaded from JSON
        full_responses, postcode_mapper = helpers.stage_validate_harmonise_postcodes(
            config,
            paths,
            full_responses,
            run_id,
            check_file_exists,
            read_csv,
            write_csv,
        )

        if load_updated_snapshot:
            secondary_full_responses = helpers.load_validate_secondary_snapshot(
                load_json,
                secondary_snapshot_path,
                config,
                network_or_hdfs,
            )
        else:
            secondary_full_responses = None

        # Write both snapshots to feather file at given path
        if is_network:
            feather_fname = f"{snapshot_name}_corrected.feather"
            s_feather_fname = f"{secondary_snapshot_name}.feather"
            helpers.df_to_feather(
                feather_path, feather_fname, full_responses, write_feather
            )
            if secondary_full_responses is not None:
                helpers.df_to_feather(
                    feather_path,
                    s_feather_fname,
                    secondary_full_responses,
                    write_feather,
                )

    # Flag invalid records
    val.flag_no_rand_spenders(full_responses, "raise")

    if config["global"]["load_manual_outliers"]:
        # Stage the manual outliers file
        StagingMainLogger.info("Loading Manual Outlier File")
        manual_path = paths["manual_outliers_path"]
        check_file_exists(manual_path, raise_error=True)
        wanted_cols = ["reference", "manual_outlier"]
        manual_outliers = read_csv(manual_path, wanted_cols)
        manual_outliers["manual_outlier"] = manual_outliers["manual_outlier"].fillna(
            False
        )
        manual_outliers = manual_outliers.drop_duplicates(
            subset=["reference"], keep="first"
        )
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
        manual_trim_df["instance"] = manual_trim_df["instance"].fillna(1)
        manual_trim_df = manual_trim_df.drop_duplicates(
            subset=["reference", "instance"], keep="first"
        )
        val.validate_data_with_schema(
            manual_trim_df, "./config/manual_trim_schema.toml"
        )
    else:
        manual_trim_df = None
        StagingMainLogger.info("Loading of Imputation Manual Trimming File skipped")

    if config["global"]["load_backdata"]:
        # Stage the manual outliers file
        StagingMainLogger.info("Loading Backdata File")
        backdata_path = paths["backdata_path"]
        check_file_exists(backdata_path, raise_error=True)
        backdata = read_csv(backdata_path)
        # To be added once schema is defined
        # Network schema file matches working schema on DAP
        # val.validate_data_with_schema(
        #     backdata_path, "./config/backdata_schema.toml"
        # )

        StagingMainLogger.info("Backdata File Loaded Successfully...")
    else:
        backdata = None
        StagingMainLogger.info("Loading of Backdata File skipped")

    # Loading ITL1 detailed mapper
    itl1_detailed_mapper = helpers.load_validate_mapper(
        "itl1_detailed_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        StagingMainLogger,
        val.validate_data_with_schema,
        None,
    )

    # Loading Civil or Defence detailed mapper
    civil_defence_detailed_mapper = helpers.load_validate_mapper(
        "civil_defence_detailed_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        StagingMainLogger,
        val.validate_data_with_schema,
        None,
    )

    # Loading SIC division detailed mapper
    sic_division_detailed_mapper = helpers.load_validate_mapper(
        "sic_division_detailed_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        StagingMainLogger,
        val.validate_data_with_schema,
        None,
    )

    pg_detailed_mapper = helpers.load_validate_mapper(
        "pg_detailed_mapper_path",
        paths,
        check_file_exists,
        read_csv,
        StagingMainLogger,
        val.validate_data_with_schema,
        None,
    )

    # Output the staged BERD data.
    if config["global"]["output_full_responses"]:
        StagingMainLogger.info("Starting output of staged BERD data...")
        staging_folder = paths["staging_output_path"]
        tdate = datetime.now().strftime("%y-%m-%d")
        survey_year = config["years"]["survey_year"]
        staged_filename = (
            f"{survey_year}_staged_BERD_full_responses_{tdate}_v{run_id}.csv"
        )
        write_csv(f"{staging_folder}/{staged_filename}", full_responses)
        StagingMainLogger.info("Finished output of staged BERD data.")
    else:
        StagingMainLogger.info("Skipping output of staged BERD data...")

    # Return staged BERD data, additional data and mappers
    return (
        full_responses,
        secondary_full_responses,
        manual_outliers,
        postcode_mapper,
        backdata,
        pg_detailed_mapper,
        itl1_detailed_mapper,
        civil_defence_detailed_mapper,
        sic_division_detailed_mapper,
        manual_trim_df,
    )
