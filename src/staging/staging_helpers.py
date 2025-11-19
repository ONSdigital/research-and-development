"""All functions that are applied in staging_main.py"""

# Core imports
import pandas as pd
from numpy import random
import logging
import re
import os
from collections.abc import Callable
from rdsa_utils.typing import PathLike

# Our own modules
from src.staging import validation as val
from src.staging import postcode_validation as pcval
from src.staging import spp_snapshot_processing as processing
from src.staging import spp_parser
from src.utils.helpers import filename_amender
from src.mapping.mapping_helpers import mapper_null_checks

# Create logger for this module
StagingHelperLogger = logging.getLogger(__name__)


def fix_anon_data(responses_df: pd.DataFrame, config: dict) -> pd.DataFrame:
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


def sic_fixer(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Ensure all SIC-related columns are strings and zero-padded.

    Args:
        df (pd.DataFrame): The DataFrame containing SIC-related columns.
        config (dict): A dictionary containing configuration details.

    Returns:
        pd.DataFrame: The DataFrame with SIC-related columns as zero-padded strings.
    """
    sic_cols = config["staging"]["sic_cols"]
    for col in sic_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.zfill(5)

    return df


def getmappername(mapper_path_key: str, split: bool) -> str:
    """
    Extracts the mapper name from a given path key.

    This function uses a regular expression to extract the name of the mapper from the
    provided key.
    The name is assumed to be the part of the key before the first underscore.
    If the 'split' parameter is True, underscores in the name are replaced with spaces.

    Args:
    mapper_path_key (str): The key from which to extract the mapper name.
    split (bool): Whether to replace underscores in the name with spaces.

    Returns:
    str: The extracted mapper name.
    """
    patt = re.compile(r"^(.*?)_path")
    mapper_name = re.search(patt, mapper_path_key).group(1)

    if split:
        mapper_name = mapper_name.replace("_", " ")

    return mapper_name


def load_validate_mapper(
    mapper_path_key: str,
    config: dict,
    logger: logging.Logger,
    rd_file_exists: callable,
    rd_read_csv: callable,
    validate_cols: list = None,
) -> pd.DataFrame:
    """
    Loads a specified mapper, validates it using a schema and an optional
    validation function.

    This function first retrieves the path of the mapper from the provided config
    dictionary using the mapper_path_key. It then checks if the file exists at
    the mapper path. If the file exists, it is read into a DataFrame. The
    DataFrame is then validated against a schema, which is located at a path
    constructed from the mapper name. If a validation function is provided, it
    is called with the DataFrame and any additional arguments.

    Args:
        mapper_path_key (str): The key to retrieve the mapper path from the config.
        config (dict): A dictionary containing configuration options.
        logger (logging.Logger): A logger to log information and errors.
        rd_file_exists (callable): A platform-specific function that checks if a
            file exists in a certain path.
        rd_read_csv(callable): A platform-specific function that reads a csv
            file into a Pandas dataframe from a given path.
        validate_cols (list, optional): Checking for unexpected NULL values.

    Returns:
        pd.DataFrame: The loaded and validated mapper DataFrame.

    Raises:
        FileNotFoundError: If no file exists at the mapper path.
        ValidationError: If the DataFrame fails schema validation or the validation func
        Warning: If the DataFrame contains unexpected NULL values.
    """
    # Get the path of the mapper from the config dictionary
    mapper_path = config["mapping_paths"][mapper_path_key]

    # Get the name of the mapper from the mapper path key
    mapper_name = getmappername(mapper_path_key, split=True)

    # Log the loading of the mapper
    logger.info(f"Loading {getmappername(mapper_path_key, split=True)} from file...")

    # Check if the file exists at the mapper path, raise an error if it doesn't
    rd_file_exists(mapper_path, raise_error=True)

    # Read the file at the mapper path into a DataFrame
    mapper_df = rd_read_csv(mapper_path)

    # Construct the path of the schema from the mapper name
    schema_prefix = "_".join(word for word in mapper_name.split() if word != "mapper")
    schema_path = f"./config/{schema_prefix}_schema.toml"

    # Validate the DataFrame against the schema
    mapper_df = val.validate_data_with_schema(mapper_df, schema_path)

    # Perform null checks on the mapper DataFrame
    mapper_null_checks(mapper_df, mapper_name, validate_cols)

    # Log the successful loading of the mapper
    logger.info(f"{mapper_name} loaded successfully")

    # Return the loaded and validated DataFrame
    return mapper_df


def load_snapshot_feather(feather_file, read_feather):
    snapdata = read_feather(feather_file)
    StagingHelperLogger.info(f"{feather_file} loaded")
    return snapdata


def load_val_snapshot_json(
    snapshot_path: str,
    load_json: Callable,
    config: dict,
) -> tuple[pd.DataFrame, str]:
    """
    Loads and validates a snapshot of survey data from a JSON file.

    This function reads a JSON file containing a snapshot of survey data, parses
        the data into contributors and responses dataframes, calculates the
        response rate, fixes any issues with anonymised data, validates the data
        against predefined schemas, combines the contributors and responses
        dataframes into a full responses dataframe, and validates the full
        responses dataframe against a combined schema.

    Args:
        snapshot_path (str): The path to the JSON file containing the snapshot
        data.
        load_json (function): The function to use to load the JSON file.
        config (dict): A dictionary containing configuration options.
        loaded from a network or HDFS.

    Returns:
        tuple: A tuple containing the full responses dataframe and the response
        rate.
    """
    StagingHelperLogger.info("Loading SPP snapshot data from json file")

    # Load data from JSON file
    snapdata = load_json(snapshot_path)

    contributors_df, responses_df = spp_parser.parse_snap_data(snapdata)

    # Get response rate
    res_rate = "{:.2f}".format(processing.response_rate(contributors_df, responses_df))
    StagingHelperLogger.success("Finished Data Ingest...")

    # Validate snapshot data
    contributors_df = val.validate_data_with_schema(
        contributors_df, "./config/contributors_schema.toml"
    )
    responses_df = val.validate_data_with_schema(
        responses_df, "./config/long_response.toml"
    )

    if config["dev_global"]["platform"] == "s3" and config["dev_global"]["dev_test"]:
        responses_df["instance"] = 0

    # Data Transmutation
    full_responses = processing.full_responses(contributors_df, responses_df)

    # the anonymised snapshot data we use in the DevTest environment
    # does not include the instance column. This fix should be removed
    # when new anonymised data is given.
    # if config["dev_global"]["platform"] == "s3" and config["dev_global"]["dev_test"]:
    #     full_responses = fix_anon_data(full_responses, config)

    StagingHelperLogger.info(
        "Finished Data Transmutation and validation of full responses dataframe"
    )
    # Validate and force data types for the full responses df
    full_responses = val.combine_schemas_validate_full_df(
        full_responses,
        "./config/contributors_schema.toml",
        "./config/wide_responses.toml",
    )

    return full_responses, res_rate


def df_to_feather(
    dir: PathLike,
    save_name: str,
    df: pd.DataFrame,
    write_feather: Callable,
    overwrite: bool = True,
) -> None:
    """_summary_

    Args:
        dir (PathLike): The save directory of the feather file.
        save_name (str): The save name of the feather file.
        df (pd.DataFrame): The df to save out as a .feather file.
        write_feather (Callable): A function that write out a feather.
        overwrite (bool, optional): Whether or not to overwrite files saved
            under the same name. Defaults to True.

    Raises:
        FileNotFoundError: Raised if the passed directory does not exist.
        FileExistsError: Raised if overwrite=False and the file already exists.
    """
    # defences
    if not os.path.exists(dir):
        raise FileNotFoundError(f"The passed directory ({dir}) does not exist")
    fpath = os.path.join(dir, save_name)
    # ensure path is feather file
    if os.path.splitext(fpath)[1].lower() != ".feather":
        fpath = f"{fpath}.feather"
    if not overwrite and os.path.exists(fpath):
        raise FileExistsError(
            f"File already saved at {fpath}. Pass overwrite=True if you would "
            "like to overwrite it."
        )
    write_feather(fpath, df)


def stage_validate_harmonise_postcodes(
    config: dict,
    full_responses: pd.DataFrame,
    check_file_exists: Callable,
    read_csv: Callable,
    write_csv: Callable,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Stages, validates, and harmonises the postcode column in the provided
    DataFrame.

    This function performs the following steps:
    1. Loads a master list of postcodes from a CSV file.
    2. Validates the postcode column in the full_responses DataFrame against the
       master list.
    3. Writes any invalid postcodes to a CSV file.
    4. Returns the original DataFrame and the master list of postcodes.

    Args:
        config (dict): A dictionary containing configuration options.
        full_responses (pd.DataFrame): The DataFrame containing the data to be
        validated.
        check_file_exists (Callable): A function that checks if a file exists.
        read_csv (Callable): A function that reads a CSV file into a DataFrame.
        write_csv (Callable): A function that writes a DataFrame to a CSV file.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the original DataFrame
        and the master list of postcodes.
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the original DataFrame
        and the master list of postcodes.
    """
    # Log the start of postcode validation
    StagingHelperLogger.info("Starting PostCode Validation")

    staging_dict = config["staging_paths"]

    # Load the master list of postcodes
    postcode_mapper = config["mapping_paths"]["postcode_mapper"]
    check_file_exists(postcode_mapper, raise_error=True)
    postcode_mapper = read_csv(postcode_mapper)

    # Validate the postcode column in the full_responses DataFrame
    full_responses, invalid_df = pcval.run_full_postcode_process(
        full_responses, postcode_mapper, config
    )
    # Log the unique unreal postcodes
    bad_postcodes = invalid_df.incorrect_postcode.unique().tolist()
    StagingHelperLogger.warning(
        f"These postcodes are not found in the ONS postcode list: {bad_postcodes}"
    )
    StagingHelperLogger.warning(
        f"Number of postcodes not found in the ONS postcode list: {len(bad_postcodes)}"
    )

    # Filter invalid postcodes for BERD or PNP data
    invalid_df = filter_pnp_data(invalid_df, config)

    # Log the saving of invalid postcodes to a file
    StagingHelperLogger.info("Saving Invalid Postcodes to File")

    # Save the invalid postcodes to a CSV file
    pcodes_folder = staging_dict["pcode_val_path"]
    invalid_filename = filename_amender(filename="invalid_postcodes", config=config)
    write_csv(f"{pcodes_folder}/{invalid_filename}", invalid_df)

    # Log the end of postcode validation
    StagingHelperLogger.success("Finished PostCode Validation")

    return full_responses, postcode_mapper


def filter_pnp_data(full_responses, config):
    """
    Filter for either PNP data or BERD data.

    Args:
        full_responses (pandas.DataFrame):
            The DataFrame containing the full resonses data.

    Returns:
        pd.DataFrame:t PNP data or BERD data.
    """

    # filter out PNP data or equivalently records with legalstatus!='7'
    if config["survey"]["survey_type"] == "BERD":
        full_responses = full_responses.loc[(full_responses["legalstatus"] != "7")]

    # create dataframe with PNP data legalstatus=='7'
    elif config["survey"]["survey_type"] == "PNP":
        full_responses = full_responses.loc[(full_responses["legalstatus"] == "7")]

    return full_responses


def output_staging_qa(full_responses, config, rd_write_csv, StagingMainLogger):
    """
    Output full reponses staged data or skip output based on various config settings.

    Args:
        full_responses (pd.DataFrame): The staged data
        config (dict): The pipeline configuration
        rd_write_csv (Callable): Function to write to a csv file.
            Avaible in s3, hdfs or network version depending "
        StagingMainLogger (logging.Logger): The logger for the staging module.
    Returns:
        None
    """
    output_responses = config["global"]["output_full_responses"]
    run_with_frozen = config["global"]["run_with_frozen_data"]
    if output_responses and not run_with_frozen:
        survey_type = config["survey"]["survey_type"]
        StagingMainLogger.info(f"Starting output of staged {survey_type} data...")

        if survey_type == "PNP":
            staged_filename = filename_amender("staged_full_responses", config)
        else:
            staged_filename = filename_amender("staged_BERD_full_responses", config)

        staging_folder = config["staging_paths"]["staging_output_path"]
        rd_write_csv(f"{staging_folder}/{staged_filename}", full_responses)
        StagingMainLogger.info(f"Finished output of staged {survey_type} data.")
    else:
        StagingMainLogger.info("Skipping output of staged data...")
