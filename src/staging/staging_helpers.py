"""All functions that are applied in staging_main.py"""
# Core imports
import pandas as pd
from numpy import random
import logging
import re
import os
import pathlib
from datetime import datetime
from typing import Callable, Tuple, Dict, Union

# Our own modules
from src.staging import validation as val
from src.staging import postcode_validation as pcval
from src.staging import spp_snapshot_processing as processing
from src.staging import spp_parser

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
    mapper_path_key: str, config: dict, logger: logging.Logger
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

    Returns:
        pd.DataFrame: The loaded and validated mapper DataFrame.

    Raises:
        FileNotFoundError: If no file exists at the mapper path.
        ValidationError: If the DataFrame fails schema validation or the validation func
    """
    # Get the path of the mapper from the config dictionary
    mapper_path = config["mapping_paths"][mapper_path_key]
    platform = config["global"]["platform"]

    if platform == "network":
        from src.utils import local_file_mods as mods

    elif platform == "hdfs":
        from src.utils import hdfs_mods as mods

    # Get the name of the mapper from the mapper path key
    mapper_name = getmappername(mapper_path_key, split=True)

    # Log the loading of the mapper
    logger.info(f"Loading {getmappername(mapper_path_key, split=True)} from file...")

    # Check if the file exists at the mapper path, raise an error if it doesn't
    mods.rd_file_exists(mapper_path, raise_error=True)

    # Read the file at the mapper path into a DataFrame
    mapper_df = mods.rd_read_csv(mapper_path)

    # Construct the path of the schema from the mapper name
    schema_prefix = "_".join(word for word in mapper_name.split() if word != "mapper")
    schema_path = f"./config/{schema_prefix}_schema.toml"

    # Validate the DataFrame against the schema
    val.validate_data_with_schema(mapper_df, schema_path)

    # Log the successful loading of the mapper
    logger.info(f"{mapper_name} loaded successfully")

    # Return the loaded and validated DataFrame
    return mapper_df


def load_snapshot_feather(feather_file, read_feather):
    snapdata = read_feather(feather_file)
    StagingHelperLogger.info(f"{feather_file} loaded")
    return snapdata


def load_val_snapshot_json(
    frozen_snapshot_path: str, load_json: Callable, config: dict, platform: str
) -> Tuple[pd.DataFrame, str]:
    """
    Loads and validates a snapshot of survey data from a JSON file.

    This function reads a JSON file containing a snapshot of survey data, parses
        the data into contributors and responses dataframes, calculates the
        response rate, fixes any issues with anonymised data, validates the data
        against predefined schemas, combines the contributors and responses
        dataframes into a full responses dataframe, and validates the full
        responses dataframe against a combined schema.

    Args:
        frozen_snapshot_path (str): The path to the JSON file containing the snapshot
        data.
        load_json (function): The function to use to load the JSON file.
        config (dict): A dictionary containing configuration options.
        platform (str): A string indicating whether the data is being
        loaded from a network or HDFS.

    Returns:
        tuple: A tuple containing the full responses dataframe and the response
        rate.
    """
    StagingHelperLogger.info("Loading SPP snapshot data from json file")

    # Load data from JSON file
    snapdata = load_json(frozen_snapshot_path)

    contributors_df, responses_df = spp_parser.parse_snap_data(snapdata)

    # Get response rate
    res_rate = "{:.2f}".format(processing.response_rate(contributors_df, responses_df))
    StagingHelperLogger.info("Finished Data Ingest...")

    # Validate snapshot data
    # TODO: this temp switched off while working on dev_test_branch
    # val.validate_data_with_schema(contributors_df, "./config/contributors_schema.toml"
    # )
    # val.validate_data_with_schema(responses_df, "./config/long_response.toml")

    if platform == "hdfs" and config["global"]["dev_test"]:
        responses_df["instance"] = 0

    # Data Transmutation
    full_responses = processing.full_responses(contributors_df, responses_df)
    # the anonymised snapshot data we use in the DevTest environment
    # does not include the instance column. This fix should be removed
    # when new anonymised data is given.
    if platform == "hdfs" and config["global"]["dev_test"]:
        full_responses = fix_anon_data(full_responses, config)

    StagingHelperLogger.info(
        "Finished Data Transmutation and validation of full responses dataframe"
    )
    # Validate and force data types for the full responses df
    # TODO Find a fix for the datatype casting before uncommenting
    # TODO commented out for Dev test for now
    # val.combine_schemas_validate_full_df(
    #     full_responses,
    #     "./config/contributors_schema.toml",
    #     "./config/wide_responses.toml",
    # )

    return full_responses, res_rate


<<<<<<< HEAD
def load_validate_secondary_snapshot(
    load_json, secondary_snapshot_path, config, platform
):
    """
    Loads and validates a secondary snapshot of survey data from a JSON file.

    This function reads a JSON file containing a secondary snapshot of survey
    data, parses the data into contributors and responses dataframes, validates
    the data against predefined schemas, combines the contributors and responses
    dataframes into a full responses dataframe, and validates the full responses
    dataframe against a combined schema.

    Args:
        load_json (function): The function to use to load the JSON file.
        secondary_snapshot_path (str): The path to the JSON file containing the
        secondary snapshot data.

    Returns:
        pandas.DataFrame: A DataFrame containing the full responses from the
        secondary snapshot.
    """
    # Load secondary snapshot data
    StagingHelperLogger.info("Loading secondary snapshot data from json file")
    secondary_snapdata = load_json(secondary_snapshot_path)

    # Parse secondary snapshot data
    secondary_contributors_df, secondary_responses_df = spp_parser.parse_snap_data(
        secondary_snapdata
    )

    # applied fix as secondary responses does not include instance column:
    # already a fix in place for DevTest environment (see load_val_snapshot_json())
    if platform == "network":
        secondary_responses_df["instance"] = 0

    # Validate secondary snapshot data
    StagingHelperLogger.info("Validating secondary snapshot data...")
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


=======
>>>>>>> RDRP-966_remove_rd_open
def df_to_feather(
    dir: Union[pathlib.Path, str],
    save_name: str,
    df: pd.DataFrame,
    write_feather: Callable,
    overwrite: bool = True,
) -> None:
    """_summary_

    Args:
        dir (Union[pathlib.Path, str]): The save directory of the feather file.
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
    config: Dict,
    full_responses: pd.DataFrame,
    run_id: str,
    check_file_exists: Callable,
    read_csv: Callable,
    write_csv: Callable,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Stages, validates, and harmonises the postcode column in the provided
    DataFrame.

    This function performs the following steps:
    1. Loads a master list of postcodes from a CSV file.
    2. Validates the postcode column in the full_responses DataFrame against the
       master list.
    2. Validates the postcode column in the full_responses DataFrame against
        the master list.
    3. Writes any invalid postcodes to a CSV file.
    4. Returns the original DataFrame and the master list of postcodes.

    Args:
        config (Dict): A dictionary containing configuration options.
        full_responses (pd.DataFrame): The DataFrame containing the data to be
        validated.
        run_id (str): The run ID for this execution.
        check_file_exists (Callable): A function that checks if a file exists.
        read_csv (Callable): A function that reads a CSV file into a DataFrame.
        write_csv (Callable): A function that writes a DataFrame to a CSV file.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the original DataFrame
        and the master list of postcodes.
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the original DataFrame
        and the master list of postcodes.
    """
    # Log the start of postcode validation
    StagingHelperLogger.info("Starting PostCode Validation")

    staging_dict = config["staging_paths"]

    # Load the master list of postcodes
    postcode_mapper = config["mapping_paths"]["postcode_mapper"]
    check_file_exists(postcode_mapper, raise_error=True)
    postcode_mapper = read_csv(postcode_mapper)
    postcode_masterlist = postcode_mapper["pcd2"]

    # Validate the postcode column in the full_responses DataFrame
    full_responses, invalid_df = pcval.run_full_postcode_process(
        full_responses, postcode_masterlist, config
    )

    # Log the saving of invalid postcodes to a file
    StagingHelperLogger.info("Saving Invalid Postcodes to File")

    # Save the invalid postcodes to a CSV file
    pcodes_folder = staging_dict["pcode_val_path"]
    tdate = datetime.now().strftime("%y-%m-%d")
    survey_year = config["years"]["survey_year"]
    invalid_filename = (
        f"{survey_year}_invalid_unrecognised_postcodes_{tdate}_v{run_id}.csv"
    )
    write_csv(f"{pcodes_folder}/{invalid_filename}", invalid_df)

    # Log the end of postcode validation
    StagingHelperLogger.info("Finished PostCode Validation")

    return full_responses, postcode_mapper


def filter_pnp_data(full_responses):
    """
    Filter out all PNP data or equivalently all records with legalstatus of 7

    Args:
        full_responses (pandas.DataFrame):
            The DataFrame containing the full resonses data.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Two dataframes; the BERD data without
        PNP data and the PNP data
    """
    # create dataframe with PNP data legalstatus=='7'
    pnp_full_responses = full_responses.loc[(full_responses["legalstatus"] == "7")]
    # filter out PNP data or equivalently records with legalstatus!='7'
    full_responses = full_responses.loc[(full_responses["legalstatus"] != "7")]

    return full_responses, pnp_full_responses
