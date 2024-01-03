import pandas as pd
from numpy import random
import logging
import re
from datetime import datetime
from typing import Callable

from src.utils.wrappers import time_logger_wrap
from src.staging import validation as val
from src.staging import spp_parser, history_loader
from src.staging import spp_snapshot_processing as processing


# Create logger for this module
StagingHelperLogger = logging.getLogger(__name__)


def postcode_topup(mystr: str, target_len: int = 8) -> str:
    """Regulates the number of spaces between the first and the second part of
    a postcode, so that the total length is 8 characters.
    Brings all letters to upper case, in line with the mapper.
    Splits the postcode string into parts, separated by any number of spaces.
    If there are two or more parts, the first two parts are used.
    The third and following parts, if present, are ignored.
    Calculates how many spaces are needed so that the total length is 8.
    If the total length of part 1 and part 2 is already 8, no space will be
    inserted.
    If their total length is more than 8, joins part 1 and part 2 without
    spaces and cuts the tail on the right.
    If there is only one part, keeps the first 8 characters and tops it up with
    spaces on the right if needed.
    Empty input string would have zero parts and will return a string of
    eight spaces.

    Args:
        mystr (str): Input postcode.
        target_len (int): The desired length of the postcode after topping up.

    Returns:
        str: The postcode topped up to the desired number of characters.
    """
    if pd.notna(mystr):
        mystr = mystr.upper()
        parts = mystr.split()

        if len(parts) == 1:
            mystr = mystr.strip()
            part1 = mystr[:-3]
            part2 = mystr[-3:]

            num_spaces = target_len - len(part1) - len(part2)
            if num_spaces >= 0:
                return part1 + " " * num_spaces + part2
            else:
                return (part1 + part2)[:target_len]

        elif len(parts) >= 2:
            part1 = parts[0]
            part2 = parts[1]

            num_spaces = target_len - len(part1) - len(part2)
            if num_spaces >= 0:
                return part1 + " " * num_spaces + part2
            else:
                return (part1 + part2)[:target_len]

        else:
            return mystr[:target_len].ljust(target_len, " ")



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


def update_ref_list(full_df: pd.DataFrame, ref_list_df: pd.DataFrame) -> pd.DataFrame:
    """
    Update long form references that should be on the reference list.

    For the first year (processing 2022 data) only, several references
    should have been designated on the "reference list", ie, should have been
    assigned cellnumber = 817, but were wrongly assigned a different cellnumber.

    Args:
        full_df (pd.DataFrame): The full_responses dataframe
        ref_list_df (pd.DataFrame): The mapper containing updates for the cellnumber
    Returns:
        df (pd.DataFrame): with cellnumber and selectiontype cols updated.
    """
    ref_list_filtered = ref_list_df.loc[
        (ref_list_df.formtype == 1) & (ref_list_df.cellnumber != 817)
    ]
    df = pd.merge(
        full_df,
        ref_list_filtered[["reference", "cellnumber"]],
        how="outer",
        on="reference",
        suffixes=("", "_new"),
        indicator=True,
    )
    # check no items in the reference list mapper are missing from the full responses
    missing_refs = df.loc[df["_merge"] == "right_only"]
    if not missing_refs.empty:
        msg = (
            "The following references in the reference list mapper are not in the data:"
        )
        raise ValueError(msg + str(missing_refs.reference.unique()))

    # update cellnumber and selectiontype where there is a match
    match_cond = df["_merge"] == "both"
    df = df.copy()
    df.loc[match_cond, "cellnumber"] = 817
    df.loc[match_cond, "selectiontype"] = "L"

    df = df.drop(["_merge", "cellnumber_new"], axis=1)

    return df


def getmappername(mapper_path_key, split):
    """
    Extracts the mapper name from a given path key.

    This function uses a regular expression to extract the name of the mapper from the provided key. 
    The name is assumed to be the part of the key before the first underscore. 
    If the 'split' parameter is True, underscores in the name are replaced with spaces.

    Parameters:
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


def load_valdiate_mapper(
    mapper_path_key,
    paths,
    file_exists_func,
    read_csv_func,
    logger,
    val_with_schema_func: Callable,
    one_to_many_val_func: Callable,
    *args,
):
    """
    Loads a specified mapper, validates it using a schema and an optional validation function.

    This function first retrieves the path of the mapper from the provided paths dictionary using the mapper_path_key.
    It then checks if the file exists at the mapper path. If the file exists, it is read into a DataFrame.
    The DataFrame is then validated against a schema, which is located at a path constructed from the mapper name.
    If a validation function is provided, it is called with the DataFrame and any additional arguments.

    Args:
        mapper_path_key (str): The key to retrieve the mapper path from the paths dictionary.
        paths (dict): A dictionary containing paths.
        file_exists_func (Callable): A function to check if a file exists at a given path.
        read_csv_func (Callable): A function to read a CSV file into a DataFrame.
        logger (logging.Logger): A logger to log information and errors.
        val_with_schema_func (Callable): A function to validate a DataFrame against a schema.
        validation_func (Callable, optional): An optional function to perform additional validation on the DataFrame.
        *args: Additional arguments to pass to the validation function.

    Returns:
        pd.DataFrame: The loaded and validated mapper DataFrame.

    Raises:
        FileNotFoundError: If no file exists at the mapper path.
        ValidationError: If the DataFrame fails schema validation or the validation function.
    """
    # Get the path of the mapper from the paths dictionary
    mapper_path = paths[mapper_path_key]

    # Get the name of the mapper from the mapper path key
    mapper_name = getmappername(mapper_path_key, split=True)

    # Log the loading of the mapper
    logger.info(f"Loading {getmappername(mapper_path_key, split=True)} to File...")

    # Check if the file exists at the mapper path, raise an error if it doesn't
    file_exists_func(mapper_path, raise_error=True)

    # Read the file at the mapper path into a DataFrame
    mapper_df = read_csv_func(mapper_path)

    # Construct the path of the schema from the mapper name
    schema_prefix = "_".join(word for word in mapper_name.split() if word != "mapper")
    schema_path = f"./config/{schema_prefix}_schema.toml"

    # Validate the DataFrame against the schema
    val_with_schema_func(mapper_df, schema_path)

    # If a one-to-many validation function is provided, validate the DataFrame
    if one_to_many_val_func:
        # Prepend the DataFrame to the arguments
        args = (mapper_df,) + args
        # Call the validation function with the DataFrame and the other arguments
        one_to_many_val_func(*args)  # args include "col_many" and "col_one"

    # Log the successful loading of the mapper
    logger.info(f"{mapper_name} loaded successfully")

    # Return the loaded and validated DataFrame
    return mapper_df


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
        StagingHelperLogger.info("No historic data to load for this run.")
        return {}
    else:
        StagingHelperLogger.info("Loading historic data...")
        history_path = paths["history_path"]
        dict_of_hist_dfs = history_loader.load_history(
            years_gen, history_path, read_csv
        )
        # Check if it has loaded and is not empty
        if isinstance(dict_of_hist_dfs, dict) and bool(dict_of_hist_dfs):
            StagingHelperLogger.info(
                "Dictionary of history data: %s loaded into pipeline",
                ", ".join(dict_of_hist_dfs),
            )
            StagingHelperLogger.info("Historic data loaded.")
        else:
            StagingHelperLogger.warning(
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
    StagingHelperLogger.info(f"{feather_file} loaded")
    return snapdata


def load_val_snapshot_json(snapshot_path, load_json, config, network_or_hdfs):
    """
    Loads and validates a snapshot of survey data from a JSON file.

    This function reads a JSON file containing a snapshot of survey data, parses the 
        data into contributors and responses dataframes, calculates the response rate, 
        fixes any issues with anonymised data, validates the data against predefined 
        schemas, combines the contributors and responses dataframes into a full responses
        dataframe, and validates the full responses dataframe against a combined schema.

    Parameters:
        snapshot_path (str): The path to the JSON file containing the snapshot data.
        load_json (function): The function to use to load the JSON file.
        config (dict): A dictionary containing configuration options.
        network_or_hdfs (str): A string indicating whether the data is being loaded from a network or HDFS.

    Returns:
        tuple: A tuple containing the full responses dataframe and the response rate.
    """
    StagingHelperLogger.info("Loading SPP snapshot data from json file")

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
    StagingHelperLogger.info("Finished Data Ingest...")

    # Validate snapshot data
    val.validate_data_with_schema(contributors_df, "./config/contributors_schema.toml")
    val.validate_data_with_schema(responses_df, "./config/long_response.toml")

    # Data Transmutation
    full_responses = processing.full_responses(contributors_df, responses_df)

    StagingHelperLogger.info(
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
    """
    Loads and validates a secondary snapshot of survey data from a JSON file.

    This function reads a JSON file containing a secondary snapshot of survey data, 
    parses the data into contributors and responses dataframes, validates the data
    against predefined schemas, combines the contributors and responses dataframes into
    a full responses dataframe, and validates the full responses dataframe against a
    combined schema.

    Parameters:
        load_json (function): The function to use to load the JSON file.
        secondary_snapshot_path (str): The path to the JSON file containing the secondary
            snapshot data.

    Returns:
        pandas.DataFrame: A DataFrame containing the full responses from the secondary 
            snapshot.
    """
    # Load secondary snapshot data
    StagingHelperLogger.info("Loading secondary snapshot data from json file")
    secondary_snapdata = load_json(secondary_snapshot_path)

    # Parse secondary snapshot data
    secondary_contributors_df, secondary_responses_df = spp_parser.parse_snap_data(
        secondary_snapdata
    )

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
    StagingHelperLogger.info(f"Written {feather_file} to {feather_dir_path}")

    if secondary_full_responses is not None:
        secondary_feather_file = os.path.join(
            feather_dir_path, f"{secondary_snapshot_name}.feather"
        )
        write_feather(secondary_feather_file, secondary_full_responses)
        StagingHelperLogger.info(
            f"Written {secondary_snapshot_name}.feather to {feather_dir_path}"
        )


def stage_validate_harmonise_postcodes(
    config, paths, full_responses, run_id, check_file_exists, read_csv, write_csv
):
    """
    Stage, validate and harmonise the postcode column
    """
    StagingHelperLogger.info("Starting PostCode Validation")
    postcode_masterlist = paths["postcode_masterlist"]
    check_file_exists(postcode_masterlist, raise_error=True)
    postcode_mapper = read_csv(postcode_masterlist)
    postcode_masterlist = postcode_mapper["pcd2"]
    invalid_df = val.validate_post_col(full_responses, postcode_masterlist, config)
    StagingHelperLogger.info("Saving Invalid Postcodes to File")
    pcodes_folder = paths["postcode_path"]
    tdate = datetime.now().strftime("%Y-%m-%d")
    invalid_filename = f"invalid_unrecognised_postcodes_{tdate}_v{run_id}.csv"
    write_csv(f"{pcodes_folder}/{invalid_filename}", invalid_df)
    StagingHelperLogger.info("Finished PostCode Validation")

    return full_responses, postcode_mapper

