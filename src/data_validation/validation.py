import os
import toml
import postcodes_uk
import pandas as pd

import logging
from src.utils.wrappers import time_logger_wrap, exception_wrap
from src.utils.helpers import Config_settings

# Get the config
conf_obj = Config_settings()
config = conf_obj.config_dict
global_config = config["global"]

# Set up logging
validation_logger = logging.getLogger(__name__)


def validate_postcode_pattern(pcode: str) -> bool:
    """A function to validate UK postcodes which uses the

    Args:
        pcode (str): The postcode to validate

    Returns:
        bool: True or False depending on if it is valid or not
    """
    if pcode is None:
        return False

    # Validation step
    valid_bool = postcodes_uk.validate(pcode)

    return valid_bool


@exception_wrap
def get_masterlist(postcode_masterlist) -> pd.Series:
    """This function loads the masterlist of postcodes from a csv file

    Returns:
        pd.Series: The dataframe of postcodes
    """
    masterlist = pd.read_csv(postcode_masterlist, usecols=["pcd"]).squeeze()
    return masterlist


@time_logger_wrap
@exception_wrap
def validate_post_col(df: pd.DataFrame, postcode_masterlist: str) -> bool:
    """This function checks if all postcodes in the specified DataFrame column
        are valid UK postcodes. It uses the `validate_postcode` function to
        perform the validation.

    Args:
        df (pd.DataFrame): The DataFrame containing the postcodes.

    Returns:
        A bool: boolean, True if number of columns is as expected, otherwise False
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"The dataframe you are attempting to validate is {type(df)}")

    unreal_postcodes = check_pcs_real(df, postcode_masterlist)

    # Log the unreal postcodes
    if not unreal_postcodes.empty:
        validation_logger.warning(
            f"These postcodes are not found in the ONS postcode list: {unreal_postcodes.to_list()}"  # noqa
        )

    # Check if postcodes match pattern
    invalid_pattern_postcodes = df.loc[
        ~df["referencepostcode"].apply(validate_postcode_pattern), "referencepostcode"
    ]

    # Log the invalid postcodes
    if not invalid_pattern_postcodes.empty:
        validation_logger.warning(
            f"Invalid pattern postcodes found: {invalid_pattern_postcodes.to_list()}"
        )

    # Combine the two lists
    combined_invalid_postcodes = pd.concat(
        [unreal_postcodes, invalid_pattern_postcodes]
    )
    combined_invalid_postcodes.drop_duplicates(inplace=True)

    if not combined_invalid_postcodes.empty:
        raise ValueError(
            f"Invalid postcodes found: {combined_invalid_postcodes.to_list()}"
        )

    validation_logger.info("All postcodes validated....")

    return True


def check_pcs_real(df: pd.DataFrame, postcode_masterlist: str):
    """Checks if the postcodes are real against a masterlist of actual postcodes"""
    if config["global"]["postcode_csv_check"]:
        master_series = get_masterlist(postcode_masterlist)

        # Check if postcode are real
        unreal_postcodes = df.loc[
            ~df["referencepostcode"].isin(master_series), "referencepostcode"
        ]
    else:
        emptydf = pd.DataFrame(columns=["referencepostcode"])
        unreal_postcodes = emptydf.loc[
            ~emptydf["referencepostcode"], "referencepostcode"
        ]

    return unreal_postcodes


@exception_wrap
def load_schema(file_path: str = "./config/contributors_schema.toml") -> dict:
    """Load the data schema from toml file into a dictionary

    Keyword Arguments:
        file_path -- Path to data schema toml file
        (default: {"./config/contributors_schema.toml"})

    Returns:
        A dict: dictionary containing parsed schema toml file
    """
    # Create bool variable for checking if file exists
    file_exists = os.path.exists(file_path)

    # Check if Data_Schema.toml exists
    if file_exists:
        # Load toml data schema into dictionary if toml file exists
        toml_dict = toml.load(file_path)
    else:
        # Return False if file does not exist
        return file_exists

    return toml_dict


@exception_wrap
def check_data_shape(
    data_df: pd.DataFrame,
    schema_path: str = "./config/contributors_schema.toml",
) -> bool:
    """Compares the shape of the data and compares it to the shape of the toml
    file based off the data schema. Returns true if there is a match and false
    otherwise.

    Keyword Arguments:
        data_df -- Pandas dataframe containing data to be checked.
        schema_path -- Path to schema dictionary file
        (default: {"./config/DataSchema.toml"})

    Returns:
        A bool: boolean, True if number of columns is as expected, otherwise False
    """
    if not isinstance(data_df, pd.DataFrame):
        raise ValueError(
            f"data_df must be a pandas dataframe, is currently {type(data_df)}."
        )

    cols_match = False

    data_dict = data_df.to_dict()

    # Load toml data schema into dictionary
    toml_string = load_schema(schema_path)

    # Compare length of data dictionary to the data schema
    if len(data_dict) == len(toml_string):
        cols_match = True
    else:
        cols_match = False

    if cols_match is False:
        validation_logger.warning(f"Data columns match schema: {cols_match}.")
    else:
        validation_logger.info(f"Data columns match schema: {cols_match}.")

    validation_logger.info(
        f"Length of data: {len(data_dict)}. Length of schema: {len(toml_string)}"
    )
    return cols_match


@exception_wrap
def check_var_names(
    data_df: pd.DataFrame,
    filePath: str = "./config/contributors_schema.toml",
) -> bool:
    """Compare the keys of the ingested data file and the data schema
    dictionaries. If they match then returns True, if not then returns
    False

    Keyword Arguments:
        data_df -- Pandas dataframe containing data to be checked.
        filePath -- Path to schema TOML file
        (default: {"./config/Data_Schema.toml"})

    Returns:
        A bool: boolean value indicating whether data file dictionary
        keys match the data schema dictionary keys.
    """

    if not isinstance(data_df, pd.DataFrame):
        raise ValueError(
            f"data_df must be a pandas dataframe, is currently {type(data_df)}."
        )

    # Specify which key in snapshot data dictionary to get correct data
    # List, with each element containing a dictionary for each row of data
    data_dict = data_df.to_dict()
    data_keys = data_dict.keys()

    # Load toml data schema into dictionary
    toml_string = load_schema(filePath)

    if data_keys == toml_string.keys():
        dict_match = True
    else:
        dict_match = False

    if dict_match is False:
        validation_logger.warning(f"Data columns names match schema: {dict_match}.")
    else:
        validation_logger.info(f"Data columns names match schema: {dict_match}.")

    return dict_match


@exception_wrap
def data_key_diffs(
    data_df: pd.DataFrame,
    filePath: str = "./config/contributors_schema.toml",
) -> dict:
    """Compare differences between data dictionary and the toml data
    schema dictionary. Outputs a dictionary with 'dictionary_items_added'
    and 'dictionary_items_removed' as keys, with values as the items
    added or removed. Added items show differences in data file dictionary,
    i.e. difference in column names. Items removed shows columns that
    are not present in the data file.

    Keyword Arguments:
        data_df -- Pandas dataframe containing data to be checked.
        filePath -- Path to schema TOML file
        (default: {"./config/Data_Schema.toml"})

    Returns:
        A dict: dictionary containing items added to and items removed
        from the data schema dictionary. Added items show differences
        in data file column names, items removed show columns missing
        from the data file.
    """

    if not isinstance(data_df, pd.DataFrame):
        raise ValueError(
            f"data_df must be a pandas dataframe, is currently {type(data_df)}."
        )

    # Convert it to dictionary
    # data_dict = data.to_dict()
    data_dict = data_df.to_dict()

    # Load toml data schema into dictionary
    schema_dict = load_schema(filePath)

    # Check differences between the keys
    missing_from_df = set(schema_dict.keys()) - set(data_dict.keys())
    added_to_df = set(data_dict.keys()) - set(schema_dict.keys())

    # Checks if the length of the key set is the same in the two dictionaries
    if len(schema_dict.keys()) != len(data_dict.keys()):
        validation_logger.warning(
            f"""Differences detected in data compared to schema: \n
                                 \n Additional columns: {added_to_df}
                                 \n Removed columns: {missing_from_df} \n"""
        )
    else:
        validation_logger.info(
            f"""Data and schema columns match. \n
                                 \n Additional columns: {added_to_df}
                                 \n Removed columns: {missing_from_df} \n"""
        )

    diff_dict = {"Add_columns": added_to_df, "Remvd_columns": missing_from_df}

    return diff_dict
