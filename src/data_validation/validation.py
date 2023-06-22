import os
import toml
import postcodes_uk
import pandas as pd

from src.utils.wrappers import logger_creator, time_logger_wrap, exception_wrap
from src.utils.helpers import Config_settings

# Get the config
conf_obj = Config_settings()
config = conf_obj.config_dict
global_config = config["global"]

# Set up logging
validationlogger = logger_creator(global_config)


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
def get_masterlist(masterlist_path) -> pd.Series:
    """This function loads the masterlist of postcodes from a csv file

    Returns:
        pd.Series: The dataframe of postcodes
    """
    masterlist = pd.read_csv(masterlist_path, usecols=["pcd"]).squeeze()
    return masterlist


@time_logger_wrap
@exception_wrap
def validate_post_col(df: pd.DataFrame, masterlist_path: str) -> bool:
    """This function checks if all postcodes in the specified DataFrame column
        are valid UK postcodes. It uses the `validate_postcode` function to
        perform the validation.

    Args:
        df (pd.DataFrame): The DataFrame containing the postcodes.

    Returns:
        bool: True if all postcodes are valid, False otherwise.

    Raises:
        ValueError: If any invalid postcodes are found, a ValueError is raised.
            The error message includes the list of invalid postcodes.

    Example:
        >>> df = pd.DataFrame(
            {"referencepostcode": ["AB12 3CD", "EFG 456", "HIJ 789", "KL1M 2NO"]})
        >>> validate_post_col(df, "example-path/to/masterlist.csv"")
        ValueError: Invalid postcodes found: ['EFG 456', 'HIJ 789']
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"The dataframe you are attempting to validate is {type(df)}")

    unreal_postcodes = check_pcs_real(df, masterlist_path)

    # Log the unreal postcodes
    if not unreal_postcodes.empty:
        validationlogger.warning(
            f"These postcodes are not found in the ONS postcode list: {unreal_postcodes.to_list()}"  # noqa
        )

    # Check if postcodes match pattern
    invalid_pattern_postcodes = df.loc[
        ~df["referencepostcode"].apply(validate_postcode_pattern), "referencepostcode"
    ]

    # Log the invalid postcodes
    if not invalid_pattern_postcodes.empty:
        validationlogger.warning(
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

        validationlogger.info("All postcodes validated....")

    return True


def check_pcs_real(df: pd.DataFrame, masterlist_path: str):
    """Checks if the postcodes are real against a masterlist of actual postcodes"""
    if config["global"]["postcode_csv_check"]:
        master_series = get_masterlist(masterlist_path)

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
        toml_string = toml.load(file_path)
    else:
        # Return False if file does not exist
        return file_exists

    return toml_string


@exception_wrap
def check_data_shape(
    data_df: pd.DataFrame,
    schema_path: str = "./config/contributors_schema.toml",
) -> bool:
    """Compares the shape of the data and compares it to the shape of the toml
    file based off the data schema. Returns true if there is a match and false
    otherwise.

    Keyword Arguments:
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
        validationlogger.warning(f"Data columns match schema: {cols_match}.")
    else:
        validationlogger.info(f"Data columns match schema: {cols_match}.")

    validationlogger.info(
        f"Length of data: {len(data_dict)}. Length of schema: {len(toml_string)}"
    )
    return cols_match
