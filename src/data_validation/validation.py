import postcodes_uk
import pandas as pd
from src.utils.wrappers import time_logger_wrap, exception_wrap
import logging

from src.utils.helpers import Config_settings


# Get the config
conf_obj = Config_settings()
config = conf_obj.config_dict

ValidationLogger = logging.getLogger(__name__)


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
        ValidationLogger.warning(
            f"These postcodes are not found in the ONS postcode list: {unreal_postcodes.to_list()}"  # noqa
        )

    # Check if postcodes match pattern
    invalid_pattern_postcodes = df.loc[
        ~df["referencepostcode"].apply(validate_postcode_pattern), "referencepostcode"
    ]

    # Log the invalid postcodes
    if not invalid_pattern_postcodes.empty:
        ValidationLogger.warning(
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

    ValidationLogger.info("All postcodes validated....")

    return True

def check_pcs_real(df: pd.DataFrame, masterlist_path: str):
    """Checks if the postcodes are real against a masterlist of actual postcodes
    """
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
import os
import toml
import pandas as pd
import pydoop.hdfs as hdfs


datafilepath = "/ons/rdbe_dev/Frozen_Group_Data2021_244_Headers.csv"


def read_data(excel_file) -> pd.DataFrame:
    """Read an excel file and convert it into a
    pandas dataframe, dropping any 'Unnamed:' columns.


    Arguments:
        excel_file -- the excel file to be converted

    Returns:
        A pd.DataFrame: a pandas dataframe object.
    """
    with hdfs.open(excel_file, "r") as file:

        # Import csv file and convert to Dataframe
        sheet = pd.read_csv(file)

    return sheet


def load_schema(filePath: str = "./config/DataSchema.toml"):
    """Load the data schema from toml file into a dictionary

    Keyword Arguments:
        filePath -- Path to data schema toml file
        (default: {"./config/DataSchema.toml"})

    Returns:
        A dict: dictionary containing parsed schema toml file
    """
    file_exists = os.path.exists(filePath)

    # Check if DataSchema.toml exists
    if not file_exists:
        return file_exists
    else:
        # Load toml data schema into dictionary
        toml_string = toml.load(filePath)
    return toml_string


def check_data_shape(
    dataFile: str = datafilepath,
    filePath: str = "./config/DataSchema.toml",
    numCols: int = 93,
) -> bool:
    """Compares the shape of the data and compares it to the shape of the toml
    file based off the data schema. Returns true if there is a match and false
    otherwise.

    Keyword Arguments:
        dataFile -- Path to data file to compare (default: {datafilepath})
        filePath -- Path to schema dictionary file
        (default: {"./config/DataSchema.toml"})
        numCols -- Number of columns in data (default: {93})

    Returns:
        A bool: boolean, True if number of columns is as expected, otherwise False
    """

    cols_match = False

    # Read data file
    data = read_data(dataFile)

    # Convert it to dictionary
    data_dict = data.to_dict()

    # Load toml data schema into dictionary
    toml_string = load_schema(filePath)

    # Create a 'shared key' dictionary
    shared_items = {k: toml_string[k] for k in toml_string if k in data_dict}

    # Compare number of 'columns' in data to data schema
    if len(shared_items) == len(toml_string):
        cols_match = True
    else:
        cols_match = False

    return cols_match


def check_var_names(
    dataFile: str = datafilepath,
    filePath: str = "./config/DataSchema.toml",
) -> bool:
    """_summary_

    Keyword Arguments:
        dataFile -- _description_ (default: {snapshot_path})
        filePath -- _description_ (default: {"./config/DataSchema.toml"})

    Returns:
        _description_
    """
    # Read data file
    data = read_data(dataFile)

    # Convert it to dictionary
    data_dict = data.to_dict()

    # Load toml data schema into dictionary
    toml_string = load_schema(filePath)

    if data_dict.keys() == toml_string.keys():
        dict_match = True
    else:
        dict_match = False

    return dict_match
