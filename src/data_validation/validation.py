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

from src.data_ingest.loading import hdfs_load_json

snapshot_path = (
    "/ons/rdbe_dev/snapshot-202012-002-fba5c4ba-fb8c-4a62-87bb-66c725eea5fd.json"
)


def check_data_shape(
    dataFile: str = snapshot_path,
    filePath: str = "./config/DataSchema.toml",
    numCols: int = 5,
) -> bool:
    """Compares the shape of the data and compares it to the shape of the toml
    file based off the data schema. Returns true if there is a match and false
    otherwise.

    Keyword Arguments:
        dataFile -- Path to data file to compare (default: {snapshot_path})
        filePath -- Path to schema dictionary file
        (default: {"./config/DataSchema.toml"})
        numCols -- Number of columns in data (default: {5})

    Returns:
        A bool: boolean, True is number of columns is as expected, otherwise False
    """
    # Check if DataSchema.toml exists
    file_exists = os.path.exists(filePath)
    snapdata, contributerdict, responsesdict = hdfs_load_json(snapshot_path)
    cols_match = False

    if not file_exists:
        return file_exists
    else:
        toml_string = toml.load(filePath)

        shared_items = {
            k: toml_string[k]
            for k in toml_string
            if k in responsesdict
            # if k in responsesdict and toml_string[k] == responsesdict[k]
        }

        data_key1 = list(contributerdict.keys())[0]
        schema_key1 = list(toml_string.keys())[0]

        data_rows, data_columns = len(contributerdict), contributerdict[data_key1]
        schema_rows, schema_columns = len(toml_string), len(toml_string[schema_key1])

        # Check if data dictionary value is of a dict or list type
        # If it isn't then set column number equal to 1, else length of value
        if not type(data_columns) == dict or not type(data_columns) == list:
            data_columns = 1
        else:
            data_columns = len(data_columns)

        outString = f"""Data has {data_rows} rows and {data_columns} columns.
        It should have {schema_rows} rows and {schema_columns} columns."""

        if data_columns == schema_columns:
            cols_match = True
        else:
            cols_match = False

    return cols_match, len(shared_items), shared_items, outString
    # return snapdata


test = check_data_shape()


def check_var_names(
    dataFile: str = snapshot_path,
    filePath: str = "./config/DataSchema.toml",
) -> bool:
    """_summary_

    Keyword Arguments:
        dataFile -- _description_ (default: {snapshot_path})
        filePath -- _description_ (default: {"./config/DataSchema.toml"})

    Returns:
        _description_
    """
    # snapdata, contributerdict, responsesdict = hdfs_load_json(snapshot_path)

    # contributerDF = pd.DataFrame.from_dict(contributerdict, orient="index")
    # responsesDF = pd.DataFrame.from_dict(responsesdict, orient="index")

    toml_dict = toml.load(filePath)
    schemaDF = pd.DataFrame.from_dict(toml_dict, orient="index")

    # merged_DF = schemaDF.merge(contributerDF, how='left')

    return schemaDF


test2 = check_var_names()
print(test2)
