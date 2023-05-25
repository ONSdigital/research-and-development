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
from deepdiff import DeepDiff
from pydantic.dataclasses import dataclass
from src.utils.helpers import Config_settings
from src.utils.hdfs_mods import hdfs_load_json as read_data


datafilepath = "/ons/rdbe_dev/Frozen_Group_Data2021_244_Headers.csv"
# dummydatapath = "/ons/rdbe_dev/Frozen_Test_Data_multi-row.csv"
dummydatapath = "/ons/rdbe_dev/Frozen_Test_Data_multi-row_Matching.csv"


# Get config settings from developer_config.yaml
conf_obj = Config_settings()
config = conf_obj.config_dict
config_paths = config["paths"]
snapshot_path = config_paths["snapshot_path"]  # Taken from config file


def load_schema(file_path: str = "./config/Data_Schema.toml") -> dict:
    """Load the data schema from toml file into a dictionary

    Keyword Arguments:
        file_path -- Path to data schema toml file
        (default: {"./config/Data_Schema.toml"})

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


def check_data_shape(
    data_file: str = snapshot_path,
    schema_path: str = "./config/Data_Schema.toml",
) -> bool:
    """Compares the shape of the data and compares it to the shape of the toml
    file based off the data schema. Returns true if there is a match and false
    otherwise.

    Keyword Arguments:
        data_file -- Path to data file to compare (default: {snapshot_path})
        schema_path -- Path to schema dictionary file
        (default: {"./config/Data_Schema.toml"})

    Returns:
        A bool: boolean, True if number of columns is as expected, otherwise False
    """

    cols_match = False

    # Read data file from json file
    snapdata = read_data(data_file)

    # Specify which key in snapshot data dictionary to get correct data
    # List, with each element containing a dictionary for each row of data
    contributerdict = snapdata["contributors"]

    # Load toml data schema into dictionary
    toml_string = load_schema(schema_path)

    # Compare length of data dictionary to the data schema
    if len(contributerdict[0]) == len(toml_string):
        cols_match = True
    else:
        cols_match = False

    return cols_match


def check_var_names(
    dataFile: str = snapshot_path,
    filePath: str = "./config/Data_Schema.toml",
) -> bool:
    """Compare the keys of the ingested data file and the data schema
    dictionaries. If they match then returns True, if not then returns
    False

    Keyword Arguments:
        dataFile -- _description_ (default: {snapshot_path})
        filePath -- _description_ (default: {"./config/Data_Schema.toml"})

    Returns:
        A bool: boolean value indicating whether data file dictionary
        keys match the data schema dictionary keys.
    """
    # Read data file
    snapdata = read_data(dataFile)

    # Specify which key in snapshot data dictionary to get correct data
    # List, with each element containing a dictionary for each row of data
    contributerdict = snapdata["contributors"][0]

    # Load toml data schema into dictionary
    toml_string = load_schema(filePath)

    if contributerdict.keys() == toml_string.keys():
        dict_match = True
    else:
        dict_match = False

    return dict_match


def data_key_diffs(
    dataFile: str = snapshot_path,
    filePath: str = "./config/Data_Schema.toml",
) -> dict:
    """Compare differences between data dictionary and the toml data
    schema dictionary. Outputs a dictionary with 'dictionary_items_added'
    and 'dictionary_items_removed' as keys, with values as the items
    added or removed. Added items show differences in data file dictionary,
    i.e. difference in column names. Items removed shows columns that
    are not present in the data file.

    Keyword Arguments:
        dataFile -- _description_ (default: {snapshot_path})
        filePath -- _description_ (default: {"./config/Data_Schema.toml"})

    Returns:
        A dict: dictionary containing items added to and items removed
        from the data schema dictionary. Added items show differences
        in data file column names, items removed show columns missing
        from the data file.
    """
    # Read data file
    data = read_data(dataFile)

    # Convert it to dictionary
    # data_dict = data.to_dict()
    data_dict = data["contributors"][0]

    # Load toml data schema into dictionary
    toml_string = load_schema(filePath)

    # Create dictionaries only containing keys of data and schema
    toml_keys_dict = {k: {} for k in toml_string}
    data_keys_dict = {k: {} for k in data_dict}

    # Does a case-sensitive comparison of the keys of two dictionaries
    diff = DeepDiff(toml_keys_dict, data_keys_dict, ignore_string_case=True)

    return diff


def create_data_dict(dataFile: str = dummydatapath):
    # Read data file
    data = read_data(dataFile)

    dummyDict = {}

    # Convert it to dictionary
    data_dict = data.to_dict(orient="index")
    for k, v in data_dict.items():
        if isinstance(v, dict):

            for sub_key, sub_val in v.items():

                # Initialise dictionary with first index
                if k == 0:
                    dummyDict[sub_key] = [sub_val]
                else:
                    dummyDict[sub_key] += [sub_val]
        else:
            print("{0} : {1}".format(k, v))

    return dummyDict


def data_types(
    dataFile: str = datafilepath,
    filePath: str = "./config/DataSchema.toml",
) -> dict:
    # Load toml data schema into dictionary
    toml_string = load_schema(filePath)
    schema_type_dict = {}

    # Loop over initial tomls schema dictionary keys:values
    for toml_key, toml_val in toml_string.items():

        # Loop over nest dictionary key:value pairs for each main key
        for nest_key, nest_val in toml_val.items():

            # Create a new dictionary only containing the data_type value
            # for each main key for comparison
            if nest_key == "data_type":
                schema_type_dict[toml_key] = nest_val

    for type_key, type_val in schema_type_dict.items():

        if type_val == "Categorical":
            schema_type_dict[type_key] = [type(1), type("")]
        elif type_val == "Numerical flat (or decimal)":
            schema_type_dict[type_key] = [type(1.0)]
        elif type_val == "Numeric Integer":
            schema_type_dict[type_key] = [type(1)]
        elif type_val == "Boolean (True or False, 0 or 1)":
            schema_type_dict[type_key] = [bool]
        else:
            schema_type_dict[type_key] = None

    return schema_type_dict


# def check_data_types(
#    dataFile: str = datafilepath,
#    filePath: str = "./config/DataSchema.toml",
# ) -> bool:
#    """_summary_
#
#    Keyword Arguments:
#        dataFile -- _description_ (default: {datafilepath})
#        filePath -- _description_ (default: {"./config/DataSchema.toml"})
#
#    Returns:
#        _description_
#    """
#    # Create data dictionary containing all values in each column
#    data_dict = create_data_dict()
#
#    # Create dictionary containing 'column - type' key - value pairs
#    type_dict = data_types()
#    data_list = []
#    data_list.append( [','.join([*type_dict]) ] )
#    print(data_list[0])
#
#    for type_key, type_val in type_dict.items():
#
#        if type_val == "Categorical":
#            type_dict[type_key] = [type(1), type("")]
#        elif type_val == "Numerical flat (or decimal)":
#            type_dict[type_key] = [type(1.0)]
#        elif type_val == "Numeric Integer":
#            type_dict[type_key] = [type(1)]
#        elif type_val == "Boolean (True or False, 0 or 1)":
#            type_dict[type_key] = [bool]
#        else:
#            type_dict[type_key] = None
#
#    for dKey, dVal in data_dict.items():
#        if dKey in type_dict.keys():
#
#            row = 0
#
#            for items in dVal:
#
#                # Ignore None types here. None types should be amended
#                # to their relevant types in data
#                if type_dict[dKey] is not None:
#
#                    # If column is of the 'Categorical' type and the data
#                    # is not either an int or a str
#                    if (
#                        len(type_dict[dKey]) > 1
#                        and type(items) is not (type_dict[dKey])[0]
#                        and len(type_dict[dKey]) > 1
#                        and type(items) is not (type_dict[dKey])[1]
#                    ):
#                        print(
#                            f"""In row {row}, {dKey} is of type {type(items)}.
#                            It should either be of type { (type_dict[dKey])[0] } or
#                            { (type_dict[dKey])[1] }"""
#                        )
#                    # Else if column is of a singular type, and the data
#                    # does not match that type
#                    elif (len(type_dict[dKey]) == 1) and type(items) is not (
#                        type_dict[dKey]
#                    )[0]:
#                        print(
#                            f"""In row {row}, {dKey} is of type {type(items)}.
#                            It should be of type { (type_dict[dKey])[0] }"""
#                        )
#                    # Pass values which match their respective schema types
#                    else:
#                        pass
#                # Signal which values are of None type
#                else:
#                    print(
#                        (
#                            f"Row {row}, column {dKey} is of type {type_dict[dKey]}.\n"
#                            f"Please amend!"
#                        )
#                    )
#                row += 1
#
#    return "End"
#


def test_data_types(
    dataFile: str = datafilepath,
    filePath: str = "./config/DataSchema.toml",
) -> bool:
    """_summary_

    Keyword Arguments:
        dataFile -- _description_ (default: {datafilepath})
        filePath -- _description_ (default: {"./config/DataSchema.toml"})

    Returns:
        _description_
    """
    # Create data dictionary containing all values in each column
    # data_dict = create_data_dict()

    # Create dictionary containing 'column - type' key - value pairs
    type_dict = data_types()
    data_list = []
    data_list.append(",".join([*type_dict]))

    vals = read_data(dummydatapath)
    for i, row in vals.iterrows():
        val_list = vals.iloc[0].tolist()

        data_list.append(",".join(map(str, val_list)))

    return data_list


# Turns a dictionary into a class
@dataclass
class Dict2Class(object):
    def __init__(self, my_dict):

        for key in my_dict:
            setattr(self, key, my_dict[key])


test = test_data_types()
test2 = data_types()
result = Dict2Class(test2)
# print(f"headcount_total: {result.headcount_total}")
# print(result.__dict__)

# print(result.__dict__)
error_count = 0
output_data = []

# for idx, d in enumerate(test[1:]):
#    try:
#        market_data = Dict2Class(*d.split(","))
#        output_data.append(json.dumps(market_data, default=pydantic_encoder))
#    except ValidationError as ve:
#        logging.error(f"row number: {idx + 1}, row: {d}, error: {ve}")
#        error_count += 1

# print(f"Error count: {error_count}")
# print(output_data)
