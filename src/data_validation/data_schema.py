import math
import toml
import pandas as pd

from typing import IO


def read_xlsx(excel_file) -> pd.DataFrame:
    """Read an excel file and convert it into a
    pandas dataframe, dropping any 'Unnamed:' columns.


    Arguments:
        excel_file -- the excel file to be converted

    Returns:
        A pd.DataFrame: a pandas dataframe object.
    """
    xl_dataframe = pd.read_excel(excel_file, nrows=93, engine="openpyxl")
    xl_dataframe = xl_dataframe[
        xl_dataframe.columns.drop(list(xl_dataframe.filter(regex="Unnamed:")))
    ]

    return xl_dataframe


def convert_dataFrame(pdFrame: pd.DataFrame) -> dict:
    """Convert a pandas dataframe into a dictionary oriented by
    index. This makes the keys in the dictionary the row index
    of the dataframe, and the values are a dictionary containing
    the other key-value information.

    Arguments:
        pdFrame -- the pandas dataframe to be converted

    Returns:
        A dict: dict object oriented by index
    """
    pd_dict = pdFrame.to_dict(orient="index")
    return pd_dict


def is_nan(value) -> bool:
    """Takes in a value and returns a boolean indicating
    whether it is a 'not a number' or not.

    Arguments:
        value -- Any value

    Returns:
        A bool: boolean indicating whether the value is
        'not a number' or not, as determined by the 'math'
        module.
    """
    return math.isnan(float(value))


def reformat_tomlDict(pdDict: dict) -> dict:
    """Creates a dictionary suitable to be converted
    into a toml file. Takes an index oriented
    dictionary as input and creates a new dictionary
    at as

    Arguments:
        pdDict -- a dictionary

    Returns:
        A dict: dictionary ready to be used to create
        a toml file.
    """
    newDict = {}
    tomlDict = {}

    # Loop over input dictionary to create a sub dictionary
    for key in pdDict:
        newDict[str(key)] = pdDict[key]

        subDict1 = newDict[str(key)]
        var = subDict1.pop("Field Name (as it appears in dataset)")

        tomlDict[var] = subDict1

    # Loop over each key in sub-dictionary and reformat values for usability
    for key in tomlDict:

        subDict2 = tomlDict[key]

        subDict2["description"] = subDict2.pop("Description")
        subDict2["data_type"] = subDict2.pop(
            """Data Type (Numeric integer/Numeric float (or decimal)
            /Text/Categorical/Boolean (True or False, 1 or 0))"""
        )
        subDict2["nullable"] = subDict2.pop(
            "Nullable (is it acceptable to have a null value? Acceptable = Yes)"
        )

        acceptable_values_str = str(subDict2["Acceptable Values (>0 or 0 – 1,000,000)"])
        acceptable_values_list = acceptable_values_str.split()

        subDict2["min_acceptable_value"] = acceptable_values_list[0]
        subDict2["max_acceptable_value"] = acceptable_values_list[-1].replace(",", "")

        if is_nan(subDict2["min_acceptable_value"]):
            subDict2["min_acceptable_value"] = acceptable_values_list[0]
        elif is_nan(subDict2["max_acceptable_value"]):
            subDict2["max_acceptable_value"] = acceptable_values_list[-1]
        else:
            subDict2["min_acceptable_value"] = int(acceptable_values_list[0])
            subDict2["max_acceptable_value"] = int(
                acceptable_values_list[-1].replace(",", "")
            )

        subDict2.pop("Acceptable Values (>0 or 0 – 1,000,000)")

        tomlDict[key] = subDict2

    return tomlDict


def create_toml(pdDict: dict) -> IO[str]:
    """Write a toml file from a dictionary.

    Arguments:
        pdDict -- A dictionary containing a dictionary as
        its values.

    Returns:
        A toml file - IO[str] type indicates a text based file
        (.toml) will be returned.
    """

    output_file_name = "./config/DataSchema.toml"
    with open(output_file_name, "w") as toml_file:
        toml.dump(pdDict, toml_file)

    return pdDict


test = read_xlsx("C:\\Users\\macrar\\Downloads\\Data Dictionary - BERD.xlsx")
test2 = convert_dataFrame(test)
test3 = reformat_tomlDict(test2)
test4 = create_toml(test3)
