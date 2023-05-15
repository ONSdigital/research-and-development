import math
import toml
import pandas as pd
import pydoop.hdfs as hdfs

from typing import IO


def read_SPP_snapshot(excel_file, excel_sheet) -> pd.DataFrame:
    """Read the updated SPP Snapshot Schema, specifying the name
    of the sheet to read. Convert it into a pandas dataframe,
    dropping any rows which include NaN values in the 'Field Name'
    column.

    Arguments:
        excel_file -- the excel file to be converted
        excel_sheet -- the name of the excel sheet to be converted
    Returns:
        A pd.DataFrame: a pandas dataframe object.
    """
    xl_dataframe = pd.read_excel(excel_file, sheet_name=excel_sheet, engine="openpyxl")

    # Drop rows with NaN values in the 'Field Name' column
    xl_dataframe = xl_dataframe.dropna(subset=["Field Name"])

    return xl_dataframe


def read_DAP_csv(excel_file) -> pd.DataFrame:
    """Read an excel file from DAP and convert it into a
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
        var = subDict1.pop("Field Name")
        var = var.replace('"', "")

        tomlDict[var] = subDict1

        return tomlDict


def create_toml(
    pd_dict: dict, output_toml_file: str = "./config/Data_Schema.toml"
) -> IO[str]:
    """Write a toml file from a dictionary.

    Arguments:
        pd_dict -- A dictionary containing a dictionary as
        its values.
        output_toml_file -- Path to the output toml file.
        (default: {"./config/DataSchema.toml"})
    Returns:
        A toml file - IO[str] type indicates a text based file
        (.toml) will be returned.
    """

    with open(output_toml_file, "w") as toml_file:
        toml.dump(pd_dict, toml_file)

    return toml_file


berd_schema_df = read_SPP_snapshot("./config/SPP Snapshot Schema.xlsx", "contributors")
berd_schema_dict = convert_dataFrame(berd_schema_df)
reshaped_schema_dict = reformat_tomlDict(berd_schema_dict)
tomlfile = create_toml(reshaped_schema_dict, "./config/Data_Schema.toml")
