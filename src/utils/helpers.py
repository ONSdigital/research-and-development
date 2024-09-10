"""Define helper functions to be used throughout the pipeline.."""
import yaml
import toml
import logging
import pandas as pd

from typing import Union

from src.utils.defence import type_defence
from src.staging.postcode_validation import run_full_postcode_process
from src.mapping.itl_mapping import join_itl_regions

# Define paths
user_config_path = "config/userconfig.toml"


class ConfigSettings:
    """Get the config settings from the config file."""

    def __init__(self, config_path):
        self.config_file = config_path
        self.config_dict = self._get_config_settings()

    def _get_config_settings(self):
        """Get the config settings from the config file."""
        with open(self.config_file, "r") as file:
            config = yaml.safe_load(file)

        return config


def user_config_reader(configfile: str = user_config_path) -> dict:
    """Function to parse the userconfig.toml file

    Args:
        configfile (str, optional): _description_. Defaults to user_config_path.

    Returns:
        dict: A nested dictionary where the keys are section titles within the TOML
        file.
        If only one variable under the section title in the TOML file is given
        then it is passed directly as a dictionary value. If more than one
        variable is defined then they are parsed as a dictionary themselves.
        An example of what is returned is given below:

        {'title': 'TOML Example config', 'period': {'start_period':
        datetime.date(1990, 10, 10), 'end_period': datetime.date(2000, 10, 5)}}
    """
    toml_dict = toml.load(configfile)

    return toml_dict


def period_select() -> tuple:
    """Function returning the start and end date under consideration.


    Returns:
        tuple: A tuple containing two datetime.date objects. The first is the
        start date of the period under consideration, the second is the
        end date of that period.
        Example:
            (datetime.date(1990, 10, 10), datetime.date(2000, 10, 5))
    """

    period_dict = user_config_reader()["period"]

    return period_dict["start_period"], period_dict["end_period"]


def convert_formtype(formtype_value: str) -> str:
    """Convert the formtype to a standardised format.

    Args:
        formtype_value (str): The value to standardise.

    Returns:
        str: The standardised value for formtype.
    """
    if pd.notnull(formtype_value):
        formtype_value = str(formtype_value)
        if formtype_value == "1" or formtype_value == "1.0" or formtype_value == "0001":
            return "0001"
        elif (
            formtype_value == "6" or formtype_value == "6.0" or formtype_value == "0006"
        ):
            return "0006"
        else:
            return None
    else:
        return None


def values_in_column(
        df: pd.DataFrame,
        col_name: str,
        values: Union[list, pd.Series]
) -> bool:
    """Determine whether a list of values are all present in a dataframe column.

    Args:
        df (pd.DataFrame): The dataframe.
        col_name (str): The column name.
        values (Union[list, pd.Series]): The values to check.

    Returns:
        bool: Whether or values are in the column.
    """
    type_defence(df, "df", pd.DataFrame)
    type_defence(col_name, "col_name", str)
    type_defence(values, "values", (list, pd.Series))
    if isinstance(values, pd.Series):
        values = list(values)
    result = set(values).issubset(set(df[col_name]))
    return result


def validate_updated_postcodes(
        df: pd.DataFrame, 
        postcode_mapper: pd.DataFrame, 
        itl_mapper: pd.DataFrame,
        config: dict,
        MainLogger: logging.Logger
    ) -> pd.DataFrame:
    
    # validate the constructed and imputed postcodes
    df, invalid_df= run_full_postcode_process(
        df,
        postcode_mapper,
        config,
    )
    # There shouldn't be invalid postcodes at this stage, if there are they will be
    # printed to the screen
    if not invalid_df.empty:
        MainLogger.warning(
            f"Invalid postcodes found in the imputed dataframe: {invalid_df}"
        )
    # re-calculate the itl columns based on imputed and constructed columns
    geo_cols = config["mappers"]["geo_cols"]   
    df = df.copy().drop(["itl"] + geo_cols, axis=1)
    df = join_itl_regions(
        df,
        postcode_mapper,
        itl_mapper,
        config,
        pc_col="postcodes_harmonised",
    ) 
    return df


def tree_to_list(
    tree: dict, path_list: list = [], prefix: str = ""
) -> list:
    """
    Convert a dictionary of paths to a list.

    This function converts a directory tree that is provided as a dictionary to a 
    list of full paths. This is done recursively, so the number of tiers is not
    pre-defined. Returns a list of absolute directory paths.
    Directory and subdirectory names must be the keys in the dictionary.
    Directory that has no sub-directories must point to an empty dictionary {}.

    Example
    Input data
    mydict = {
        "BERD": {
            "01":{},
            "02":{},
        },
        "PNP": {
            "03":{},
            "04":{"qa":{}},
        },
    }

    Usage: tree_to_list(mydict, prefix="R:/2023")

    Result:
    ['R:/2023/BERD', 'R:/2023/BERD/01', 'R:/2023/BERD/02', 'R:/2023/PNP',
    'R:/2023/PNP/03', 'R:/2023/PNP/04', 'R:/2023/PNP/04/qa']

    Args:
        tree (dict): The whole tree or its branch
        path_list (list): A list of full paths that is populated when the function
            runs. Must be empty when you call the function.
        prefix (str): The common prefix. It should start with the platform-
            specific root, such as "R:/dap_emulation" or "dapsen/workspace_zone_res_dev",
            followed by the year_surveys. Do not add a forward slash at the end.

    Returns:
        A list of all absolute paths

    """
    # Separator is hardcoded to avoid any errors.
    sep = "/"

    # Input must be a dictionary of dictionaries or an empty dictionary
    if isinstance(tree, dict):

        # The recursive iteration will proceed if the current tree is not empty.
        # The recursive iterations will stop once we reach the lowest level
        # indicated by an empty dictionary.
        if tree:

            # For a non-empty dictionary, iterating through all top-level keys.
            for key in tree:
                if prefix == "":
                    # If the prefix is empty, we don't want to start from slash. We
                    # just set the prefix to be the key, which is the directory name
                    mypref = key
                else:
                    # If the prefix is not empty, we add the separator and the
                    # directory name to it
                    mypref = prefix + sep + key

                # The updated prefix is appended to the path list
                path_list += [mypref]

                # Doing the same for the underlying sub-directory
                path_list = tree_to_list(tree[key], path_list, mypref)

        return path_list
    else:
        raise TypeError(f"Input must be a dictionary, but {type(tree)} is given")
