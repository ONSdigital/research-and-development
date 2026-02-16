"""Define helper functions to be used throughout the pipeline.."""

import yaml
import tomli
import os
import pandas as pd

from typing import Union
from collections.abc import Callable

from src.utils.defence import type_defence
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
    with open(configfile, "rb") as file:
        toml_dict = tomli.load(file)

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
    df: pd.DataFrame, col_name: str, values: Union[list, pd.Series]
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
) -> pd.DataFrame:
    """Update the postcodes_harmonised column and re-map the itl columns.

    Args:
        df (pd.DataFrame): The full responses dataframe.
        postcode_mapper (pd.DataFrame): The postcode mapper dataframe mapping to itl.
        itl_mapper (pd.DataFrame): The ITL mapper dataframe mapping to ITL regions.
        config (dict): The pipeline configuration settings.

    Returns:
        pd.DataFrame: The updated full responses dataframe with the postcodes_harmonised
            column updated and the itl columns re-mapped.
    """
    # filter out records that have been constructed or imputed with backdata
    imp_marker_mask = df["imp_marker"].isin(["CF", "MoR", "constructed"])
    if "is_constructed" in df.columns:
        constructed_mask = df["is_constructed"].isin([True])
        mask = imp_marker_mask | constructed_mask
    else:
        mask = imp_marker_mask
    filtered_df = df.copy().loc[mask]

    # re-calculate the itl columns based on imputed and constructed columns
    geo_cols = config["mappers"]["geo_cols"]
    filtered_df = filtered_df.copy().drop(["itl"] + geo_cols, axis=1)
    filtered_df = join_itl_regions(
        filtered_df,
        postcode_mapper,
        itl_mapper,
        config,
        pc_col="postcodes_harmonised",
        warn_only=True,
    )

    filtered_df = filtered_df[list(df.columns)]

    df = pd.concat([df.loc[~mask], filtered_df])
    return df


def tree_to_list(tree: dict, path_list: list = [], prefix: str = "") -> list:
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
            specific root, such as "R:/dap_emulation" or "dapsen/workspace_zone_res_dev"
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


def filename_amender(filename, config):
    """Append the survey type, survey year, run_id, and date to filename if survey
    is PNG, else append only survey year, run_id, and date to filename.

    Args:
        filename (str): The filename
        config (dict): The configuration dictionary

    Returns:
        str: The amended filename
    """
    survey_year = config["survey"]["survey_year"]
    survey_type = config["survey"]["survey_type"]
    run_id = config["filename_items"]["run_id"]
    tdate = config["filename_items"]["tdate"]

    if survey_type == "PNP":
        filename = f"{survey_type}_{survey_year}_{filename}_{tdate}_v{run_id}.csv"
    else:
        filename = f"{survey_year}_{filename}_{tdate}_v{run_id}.csv"

    return filename


def save_config_files(config: dict, rd_copy_file: Callable) -> None:
    """
    Save the paths to the user and developer config files in the config dictionary.

    NOTE: running in Windows, there is the possibility that different people create a
    run with the same run_id. In this case, the old file would be overwritten.
    As we are no longer developing on Windows, this is not a concern.
    In the production envrionment, it will not be a problem.

    Args:
        config (dict): The configuration dictionary.
        rd_copy_file (Callable): The function to copy files.

    Returns:
        None
    """
    user_config_path = config["global"]["user_config_path"]
    dev_config_path = config["global"]["dev_config_path"]
    old_conf_folder = config["relative_paths"]["old_configs_folder"]
    run_id = config["filename_items"]["run_id"]

    # create the old_configs folder if it doesn't exist
    if not os.path.exists(old_conf_folder):
        os.makedirs(old_conf_folder)

    user_output_path = os.path.join(old_conf_folder, f"user_config_{run_id}.yaml")
    dev_output_path = os.path.join(old_conf_folder, f"dev_config_{run_id}.yaml")

    # Copy the user and developer config files to the old_configs folder
    rd_copy_file(user_config_path, user_output_path)
    rd_copy_file(dev_config_path, dev_output_path)


def order_dataframe_for_output(
    df: pd.DataFrame, cols=["reference", "instance"]
) -> pd.DataFrame:
    """Order the dataframe for output.

    Args:
        df (pd.DataFrame): The dataframe to order.
        cols (list): The columns to order by. Defaults to ["reference", "instance"]

    Returns:
        pd.DataFrame: The ordered dataframe.
    """
    # Check if the columns exist in the dataframe
    for col in cols:
        if col not in df.columns:
            raise ValueError(f"Column {col} not found in dataframe.")

    # Order the dataframe by the specified columns
    df = df.sort_values(by=cols, ascending=True)
    df = df.reset_index(drop=True)

    return df
