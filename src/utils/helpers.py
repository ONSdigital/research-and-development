"""Define helper functions that wrap regularly-used functions."""

from typing import Union

import pandas as pd
import yaml
import toml

from src.utils.defence import type_defence

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