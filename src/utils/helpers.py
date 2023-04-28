"""Define helper functions that wrap regularly-used functions."""

import toml
import yaml
import os

# Define paths
user_config_path = "config/userconfig.toml"


class Config_settings:
    """Get the config settings from the config file."""

    def __init__(self):
        self.config_file = "src/developer_config.yaml"
        self.config_dict = self._get_config_settings()

    def _get_config_settings(self):
        """Get the config settings from the config file."""
        with open(self.config_file, "r") as file:
            config = yaml.safe_load(file)

        return config


user_config_path = "config/userconfig.toml"


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


class Config_settings:
    """Get the config settings from the config file."""

    def __init__(self):
        self.config_file = "src/developer_config.yaml"
        self.config_dict = self._get_config_settings()

    def _get_config_settings(self):
        """Get the config settings from the config file."""
        with open(self.config_file, "r") as file:
            config = yaml.safe_load(file)

        return config
