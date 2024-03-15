"""Define helper functions that wrap regularly-used functions."""

import toml
import yaml

from itertools import chain

# Define paths
user_config_path = "config/userconfig.toml"


class Config_settings:
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


def get_numeric_cols(config: dict) -> list:
    """Return a list of numeric columns to use for imputation.

    These include columns of the form 2xx, 3xx, also the columns of the form
    emp_xx (which have been apportioned across product groups from the 4xx columns) and
    headcount_xx (which have been apportioned across proudct gropues from the 5xx cols)

    Args:
        config (dict): The pipeline configuration settings.

    Returns:
        numeric_cols (list): A list of all the columns imputation is applied to.
    """
    master_cols = list(config["breakdowns"].keys())
    bd_qs_lists = list(config["breakdowns"].values())
    bd_cols = list(chain(*bd_qs_lists))

    sum_cols = config["variables"]["sum_cols"]
    other_sum_cols = [c for c in sum_cols if c not in master_cols]

    numeric_cols = master_cols + bd_cols + other_sum_cols

    return numeric_cols