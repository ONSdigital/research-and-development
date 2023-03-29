"""Define helper functions that wrap regularly-used functions."""

import yaml
import toml
import pydoop.hdfs as hdfs
import csv
import pandas as pd


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


def hdfs_csv_creator(filepath, columns):
    """Creates a csv file in DAP with user
    defined headers if it doesn't exist.
    Args:
        filename (string): Example: "name_of_file.csv"
        columns (list): Example: ["a","b","c","d"]
    """
    if not hdfs.path.isfile(filepath):
        with hdfs.open(filepath, "wt") as file:
            writer = csv.writer(file)
            writer.writerow(columns)

    return None


def hdfs_append(filepath, last_run):
    """Function to append latest log metadata to csv in DAP

    Args:
        filepath (string): The filepath in Hue
        last_run (Dataframe): Dataframe of latest run data
    """
    with hdfs.open(filepath, "r") as file:
        df_imported_from_hdfs = pd.read_csv(file)
        last_run_data = df_imported_from_hdfs.append(last_run)

    with hdfs.open(filepath, "wt") as file:
        last_run_data.to_csv(file, index=False)

    return None


user_config_path = "config/userconfig.toml"


def user_config_reader(configfile: str = user_config_path) -> dict:
    """Function to parse the userconfig.toml file.

    Returns:
        A nested dictionary where the keys are section titles within the TOML file.
        If only one variable under the section title in the TOML file is given
        then it is passed directly as a dictionary value. If more than one
        variable is defined then they are parsed as a dictionary themselves.
        An example of what is returned is given below:

        {'title': 'TOML Example config', 'period': {'start_period':
        datetime.date(1990, 10, 10), 'end_period': datetime.date(2000, 10, 5)}}
    """
    toml_dict = toml.load(configfile)

    return toml_dict
