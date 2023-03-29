"""Define helper functions that wrap regularly-used functions."""

import toml
import pydoop.hdfs as hdfs
import csv
import pandas as pd

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


def hdfs_csv_creator(filepath: str, columns: list):
    """Creates a csv file in DAP with user
    defined headers if it doesn't exist.
    Args:
        filename (string): Example: "name_of_file.csv"
        columns (list): Example: ["a","b","c","d"]
    """

    # Check if the file exists
    if not hdfs.path.isfile(filepath):
        # open the file in write mode inside Hadoop context
        with hdfs.open(filepath, "wt") as file:
            # Create new csv file in specified folder
            writer = csv.writer(file)
            # Add the headers to the new csv
            writer.writerow(columns)

    return None


def hdfs_append(filepath: str, last_run: pd.DataFrame):
    """Function to append latest log metadata to csv in DAP

    Args:
        filepath (string): The filepath in Hue
        last_run (Dataframe): Dataframe of latest run data
    """

    # Open the file in read mode inside Hadoop context
    with hdfs.open(filepath, "r") as file:
        # Import csv file and convert to Dataframe
        df_imported_from_hdfs = pd.read_csv(file)
        # Append new data
        last_run_data = df_imported_from_hdfs.append(last_run)

    # Open the same file in write mode
    with hdfs.open(filepath, "wt") as file:
        # Write new updated dataframe to DAP context
        last_run_data.to_csv(file, index=False)

    return None

    return None


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
