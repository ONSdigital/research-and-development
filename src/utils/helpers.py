"""Define helper functions that wrap regularly-used functions."""

import yaml
import toml
import os


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


def csv_creator(filename, columns) -> None:
    """Creates a csv file with user
    defined headers.
    Args:
        filename (string): Example: "name_of_file.csv"
        columns (list): Example: ["a","b","c","d"]
    """
    if not os.path.exists(filename):
        with open(filename, mode="w", encoding="utf-8") as f:
            f.write(",".join(columns) + "\n")
    return None


def toml_parser() -> dict:
    """Function to parse the userconfig.toml file.

    Returns:
        A dictionary where the keys are section titles within the TOML file.
        If only one variable under the section title in the TOML file is given
        then it is passed directly as a dictionary value. If more than one
        variable is defined then they are parsed as a dictionary themselves.
        An example of what is returned is given below:
        
        {'title': 'TOML Example config', 'period': {'start_period': 
        datetime.date(1990, 10, 10), 'end_period': datetime.date(2000, 10, 5)}, 
        'source_file': {'location': 'D:/Data', 'fileName': 'file.txt'}, 'output_location': 
        {'hive_db': 'hive.db', 'tableName': 'name.table'}, 'outlier_correction': 
        {'location': 'D:/', 'fileName': 'outliers.txt', 'bool': True}}
    """
    return toml.load("/home/cdsw/research-and-development/config/userconfig.toml")

def period_select() -> tuple:
    """Function returns the start and end date under consideration.

    Returns:
        A tuple containing two datetime.date objects. The first is the
        start date of the period under consideration, the second is the
        end date of that period. 
        Example:
    
        (datetime.date(1990, 10, 10), datetime.date(2000, 10, 5))
    """
    period_dict = toml_parser()["period"]
    return period_dict["start_period"], period_dict["end_period"]

def source_file() -> tuple:
    """Function returns the file path and file name of the source file

    Returns:
        A tuple containing two elements. The first is the file path to the source
        file, the second is the file name itself.
        Example of return values:

        ('D:/Data', 'file.txt')
    """
    source_dict = toml_parser()["source_file"]
    return source_dict["location"], source_dict["fileName"]

def output_loc() -> tuple:
    """Function returns the output location and table name

    Returns:
        A tuple containing two element. The first is the output location, the second
        is the name of the output table.
        Example of return values:
        
        ('hive.db', 'name.table')
    """
    output_dict = toml_parser()["output_location"]
    return output_dict["hive_db"], output_dict["tableName"]

def outlier_correction() -> tuple:
    """Function returns file path and file name of manual outlier correction file

    Returns:
        Tuple containing three elements. The first is the file path to where the manual
        outlier correction file is stored, the second is the name of the outlier
        correction file, and the third is a boolean value indicating whether to include
        the manual correction file or not.
        Example of return values:

        ('D:/', 'outliers.txt', True)
    """
    outlier_dict = toml_parser()["outlier_correction"]
    return outlier_dict["location"], outlier_dict["fileName"], outlier_dict["bool"]
