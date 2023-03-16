"""The main pipeline"""

import toml
from utils.testfunctions import add


def toml_parser():
    """Function to parse the userconfig.toml file.

    Returns:
        A dictionary where the keys are section titles within the TOML file.
        If only one variable under the section title in the TOML file is given
        then it is passed directly as a dictionary value. If more than one
        variable is defined then they are parsed as a dictionary themselves.
        An example of what is returned is given below:

        {'title': 'TOML Example config', 'period': {'start_period':
        datetime.date(1990, 10, 10), 'end_period': datetime.date(2000, 10, 5)}}
    """
    return toml.load("/home/cdsw/research-and-development/config/userconfig.toml")


def run_pipeline():
    """Run the pipeline"""
    return add(1, 2)
