"""The main pipeline"""

import toml
from utils.testfunctions import add

def toml_parser():
    """Parse the toml config file"""
    return toml.load("/home/cdsw/research-and-development/config/userconfig.toml")

def period_select():
    """Selects period defined by user in userconfig.toml"""
    period_dict = toml_parser()["period"]
    return period_dict["start_period"], period_dict["end_period"]

def run_pipeline():
    """Run the pipeline"""
    return add(1, 2)
