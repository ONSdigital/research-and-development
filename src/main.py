"""The main pipeline"""

import toml
from utils.testfunctions import add

def toml_parser():
    """Parse the toml config file"""
    return toml.load("/home/cdsw/research-and-development/config/userconfig.toml")

def run_pipeline():
    """Run the pipeline"""
    return add(1, 2)
