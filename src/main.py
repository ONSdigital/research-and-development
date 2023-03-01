"""The main pipeline"""

#from src.utils.testfunctions import add
import toml

def toml_parser():
    """Parse the toml config file"""
    return print( toml.load("research-and-development/config/userconfig.toml") )

#def run_pipeline():
#    """Run the pipeline"""
#    return add(1, 2)
