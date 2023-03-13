"""The main pipeline"""

import toml
from utils.testfunctions import add

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

def outlier_correction():
    """_summary_

    Returns:
        _description_
    """    
    outlier_dict = toml_parser()["outlier_correction"]
    return outlier_dict["location"], outlier_dict["fileName"], outlier_dict["bool"]

def run_pipeline():
    """Run the pipeline"""
    return add(1, 2)