import pandas as pd
import numpy as np

import logging


from src.utils.wrappers import time_logger_wrap, exception_wrap

# Set up logging
NIValidationLogger = logging.getLogger(__name__)


def rename_columns(df: pd.DataFrame, schema:dict):
    """Rename columns in the NI data using the schema."""
    

def validate_datatypes(df: pd.DataFrame, schema:dict):
    """Takes the schema from the toml file and validates the survey data df.

    Args:
        df (pd.DataFrame): Survey data in a pd.df format
        schema_path (str): path to the schema toml (should be in config folder)
    """
    NIValidationLogger.info(f"Starting validation with {schema}")
    # Load schema from toml

    # Create a dict for dtypes only
    dtypes_dict = {
        column_nm: schema[column_nm]["Deduced_Data_Type"]
        for column_nm in schema.keys()
    }

    # Cast each column individually and catch any errors
    for column in dtypes_dict.keys():

        # Fix for the columns which contain empty strings. We want to cast as NaN
        if dtypes_dict[column] == "pd.NA":
            # Replace whatever is in that column with np.nan
            df[column] = np.nan
            dtypes_dict[column] = "float64"

        try:
            NIValidationLogger.debug(f"{column} before: {df[column].dtype}")
            if dtypes_dict[column] == "Int64":
                # Convert non-integer string to NaN
                df[column] = df[column].apply(
                    pd.to_numeric, errors="coerce"
                )
                # Cast columns to Int64
                df[column] = df[column].astype(pd.Int64Dtype())
            elif dtypes_dict[column] == "str":
                df[column] = df[column].astype("string")
            else:
                df[column] = df[column].astype(dtypes_dict[column])
            NIValidationLogger.debug(f"{column} after: {df[column].dtype}")
        except Exception as e:
            NIValidationLogger.error(e)
    NIValidationLogger.info("Validation successful")