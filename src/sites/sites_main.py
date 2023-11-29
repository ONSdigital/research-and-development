"""The main file for apportionment to sites module."""
import logging
#from numpy import random
#from typing import Callable, Tuple
#from datetime import datetime
import pandas as pd
#import os

#from src.staging import spp_parser, history_loader
#from src.staging import spp_snapshot_processing as processing
#from src.staging import validation as val
#from src.staging import pg_conversion as pg
#from src.utils.wrappers import time_logger_wrap

StagingMainLogger = logging.getLogger(__name__)

def run_sites(
    config: dict,
    df: pd.DataFrame,
    #check_file_exists: Callable,
    #load_json: Callable,
    #read_csv: Callable,
    #write_csv: Callable,
    #read_feather: Callable,
    #write_feather: Callable,
    #isfile: Callable,
    #list_files: Callable,
    #run_id: int,
) -> pd.DataFrame:
    """Run the apportionment to sites module.

    For short forms, the percentage is set to 100% on all instances.
    For long forms, the percentage is used to apportion the variables from 
    instance 0 to all other instances.
    Same percentages are used for each product group.

    When running on the local network,

    Args:
        config (dict): The pipeline configuration
        df (pd.DataFrame): Main dataset before the outputs
    Returns:
        df_out (pd.DataFrame): Percentages filled in for short forms and applied
        to apportion  for long forms
    """

    # Definitions
    #ref_col = "reference"
    #ins_col = "instance"
    #period_col = "period"
    pecent_col = "602"
    form_col = "formtype"
    short_code = 6
    #long_form = 1

    # Check if this module needs to be applied
    if config["global"]["prorate_sites"]:
        OutputMainLogger.info("Starting pro-rating to sites...")
    
        # Apply 100 percents to short forms
        df_out = df.copy()
        cond = df_out[form_col] == short_code
        df_out[pecent_col].mask(cond, other=100, inplace=True)
        OutputMainLogger.info("Short forms assigned 100 pecent.")

        # Return the result
        OutputMainLogger.info("Pro-rating to sites finished.")
        return df_out
    else:
        OutputMainLogger.info("Pro-rating disabled and skipped.")
        return df