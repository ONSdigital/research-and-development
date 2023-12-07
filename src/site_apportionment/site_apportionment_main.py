"""The main file for apportionment to sites module."""
import logging
# from numpy import random
# from typing import Callable, Tuple
# from datetime import datetime
import pandas as pd
# import os

# from src.staging import spp_parser, history_loader
# from src.staging import spp_snapshot_processing as processing
# from src.staging import validation as val
# from src.staging import pg_conversion as pg
# from src.utils.wrappers import time_logger_wrap

from src.site_apportionment.site_apportionment_helpers import (apply_short_percent)

SitesMainLogger = logging.getLogger(__name__)

# Make a dictionary of column names that are more human-readable
col_name_reference = {
    "ref": "reference",
    "ins": "instance",
    "period": "period",
    "form": "formtype",
    "postcode": "postcodes_harmonised",
    "percent": "602",
    "product": "201",
    "civdef": "200",
}


def run_site_apportionment(
    config: dict,
    df: pd.DataFrame,
    # check_file_exists: Callable,
    # load_json: Callable,
    # read_csv: Callable,
    # write_csv: Callable,
    # read_feather: Callable,
    # write_feather: Callable,
    # isfile: Callable,
    # list_files: Callable,
    # run_id: int,
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
    # ref_col = "reference"
    # ins_col = "instance"
    # period_col = "period"
    pecent_col = "602"
    form_col = "formtype"
    short_code = "0006"
    short_percent = 100.0

    # long_code = "0001"

    # Check if this module needs to be applied
    if config["global"]["prorate_sites"]:
        SitesMainLogger.info("Starting pro-rating to sites...")
        df_out = df.copy()

        # Apply 100 percent to short forms
        cond = df_out[form_col] == short_code
        short_forms_df = df_out.loc[cond]
        short_forms_df = apply_short_percent(short_forms_df, short_percent = short_percent)
        SitesMainLogger.info(f"Short forms assigned {short_percent}%.")
        
        # Apportionment of long forms 
        # Extract the long forms
        df = df[df[col_name_reference["form"]] == long_code]

# Count distinct non-empty codes
for code in ["postcode", "product", "civdef"]:
    df = count_unique_codes_in_col(df, code)

        # Return the result
        SitesMainLogger.info("Pro-rating to sites finished.")
        return df_out
    else:
        SitesMainLogger.info("Pro-rating disabled and skipped.")
        return df
