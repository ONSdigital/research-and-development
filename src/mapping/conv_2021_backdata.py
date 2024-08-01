"""NOTE: This is a temporary script to convert the 2021 backdata to the format required
for MoR imputation. When the mapping module is complete, we can produce a one-off update
of the 2021 data and remove this script."""

import os
import re
import logging
import pandas as pd

from src.utils.local_file_mods import rd_read_csv, rd_write_csv
from src.staging import staging_helpers as stage_hlp
from src.staging import postcode_validation as pcval
from src.mapping.pg_conversion import pg_to_pg_mapper
from src.imputation.tmi_imputation import create_imp_class_col
from src.imputation.apportionment import run_apportionment

MappingMainLogger = logging.getLogger(__name__)


def do_pg_conv(backdata, config) -> pd.DataFrame:

    # Load and validate the PG mappers
    pg_num_alpha = stage_hlp.load_validate_mapper(
        "pg_num_alpha_mapper_path",
        config,
        MappingMainLogger,
    )

    backdata = pg_to_pg_mapper(
        backdata,
        pg_num_alpha,
    )
    return backdata


def prep_2021_backdata(backdata) -> pd.DataFrame:
    """Prepare the backdata for MoR imputation.

    Args:
        backdata (pd.DataFrame): Backdata for the current year.

    Returns:
        pd.DataFrame: Prepped backdata.
    """
    # Convert backdata column names from qXXX to XXX
    # Note that this is only applicable when using the backdata on the network
    p = re.compile(r"q\d{3}")
    cols = [col for col in list(backdata.columns) if p.match(col)]
    to_rename = {col: col[1:] for col in cols}
    backdata = backdata.rename(columns=to_rename)

    # Apply the postcode formatting to clean the postcodes in col 601 of the back data
    backdata["601"] = backdata["601"].apply(pcval.format_postcodes)

    return backdata


def get_backdate_wanted_cols(backdata: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Get the columns required for the backdata.

    Args:
        backdata (pd.DataFrame): The backdata.
        config (dict): The configuration settings.

    Returns:
        pd.DataFrame: The backdata with only the required columns.
    """
    # Load the columns to keep
    backdata_cols = stage_hlp.load_required_columns(
        "backdata_required_cols_path",
        config,
        MappingMainLogger,
    )

    # Get the columns that are in the backdata
    cols = list(backdata.columns)
    wanted_cols = [col for col in backdata_cols if col in cols]

    return backdata[wanted_cols]


def create_imp_marker_col(df: pd.DataFrame) -> pd.DataFrame:
    """Create the imp_marker column for the backdata.

    Args:
        df (pd.DataFrame): The backdata.

    Returns:
        pd.DataFrame: The backdata with the imp_marker column.
    """
    clear_responders_mask = df.status.isin(["Clear", "Clear - overridden"])
    df.loc[clear_responders_mask, "imp_marker"] = "R"
    df.loc[~clear_responders_mask, "imp_marker"] = "no_imputation"

    return df


def create_backdata(backdata: pd.DataFrame, config: dict) -> pd.DataFrame:
    staging_dict = config["staging_paths"]
    backdata_path = staging_dict["backdata_path"]

    backdata = rd_read_csv(backdata_path)

    backdata = prep_2021_backdata(backdata)

    backdata = do_pg_conv(backdata, config)

    backdata = run_apportionment(backdata)

    backdata = create_imp_class_col(backdata, "200", "201")

    backdata = create_imp_marker_col(backdata)

    backdata_out_path = config["imputation_paths"]["backdata_out_path"]

    rd_write_csv(os.path.join(backdata_out_path, "2021_backdata.csv"), backdata)
