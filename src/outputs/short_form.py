"""The main file for the Outputs module."""
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Callable, Dict, Any

import src.outputs.map_output_cols as map_o
from src.outputs.outputs_helpers import create_output_df


OutputMainLogger = logging.getLogger(__name__)


def create_period_year(df: pd.DataFrame) -> pd.DataFrame:
    """Created year column for short form output

    The 'period_year' column is added containing the year in form 'YYYY'.

    Args:
        df (pd.DataFrame): The main dataframe to be used for short form output.

    Returns:
        pd.DataFrame: returns short form output data frame with added new col
    """

    # Extracted the year from period and crated new columns 'period_year'
    df["period_year"] = df["period"].astype("str").str[:4]

    return df


def create_headcount_cols(
    df_in: pd.DataFrame, fte_civil, fte_defence, hc_total, round_val=4
) -> pd.DataFrame:
    """Create new columns with headcounts for civil and defence.

    The column represented by hc_total contains the total headcount value, and
    from this the headcount values for civil and defence are calculated
    based on the percentages of civil and defence in fte_civil (civil)
    fte_defence (defence). Note that fte_civil and fte_defence measure different
    things to hc_total so will not in general total to the hc_total value.

    fte_civil will usually be 706, fte_defence will usually be 707, and
    hc_total will usually be 705.

    Args:
        df_in (pd.DataFrame): The survey dataframe being prepared for
            short form output.
        fte_civil (str): Column containing percentage of civil employees.
        fte_defence (str): Column containing percentage of defence employees.
        hc_total (str): Column containing total headcount value.
        round_val (int): The number of decimal places for rounding.

    Returns:
        pd.DataFrame: The dataframe with extra columns for civil and
            defence headcount values.
    """
    # Deep copying to avoid "returning a vew versus a copy" warning
    df= df_in.copy()

    # Use np.where to avoid division by zero.
    df["headcount_civil"] = np.where(
        df[fte_civil] + df[fte_defence] != 0,  # noqa
        df[hc_total] * df[fte_civil] / (df[fte_civil] + df[fte_defence]),
        0,
    )

    df["headcount_defence"] = np.where(
        df[fte_civil] + df[fte_defence] != 0,  # noqa
        df[hc_total] * df[fte_defence] / (df[fte_civil] + df[fte_defence]),
        0,
    )

    df["headcount_civil"] = round(df["headcount_civil"], round_val)
    df["headcount_defence"] = round(df["headcount_defence"], round_val)

    return df


def run_shortform_prep(
    df: pd.DataFrame,
    round_val: int = 4,
) -> pd.DataFrame:
    """Prepare data for short form output.

    Perform various steps to create new columns and modify existing
    columns to prepare the main survey dataset for short form
    micro data output.

    Args:
        df (pd.DataFrame): The survey dataframe being prepared for
            short form output.
        round_val (int): The number of decimal places for rounding.

    Returns:
        pd.DataFrame: The dataframe prepared for short form output.
    """

    # Filter for short-forms, CORA statuses and instance
    df = df.loc[
        (df["formtype"] == "0006")
        & (df["form_status"].isin(["600", "800"]))
        & (df["instance"] == 0)
    ]

    # Create a 'year' column
    df = create_period_year(df)

    # create columns for headcounts for civil and defense
    df = create_headcount_cols(
        df, "706_estimated", "707_estimated", "705_estimated", round_val
    )

    return df

def output_short_form(
    df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    ultfoc_mapper: pd.DataFrame,
    cora_mapper: pd.DataFrame,
    postcode_itl_mapper: pd.DataFrame,
):
    """Run the outputs module.

    Args:
        df (pd.DataFrame): The main dataset for short form output
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        ultfoc_mapper (pd.DataFrame): The ULTFOC mapper DataFrame.
        cora_mapper (pd.DataFrame): used for adding cora "form_status" column


    """

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    # Prepare the columns needed for outputs:

    # Join foriegn ownership column using ultfoc mapper
    df = map_o.join_fgn_ownership(df, ultfoc_mapper)

    # Map to the CORA statuses from the statusencoded column
    df = map_o.create_cora_status_col(df, cora_mapper)

    # Map the sizebands based on frozen employment
    df = map_o.map_sizebands(df)

    # Map the itl regions using the postcodes
    df = map_o.join_itl_regions(df, postcode_itl_mapper)

    # Map q713 and q714 to numeric format
    df = map_o.map_to_numeric(df)

    # Prepare the shortform output dataframe
    df = run_shortform_prep(df, round_val=4)

    # Create short form output dataframe with required columns from schema
    schema_path = config["schema_paths"]["frozen_shortform_schema"]
    shortform_output = create_output_df(df, schema_path)

    tdate = datetime.now().strftime("%Y-%m-%d")
    filename = f"output_short_form_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_short_form/{filename}", shortform_output)
