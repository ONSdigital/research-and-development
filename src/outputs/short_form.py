"""The main file for the Outputs module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any

import src.outputs.map_output_cols as map_o
from src.staging.validation import load_schema
from src.imputation.imputation_helpers import fill_sf_zeros
from src.outputs.outputs_helpers import create_output_df, create_period_year


OutputMainLogger = logging.getLogger(__name__)


def create_headcount_cols(
    df: pd.DataFrame,
    round_val: int = 4,
) -> pd.DataFrame:
    """Create new columns with headcounts for civil and defence.

    Column '705' contains the total headcount value, and
    from this the headcount values for civil and defence are calculated
    based on the percentages of civil and defence in columns '706' (civil)
    and '707' (defence). Note that columns '706' and '707' measure different
    things to '705' so will not in general total to the '705' value.

    Args:
        df (pd.DataFrame): The survey dataframe being prepared for
            short form output.
        round_val (int): The number of decimal places for rounding.

    Returns:
        pd.DataFrame: The dataframe with extra columns for civil and
            defence headcount values.
    """
    # fill nulls with zeros for numerical rows
    df = fill_sf_zeros(df)

    headcount_tot_mask = (df["706"] + df["707"]) > 0

    df.loc[(headcount_tot_mask), "headcount_civil"] = (
        df.copy()["705"] * df.copy()["706"] / (df.copy()["706"] + df.copy()["707"])
    )
    df.loc[~(headcount_tot_mask), "headcount_civil"] = 0

    df.loc[(headcount_tot_mask), "headcount_defence"] = (
        df.copy()["705"] * df.copy()["707"] / (df.copy()["706"] + df.copy()["707"])
    )
    df.loc[~(headcount_tot_mask), "headcount_defence"] = 0

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

    # create columns for headcounts for civil and defense
    df = create_headcount_cols(df, round_val)

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

    # Create a 'year' column
    df = create_period_year(df)

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
    schema_path = config["schema_paths"]["shortform_schema"]
    schema_dict = load_schema(schema_path)
    shortform_output = create_output_df(df, schema_dict)

    tdate = datetime.now().strftime("%Y-%m-%d")
    filename = f"output_short_form_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_short_form/{filename}", shortform_output)
