"""Prepare the survey data for short form microdata output."""
import pandas as pd
import numpy as np
from src.staging.validation import load_schema, validate_data_with_schema


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
    df: pd.DataFrame, fte_civil, fte_defence, hc_total, round_val=4
) -> pd.DataFrame:
    """Create new columns with headcounts for civil and defence.

    The column represented by hc_total contains the total headcount value, and
    from this the headcount values for civil and defence are calculated
    based on the percentages of civil and defence in fte_civil (civil)
    fte_defence (defence). Note that fte_civil and fte_defence measure different
    things to hc_total so will not in general total to the hc_total value.

    Args:
        df (pd.DataFrame): The survey dataframe being prepared for
            short form output.
        fte_civil (str): Column containing percentage of civil.
        fte_defence (str): Column containing percentage of defence.
        hc_total (str): Column containing total headcount value.
        round_val (int): The number of decimal places for rounding.

    Returns:
        pd.DataFrame: The dataframe with extra columns for civil and
            defence headcount values.
    """
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


def create_shortform_df(df: pd.DataFrame, schema: str) -> pd.DataFrame:
    """Creates the shortform dataframe for outputs with
    the required columns. The naming of the columns comes
    from the schema provided.

    Args:
        df (pd.DataFrame): Dataframe containing all columns
        schema (str): Toml schema containing the old and new
        column names for the outputs

    Returns:
        (pd.DataFrame): A dataframe consisting of only the
        required short form output data
    """
    # Load schema using pre-built function
    output_schema = load_schema(schema)

    # Create dict of current and required column names
    colname_dict = {
        output_schema[column_nm]["old_name"]: column_nm
        for column_nm in output_schema.keys()
    }

    # Create subset dataframe with only the required outputs
    output_df = df[df.columns.intersection(colname_dict.keys())]

    # Rename columns to match the output specification
    output_df.rename(
        columns={key: colname_dict[key] for key in colname_dict}, inplace=True
    )

    # Rearrange to match user defined output order
    output_df = output_df[colname_dict.values()]

    # Validate datatypes before output
    validate_data_with_schema(output_df, schema)

    return output_df
