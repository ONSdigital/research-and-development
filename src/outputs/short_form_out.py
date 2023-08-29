import pandas as pd
import numpy as np


def create_new_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Created blank columns & year column for short form output

    Blank columns are created for short form output that are required
    but will currently be supplied empty.
    The 'period_year' column is added containing the year in form 'YYYY'.

    Args:
        df (pd.DataFrame): The main dataframe to be used for short form output.

    Returns:
        pd.DataFrame: returns short form output data frame with added new cols
    """
    columns_names = [
        "freeze_id",
        "inquiry_id",
        "period_contributor_id",
        "post_code",
        "ua_county",
        "foreign_owner",
        "product_group",
        "sizeband",
    ]

    # Added new columns from column_names list
    for col in columns_names:
        df[col] = np.nan

    # Extracted the year from period and crated new columns 'period_year'
    df["period_year"] = df["period"].astype("str").str[:4]

    return df
