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


    def create_headcount_cols(
        df: pd.DataFrame, 
        round_val: int = 4,
    ) -> pd.DataFrame:
        """Create new columns with headcounts for civil and defence.

        Column '705' contains the total headcount value, and 
        from this the headcount values for civil and defence are calculated
        based on the percentages of civil and defence in columns '706' (civil)
        and '707' (defence).

        Args:
            df: The survey dataframe being prepared for short form output.

        Returns:
            pd.DataFrame: The same dataframe with extra columns for civil and
                defence headcount values.
        """
        df["headcount_civil"] = round(
            df["705"] * df["706"]/(df["706"] + df["707"]), round_val
        )

        df["headcount_defence"] = round(
            df["705"] * df["707"]/(df["706"] + df["707"]), round_val
        )

        return df

