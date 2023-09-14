"""Implement apportionment of headcount and FTE."""
import logging
import pandas as pd

ApportionmentLogger = logging.getLogger(__name__)


def calc_202_totals(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate totals of q202 by reference.
    
    Args:
        df (pd.DataFrame): The main dataset for apportionment.

    Returns:
        pd.DataFrame: The main dataset with totals.
    """
    df["202_tot_all"] = df.groupby("reference")["202"].transform(sum)
    df["202_tot_CD"] = df.groupby(["reference", "200"])["202"].transform(sum)
    print(df.head())
    return df


def run_apportionment(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate apportionment for headcount and FTE.
    
    Args:
        df (pd.DataFrame): The main dataset for apportionment.

    Returns:
        pd.DataFrame: The main dataset with new apportionment columns.
    """
    df_totals = calc_202_totals(df)
    return df