"""Implement apportionment of headcount and FTE."""
import logging
import pandas as pd
import numpy as np

ApportionmentLogger = logging.getLogger(__name__)


def calc_202_totals(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate totals of q202 by reference.

    For each reference, the sum over all instances for q202 is 
    calculated and entered in column qtot_202_all for each instance. 
    Also, the sums over all instances for just the civil or just 
    defence is entered in column qtot_202_CD.
    
    Args:
        df (pd.DataFrame): The main dataset for apportionment.

    Returns:
        pd.DataFrame: The main dataset with totals.
    """
    total_202_all = df.groupby("reference")["202"].transform(sum)
    df["tot_202_all"] = np.where(total_202_all >0, total_202_all, np.nan)

    total_202_CD = df.groupby(["reference", "200"])["202"].transform(sum)
    df["tot_202_CD"] = np.where(total_202_CD >0, total_202_CD, np.nan)

    return df


def update_column(df: pd.DataFrame, col: str) -> pd.Series:
    """Copy item in insance 0 to all other instances in a given reference.

    For long form entries, questions 405 - 412 and 501 - 508 are recorded
    in instance 0. A series is returned representing an updated version of the 
    column with the values from instance 0 copied to all other instances.

    Args:
        df (pd.DataFrame): The main dataset for apportionment.
        col (str): The name of the 4xx or 5xx column being treated.

    Returns:
        pd.Series: A series with the values in instance 0 copied to
            other instances for the same reference.
    """
    updated_col = df.groupby("reference")[col].transform(sum)
    return updated_col


def calc_fte_column(df: pd.DataFrame, round_val=4) -> pd.DataFrame:
    """Create a new column for FTE values.

    FTE is Full Time Equivalent and the values from instance 0
    are apportioned to other instances, based on values in 
    the 4xx columns (405 - 412)
    8 new columns are created, 4 each for Civil and Defence.

    Args:
        df (pd.DataFrame): The main dataset for apportionment.
    Returns:
        pd.Dataframe: The dataset with new columns for FTE.    
    """
    #TODO: Generalise this function to pass a dictionary {newcol:oldcol} 
    #TODO and whether C or D.
    df.loc[df["200"] == "C", "emp_researcher"] = round(
        update_column(df, "405") * df["202"] / df["tot_202_CD"],
        round_val
    )
    return df



def apportion_headcounts(df: pd.DataFrame) -> pd.DataFrame:
    """Create new columns for FTE values.

    The values from instance 0 are apportioned to other instances, 
    based on values in the 5xx columns (501 - 508)
    9 new columns are created, and Civil and Defence are treated together.

    Args:
        df (pd.DataFrame): The main dataset for apportionment.
    Returns:
        pd.Dataframe: The dataset with new columns headcounts
    """
    #TODO: just one column created so far and code not tested
    df["headcount_res_m"] = (
        update_column(df, "501") * df["202"] / df["tot_202_all"]
    )
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