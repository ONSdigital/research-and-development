"""Implement apportionment of headcount and FTE."""
import logging
import pandas as pd
import numpy as np

from typing import Dict, List

ApportionmentLogger = logging.getLogger(__name__)


def calc_202_totals(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate subtotals of q202 by reference.

    For each reference, the sum over all instances for q202 is 
    calculated and entered in column tot_202_all. 
    Also, for each reference, the sums over all instances 
    for just the civil or just defence is entered in column tot_202_CD.
    
    Args:
        df (pd.DataFrame): The main dataset for apportionment.

    Returns:
        pd.DataFrame: The main dataset with 202 subtotals.
    """
    df["tot_202_all"] = df.groupby("reference")["202"].transform(sum)

    df["tot_202_CD"] = df.groupby(["reference", "200"])["202"].transform(sum)

    return df


def update_column(df: pd.DataFrame, col: str) -> pd.Series:
    """Copy item in insance 0 to all other instances in a given reference.

    For long form entries, questions 405 - 412 and 501 - 508 are recorded
    in instance 0. A series is returned representing the updated column with
    values from instance 0 copied to all other instances of a reference.

    Args:
        df (pd.DataFrame): The main dataset for apportionment.
        col (str): The name of the 4xx or 5xx column being treated.

    Returns:
        pd.Series: A single column dataframe with the values in instance 0 
        copied to other instances for the same reference.
    """
    updated_col = df.groupby("reference")[col].transform(sum)
    return updated_col


def calc_fte_column(
    df: pd.DataFrame, 
    fte_dict: Dict[str, List[str]], 
    round_val: int = 4
) -> pd.DataFrame:
    """Create new columns for FTE values.

    8 new columns are created, 4 each for Civil and Defence.

    Args:
        df (pd.DataFrame): The main dataset for apportionment.
        fte_dict (Dict[str, List[str]]): A dictionary containing the new
            column name as key and a list of the old column names as value.
        round_val (int): The number of decimal places for rounding.

    Returns:
        pd.Dataframe: The dataset with new columns for FTE.    
    """

    # create new apportionment column for each item in the dictionary
    for new_col, old_cols in fte_dict.items():
        # take the civil and defence cases in turn
        cases_list = [["C", 0], ["D", 1]]
        for item in cases_list:
            condition = (df["200"] == item[0]) & (df["tot_202_CD"] > 0)
            df.loc[condition, new_col] = round(
                update_column(
                    df, old_cols[item[1]]
                ) * df["202"] / df["tot_202_CD"],
                round_val
            )

    return df


def calc_headcount_column(
    df: pd.DataFrame, 
    hc_dict: Dict[str, str], 
    round_val=4
) -> pd.DataFrame:
    """Create a new column for headcount values.

    Args:
        df (pd.DataFrame): The main dataset for apportionment.
        fc_dict (Dict[str, str]): A dictionary containing the new
            column name as key and old column names as the value.
        round_val (int): The number of decimal places for rounding.
    Returns:
        pd.Dataframe: The dataset with one new column for headcounts
    """

    # create new apportionment column (avoid division by 0)
    for new_col, old_col in hc_dict.items():
        df.loc[df["tot_202_all"] > 0, new_col] = round(
            update_column(df, old_col) * df["202"] / df["tot_202_all"],
            round_val
        )

    return df


def apportion_fte(df: pd.DataFrame, round_val=4) -> pd.DataFrame:
    """Call funtion to apportion FTE values.

    FTE is Full Time Equivalent and the values from instance 0
    are apportioned to other instances, based on values in 
    the 4xx columns (405 - 412)
    
    Args:
        df (pd.DataFrame): The main dataset for apportionment.
        round_val (int): The number of decimal places for rounding.
    Returns:
        pd.Dataframe: The dataset with all the new columns for FTE.   
    """
    fte_dict = {
        "emp_researcher": ["405", "406"],
        "emp_technician": ["407", "408"],
        "emp_other": ["409", "410"],
        "emp_total": ["411", "412"],
    }

    df = calc_fte_column(df, fte_dict, round_val=4)

    return df



def apportion_headcounts(df: pd.DataFrame) -> pd.DataFrame:
    """Call function to apportion headcount.

    The values from instance 0 are apportioned to other instances, 
    based on values in the 5xx columns (501 - 508)
    9 new columns are created, and Civil and Defence are treated together.

    Args:
        df (pd.DataFrame): The main dataset for apportionment.
    Returns:
        pd.Dataframe: The dataset with new columns headcounts
    """
    hc_dict = {
        "headcount_res_m": "501",
        "headcount_res_f": "502",
        "headcount_tec_m": "503",
        "headcount_tec_f": "504",
        "headcount_oth_m": "505",
        "headcount_oth_f": "506",
        "headcount_tot_m": "507", 
        "headcount_tot_f": "508"
    }

    df = calc_headcount_column(df, hc_dict, round_val=4)

    df["headcount_total"] = df["headcount_tot_m"] + df["headcount_tot_f"]

    return df

def run_apportionment(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate apportionment for headcount and FTE.
    
    Args:
        df (pd.DataFrame): The main dataset for apportionment.

    Returns:
        pd.DataFrame: The main dataset with new apportionment columns.
    """
    df = calc_202_totals(df)
    df = apportion_fte(df)
    df = apportion_headcounts(df)
    
    # drop temporary columns
    df = df.drop(["tot_202_all", "tot_202_CD"], axis=1)

    return df