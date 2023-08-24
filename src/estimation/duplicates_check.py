import pandas as pd
import logging
import sys

DupLogger = logging.getLogger(__name__)


def filter_short_forms(df: pd.DataFrame, form_type_no = "0006") -> pd.DataFrame:
    '''Returns dataframe with filterred for short 
    forms formtype = "0006"

    Args:
        df (pd.DataFrame): Pandas dataframe
        with responses data
        form_type_no (str): defaults to "0006"
        denoting short forms

    Returns:
        df (pd.DataFrame): Pandas dataframe
        with just short form data 
    '''
    # filter for just form_type_no = "0006"
    df = df[df["formtype"] == form_type_no].copy()

    return df


def count_unique(df: pd.DataFrame, col:str) -> pd.DataFrame:
    '''Returns dataframe with count column
    giving count of unique row

    Args:
        df (pd.DataFrame): Pandas dataframe
        with responses data
        col (str): name of column to check for
        duplicates instances

    Returns:
        duplicates (pd.DataFrame): Pandas dataframe
        with duplicate rows or empty dataframe 
        if no duplicates exist 
    '''

    # group by all columns and
    # add col "count" with count of unique rows
    duplicates = df.groupby(
        [col]
        ).size().reset_index().rename(columns={0:'count'})

    return duplicates


def duplicates_check(df: pd.DataFrame, col:str) -> pd.DataFrame:
    '''Creates error message with duplicates
    if they exist. If not gives an info message

    Args:
        df (pd.DataFrame): Pandas dataframe
        with responses data
        col (str): name of column to check for
        duplicates instances
    '''
    # filter short forms
    df = filter_short_forms(df)
    # obtain counts of unique rows
    # and filter for any not 1
    duplicates = count_unique(df, col)
    duplicates = duplicates[duplicates["count"] != 1]

    if len(duplicates) > 0:
        print("within if")
        DupLogger.error(
            (
                "The following dataframe shows duplicate rows "
                "(see the count column for a count of duplicate rows)"
                f": {duplicates}"
            )
        )

        #exit pipeline if any duplicates
        sys.exit()
    else:
        print("wihtin else")
        DupLogger.info(
            (
                f"There are no duplicate rows in the data"
            )
        )
