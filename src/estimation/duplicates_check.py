import pandas as pd
import logging
import sys

DupLogger = logging.getLogger(__name__)

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
        [col]#df.columns.tolist()
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

    # obtain counts of unique rows
    # and filter for any not 1
    duplicates = count_unique(df, col)
    duplicates = duplicates[duplicates["count"] != 1]
    
    if len(duplicates) > 0:
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
        DupLogger.info(
            (
                f"There are no duplicate rows in the data"

            )
        )
