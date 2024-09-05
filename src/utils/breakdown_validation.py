"""Function to validate the breakdown totals."""
import os
import logging
import pandas as pd
import numpy as np
from pandas import DataFrame as pandasDF

BreakdownValidationLogger = logging.getLogger(__name__)

equals_checks = {
    'check1': ['222', '223', '203'],
    'check2': ['202', '203', '204'],
    'check3': ['205', '206', '207', '204'],
    'check4': ['219', '220', '209', '210'],
    'check5': ['204', '210', '211'],
    'check6': ['212', '214', '216', '242', '250', '243', '244', '245', '246', '247', '248', '249', '218'],
    'check7': ['211', '218'],
    'check8': ['225', '226', '227', '228', '229', '237', '218'],
    'check9': ['302', '303', '304', '305'],
    'check10': ['501', '503', '505', '507'],
    'check11': ['502', '504', '506', '508'],
    'check12': ['405', '407', '409', '411'],
    'check13': ['406', '408', '410', '412'],
}

greater_than_checks = {
    'check14': ['209', '221'],
}


def replace_nulls_with_zero(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace nulls with zeros where the total is zero.

    Args:
        df (pd.DataFrame): The dataframe to check.

    Returns:
        pd.DataFrame
    """
    BreakdownValidationLogger.info("Replacing nulls with zeros where total zero")
    for check, columns in equals_checks.items():
        total_column = columns[-1]
        breakdown_columns = columns[:-1]
        for index, row in df.iterrows():
            if row[total_column] == 0:
                for column in breakdown_columns:
                    if pd.isnull(row[column]):
                        df.at[index, column] = 0
    return df


def remove_all_nulls_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows where all breakdown/total cols are null from validation.

    Args:
        df (pd.DataFrame): The dataframe to check.

    Returns:
        pd.DataFrame
    """
    BreakdownValidationLogger.info("Removing rows with all null values from validation")
    rows_to_validate = df.dropna(
        subset=[
            '222', '223', '203', '202', '204', '205', '206', '207', '221', '209', '219',
            '220', '210', '204', '211', '212', '214', '216', '242', '250', '243', '244',
            '245', '246', '218', '225', '226', '227', '228', '229', '237', '302', '303',
            '304', '305', '501', '503', '505', '507', '502', '504', '506', '508', '405',
            '407', '409', '411', '406', '408', '410', '412'
            ], how='all').reset_index(drop=True)
    return rows_to_validate


def equal_validation(rows_to_validate: pd.DataFrame) -> pd.DataFrame:
    """
    Check where the sum of some columns should equal another column.

    Args:
        rows_to_validate (pd.DataFrame): The dataframe to check.

    Returns:
        tuple(str, int)
    """
    BreakdownValidationLogger.info("Doing breakdown total checks...")
    msg = ""
    count = 0
    for index, row in rows_to_validate.iterrows():
        for key, columns in equals_checks.items():
            total_column = columns[-1]
            breakdown_columns = columns[:-1]
            if rows_to_validate[columns].isnull().all(axis=1).iloc[index] or (rows_to_validate[columns] == 0).all(axis=1).iloc[index]:
                continue
            if not (rows_to_validate[breakdown_columns].sum(axis=1) == rows_to_validate[total_column]).iloc[index]:
                msg += f"Columns {breakdown_columns} do not equal column {total_column} for reference: {row['reference']}, instance {row['instance']}.\n "
                count += 1
    return msg, count


def greater_than_validation(
    rows_to_validate: pd.DataFrame,
    msg: str,
    count: int
    ) -> pd.DataFrame:
    """
    Check where one value should be greater than another.

    Args:
        rows_to_validate (pd.DataFrame): The dataframe to check.
        msg (str): The message to append to.
        count (int): The count of errors.

    Returns:
        pd.DataFrame
    """
    BreakdownValidationLogger.info("Doing checks for values that should be greater than...")
    for index, row in rows_to_validate.iterrows():
        for key, columns in greater_than_checks.items():
            should_be_greater = columns[0]
            should_not_be_greater = columns[1]
            if (rows_to_validate[should_not_be_greater] > rows_to_validate[should_be_greater]).all():
                msg += f"Column {should_not_be_greater} is greater than {should_be_greater} for reference: {row['reference']}, instance {row['instance']}.\n "
                count += 1
    return msg, count


def run_breakdown_validation(
    df: pd.DataFrame,
    check: str = 'all'
    ) -> pd.DataFrame:
    """
    Function to validate the breakdown totals.

    Args:
        df (pd.DataFrame): The dataframe to check.
        check (str): The type of validation to run. Default is 'all'.

    Returns:
        pd.DataFrame
    """
    if check == 'all':
        df = replace_nulls_with_zero(df)

    rows_to_validate = remove_all_nulls_rows(df)
    msg, count = equal_validation(rows_to_validate)
    msg, count = greater_than_validation(rows_to_validate, msg, count)

    BreakdownValidationLogger.info(f"There are {count} errors with the breakdown values")

    if not msg:
        BreakdownValidationLogger.info("All breakdown values are valid.")
    else:
        raise ValueError(msg)


    return df
