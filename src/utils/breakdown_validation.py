"""Function to validate the breakdown totals from the imputation."""
import os
import logging
import pandas as pd

BreakdownValidationLogger = logging.getLogger(__name__)

def breakdown_validation(df: pd.DataFrame) -> dict:
    """Function to check that the breakdown values match the criteria provided. 
    
    Args:
        df - the BERD 2023 form dataframe that is to be checked
    Returns:
        A boolean dicttionary and messages which update based upon if there are any
        issues with the logic of the columns.   
    
    """
    bool_dict = {}
    msg = ""

    df['check1'] = df['222'] + df['223'] != df['203'] #
    df['check2'] = df['202'] + df['203'] != df['204'] #
    df['check3'] = df['205'] + df['206'] + df['207'] != df['204'] #
    df['check4'] = df['221'] > df['209'] #
    df['check5'] = df['219'] + df['220'] + df['209'] != df['210'] #
    df['check6'] = df['204'] + df['210'] != df['211'] #
    df['check7'] = (df['212'] + df['214'] + df['216'] + df['242'] + df['250'] + df['243'] + df['244'] + df['245'] + df['246'] != df['218']) & (df['211'] != df['218']) #
    df['check8'] = df['225'] + df['226'] + df['227'] + df['228'] + df['229'] + df['237'] != df['218'] #
    df['check9'] = df['302'] + df['303'] + df['304'] != df['305'] #
    df['check10'] = df['501'] + df['503'] + df['505'] != df['507'] #
    df['check11'] = df['502'] + df['504'] + df['506'] != df['508'] #
    df['check12'] = df['405'] + df['407'] + df['409'] != df['411'] #
    df['check13'] = df['406'] + df['408'] + df['410'] != df['412'] #

    false_df = df[df.isin([True]).any(axis=1)]
    # if rows is all null raise error (missingness)
    if len(false_df) > 0 :
        msg += "There are issues with the logic of the columns.\n "
    else:
        msg += "There are no issues with the logic of the columns.\n"

    for index, row in false_df.iterrows():
        if row['check1']:
            bool_dict[index] = False
            msg += f"Columns 222 + 223 do not equal column 203 for reference: {row['reference']}.\n "
        if row['check2']:
            bool_dict[index] = False
            msg += f"Columns 202 + 223 do not equal column 204 for reference: {row['reference']}.\n "
        if row['check3']:
            bool_dict[index] = False
            msg += f"Columns 205 + 206 + 207 do not equal column 204 for reference: {row['reference']}.\n "
        if row['check4']:
            bool_dict[index] = False
            msg += f"Columns 221 is greater than column 209 for reference: {row['reference']}.\n "
        if row['check5']:
            bool_dict[index] = False
            msg += f"Columns 219 + 220 + 209 do not equal column 210 for reference: {row['reference']}.\n "
        if row['check6']:
            bool_dict[index] = False
            msg += f"Columns 204 + 210 + 209 do not equal column 211 for reference: {row['reference']}.\n "
        if row['check7']:
            bool_dict[index] = False
            msg += f"Columns 212 + 246 do not equal 218 and Columns 212 + 246 do not equal column 211 for reference: {row['reference']}.\n "
        if row['check8']:
            bool_dict[index] = False
            msg += f"Columns 225 + 226 + 227 + 228 + 229 + 237 do not equal column 218 for reference: {row['reference']}.\n "
        if row['check9']:
            bool_dict[index] = False
            msg += f"Columns 302 + 303 + 304 do not equal column 305 for reference: {row['reference']}.\n "
        if row['check10']:
            bool_dict[index] = False
            msg += f"Columns 501 + 503 + 505 do not equal column 507 for reference: {row['reference']}.\n "
        if row['check11']:
            bool_dict[index] = False
            msg += f"Columns 502 + 504 + 506 do not equal column 508 for reference: {row['reference']}.\n "
        if row['check12']:
            bool_dict[index] = False
            msg += f"Columns 405 + 407 + 409 do not equal column 411 for reference: {row['reference']}.\n "
        if row['check13']:
            bool_dict[index] = False
            msg += f"Columns 406 + 408 + 410 do not equal column 412 for reference: {row['reference']}.\n " 

    return bool_dict, msg


def run_breakdown_validation(df: pd.DataFrame) -> pd.DataFrame:
    """Runs the breakdown_validation function and outputs the msg to the logger"""
    bool_dict, msg = breakdown_validation(df)

    if all(bool_dict.values()):
        BreakdownValidationLogger.info("All breakdown values are valid.\n")
    else:
        BreakdownValidationLogger.error("There are errors with the breakdown values, please make the adjustments for the references that have issues.\n")
        raise ValueError(msg)

    return df