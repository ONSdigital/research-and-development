"""Function to validate the breakdown totals from the imputation."""
import os
import logging
import pandas as pd
import numpy as np
from pandas import DataFrame as pandasDF

BreakdownValidationLogger = logging.getLogger(__name__)

def zero_totals(df : pd.DataFrame) -> pd.DataFrame:
    """ Function to fill in the breakdown columns with 0 if the total of the breakdown columns is 0.
    
    Args:
        df - the BERD 2023 form dataframe
    Returns:
        A dataframe with values filled in.   

    """
        df.loc[df['203'] == 0, ['222', '223']] = 0
        df.loc[df['204'] == 0, ['202', '203', '205', '206', '207']] = 0
        df.loc[df['210'] == 0, ['219', '220', '209']] = 0
        df.loc[df['211'] == 0, ['204', '210']] = 0
        df.loc[df['218'] == 0, ['212', '214', '216', '242', '250', '243', '244', '245', '246', '211', '225', '226', '227', '228', '229', '237']] = 0
        df.loc[df['305'] == 0, ['302', '303', '304']] = 0
        df.loc[df['507'] == 0, ['501', '503', '505']] = 0
        df.loc[df['508'] == 0, ['502', '504', '506']] = 0
        df.loc[df['411'] == 0, ['405', '407', '409']] = 0
        df.loc[df['412'] == 0, ['406', '408', '410']] = 0

        return df

def breakdown_checks(no_null_df : pd.DataFrame) -> dict:
    """Function to check that the breakdown values match the criteria provided. 
    
    Args:
        df - the BERD 2023 form dataframe that is to be checked
    Returns:
        A boolean dictionary and messages which update based upon if there are any
        issues with the logic of the columns.   
    
    """
    df = no_null_df.copy()
    df = zero_totals(df)
    
    df['check1'] = df['222'] + df['223'] != df['203'] 
    df['check2'] = df['202'] + df['203'] != df['204'] 
    df['check3'] = df['205'] + df['206'] + df['207'] != df['204'] 
    df['check4'] = df['221'] > df['209']
    df['check5'] = df['219'] + df['220'] + df['209'] != df['210'] 
    df['check6'] = df['204'] + df['210'] != df['211'] 
    df['check7'] = (df['212'] + df['214'] + df['216'] + df['242'] + df['250'] + df['243'] + df['244'] + df['245'] + df['246'] != df['218']) & (df['211'] != df['218']) 
    df['check8'] = df['225'] + df['226'] + df['227'] + df['228'] + df['229'] + df['237'] != df['218'] 
    df['check9'] = df['302'] + df['303'] + df['304'] != df['305'] 
    df['check10'] = df['501'] + df['503'] + df['505'] != df['507'] 
    df['check11'] = df['502'] + df['504'] + df['506'] != df['508'] 
    df['check12'] = df['405'] + df['407'] + df['409'] != df['411'] 
    df['check13'] = df['406'] + df['408'] + df['410'] != df['412'] 
    
    if df['221'] is None and df['209'] is None:
        df['check4'] == False
    else:
        df['check4']
        
    return df



    def breakdown_validation(no_null_df : pd.DataFrame) -> dict:
    """Function to check that the breakdown values match the criteria provided. 
    
    Args:
        df - the BERD 2023 form dataframe that is to be checked
    Returns:
        A boolean dictionary and messages which update based upon if there are any
        issues with the logic of the columns.   
    
    """
    bool_dict = {}
    msg = ""
    
    df = no_null_df.copy()
    df = breakdown_checks(df)

    issues_df = df[df.isin([True]).any(axis=1)] # Filter dataframe to only include rows with issues with logic

    for index, row in issues_df.iterrows():
        bool_dict[index] = True
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
        if True in bool_dict.values():
            bool_dict[index] = True
            msg += f"All breakdown values are valid for references: {row['reference']}.\n"
    
    return bool_dict, msg

def run_breakdown_validation(df: pd.DataFrame) -> pd.DataFrame:
    """Runs the breakdown_validation function and outputs the msg to the logger"""
    # not_null_df = df.dropna(subset=['222', '223', '203', '202', '204', '205', '206', '207', '221', '209', '219', '220', '210', '204', '211', '212', 
    # '214', '216', '242', '250', '243', '244', '245', '246', '218', '225', '226', '227', '228', '229', '237', '302', '303', '304', '305', '501', '503', 
    # '505', '507', '502', '504', '506', '508', '405', '407', '409', '411', '406', '408', '410', '412'], 
    # how = 'all')

    bool_dict, msg = breakdown_validation(not_null_df) 

    issue_counts = sum(value == False for value in bool_dict.values())

    if all(bool_dict.values()):
        BreakdownValidationLogger.info("All breakdown values are valid.\n")
    else:
        BreakdownValidationLogger.error(f"There are {issue_counts} errors with the breakdown values, please make the adjustments for the references that have issues.\n")
        raise ValueError(msg)

    return df