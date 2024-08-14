"""Function to validate the breakdown totals from the imputation."""
import os
import logging
import pandas as pd
from pandas import DataFrame as pandasDF


BreakdownValidationLogger = logging.getLogger(__name__)

def breakdown_validation(df: pd.DataFrame) -> dict:
    """Function to check that the breakdown values match the criteria provided. 
    
    Args:
        df - the BERD 2023 form dataframe that is to be checked
    Returns:
        A boolean dictionary and messages which update based upon if there are any
        issues with the logic of the columns.   
    
    """
    bool_dict = {}
    msg = ""

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

    issues_df = df[df.isin([True]).any(axis=1)] # Filter dataframe to only include rows with issues with logic

    if issues_df.isna().any().any(): # If any rows are null
        print(f"Please note, the following references contain NULL values: {issues_df['reference']}. \n")

    if len(issues_df) > 0:
        msg += "There are issues with the logic of some columns.\n"
    else:
        msg += "There are no issues with the logic of any columns.\n"

    if not issues_df.isna().any().any():
        for index, row in issues_df.iterrows():
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

def create_input_df():
    """Create an input dataframe for the test."""
    input_cols = ["reference", "202", "203", "222", "223", "204","205", "206", "207","219","220", "209", 
    "221", "210", "211", '212','214','216','242','250','243','244','245','246','247','248', '249', '218',
    '225','226','227','228','229','237','302', '303','304','305','501','503','505','507','502','504','506',
    '508','405', '407','409', '411', '406', '408', '410', '412']

    data = [
            ['A', 1, 30, 15, 15, 40, 10, 10, 20, 10, 15, 20, 10, 45, 85, 10, 10, 10, 10, 10, 10, 10, 10, 1, 1, 2, 1, 85, 20, 20, 20, 20, 2, 3, 50, 20, 10, 80, 50, 20, 25, 95, 60, 20, 10, 90, 80, 20, 10, 110, 80, 90, 20, 190],  
            ['B', 10, 30, 15, 15, 40, 10, 10, 20, 10, 15, 20, 10, 45, 85, 10, 10, 10, 10, 10, 10, 10, 10, 1, 1, 2, 1, 85, 20, 20, 20, 20, 2, 3, 50, 20, 10, 80, 50, 20, 25, 95, 60, 20, 10, 90, 80, 20, 10, 110, 80, 90, 20, 190],
            ['C', 10, 30, 15, 15, 40, 10, 10, 20, 10, 15, 20, None , 45, 85, 10, 10, 10, 10, 10, 10, 10, 10, 1, 1, 2, 1, 85, 20, 20, 20, 20, 2, 3, 50, 20, 10, 80, 50, 20, 25, 95, 60, 20, 10, 90, 80, 20, 10, 110, 80, 90, 20, 190],
            ]

    input_df = pandasDF(data=data, columns=input_cols)

    return input_df


input_df = create_input_df()
# input_df = input_df.loc[(input_df['reference'] == 'A')]
# input_df = input_df.loc[(input_df['reference'] == 'B')]
input_df = input_df.loc[(input_df['reference'] == 'C')]


input_df = run_breakdown_validation(input_df)