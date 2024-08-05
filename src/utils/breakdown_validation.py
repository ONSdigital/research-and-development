"""Function to validate the breakdown totals from the imputation."""
import os
import logging
import pandas as pd

BreakdownValidationLogger = logging.getLogger(__name__)

def breakdown_validation(df: pd.DataFrame) -> dict :
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

    for index, row in false_df.iterrows():
        if row['check1']:
            bool_dict[index] = False
            msg += f"222 + 223 does not equal 203 for reference: {row['reference']}. "
        if row['check2']:
            bool_dict[index] = False
            msg += f"202 + 223 does not equal 204 for reference: {row['reference']}. "
        if row['check3']:
            bool_dict[index] = False
            msg += f"205 + 206 + 207 does not equal 204 for reference: {row['reference']}. "
        if row['check4']:
            bool_dict[index] = False
            msg += f"221 is greater than 209 for reference: {row['reference']}. "
        if row['check5']:
            bool_dict[index] = False
            msg += f"219 + 220 + 209 does not equal 210 for reference: {row['reference']}. "
        if row['check6']:
            bool_dict[index] = False
            msg += f"204 + 210 + 209 does not equal 211 for reference: {row['reference']}. "
        if row['check7']:
            bool_dict[index] = False
            msg += f"212 + 246 does not equal 218 and 212 + 246 does not equal 211 for reference: {row['reference']}. "
        if row['check8']:
            bool_dict[index] = False
            msg += f"225 + 226 + 227 + 228 + 229 + 237 does not equal 218 for reference: {row['reference']}. "
        if row['check9']:
            bool_dict[index] = False
            msg += f"302 + 303 + 304 does not equal 305 for reference: {row['reference']}. "
        if row['check10']:
            bool_dict[index] = False
            msg += f"501 + 503 + 505 does not equal 507 for reference: {row['reference']}. "
        if row['check11']:
            bool_dict[index] = False
            msg += f"502 + 504 + 506 does not equal 508 for reference: {row['reference']}. "
        if row['check12']:
            bool_dict[index] = False
            msg += f"405 + 407 + 409 does not equal 411 for reference: {row['reference']}. "
        if row['check13']:
            bool_dict[index] = False
            msg += f"406 + 408 + 410 does not equal 412 for reference: {row['reference']}. " 
        else:
            msg += "There are no issues with the breakdown values."   

    return bool_dict, msg


def filename_validation(config: dict) -> dict:
    """Checks that the mapping filenames are valid"""
    bool_dict, msg = breakdown_validation(config) # needs adjusting

    if all(bool_dict.values()):
        BreakdownValidationLogger.info("All breakdown values are valid.")
    else:
        BreakdownValidationLogger.error("There are errors with the breakdown values, please adjust the references that have issues.")
        raise ValueError(msg)

    return config