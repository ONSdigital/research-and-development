"""Functions for the Mean of Ratios (MoR) methods."""
import pandas as pd
import re

from src.imputation.tmi_imputation import (
    apply_to_original, 
    create_imp_class_col,
    sort_df,
    apply_trim_check,
    trim_bounds
)
from src.imputation.apportionment import run_apportionment

good_statuses = ['Clear', 'Clear - overridden']
bad_statuses = ['Form sent out', 'Check needed']

def run_mor(
    df,
    backdata,
    target_vars
):
    # Carry forward previous values
    to_carry_df, backdata = mor_preprocessing(df, backdata)
    carried_forwards_df = carry_forwards(to_carry_df, backdata, target_vars)
    carried_forwards_df['imp_marker'] = 'CF'
    #TODO This all needs sorting to link the first part to the second

    #TEMPORARY FIX to get cellnumber into `backdata`
    backdata = pd.merge(backdata, df[['reference', 'cellnumber']], on='reference', how='left')
    
    # Apply MoR
    backdata = create_imp_class_col(backdata, '200', '201')
    df = create_imp_class_col(df, '200', '201')
    df = calculate_links(df, backdata, target_vars)
    df = apply_links(df)
    
    return apply_to_original(carried_forwards_df, df)

def mor_preprocessing(df, backdata):
    """Apply pre-processing ready for MoR

    Args:
        df (pd.DataFrame): full responses for the current year
        backdata (pd.Dataframe): backdata file read in during staging.
    """
    # Select only values to be imputed and remove duplicate instances
    to_impute_df = df.copy().loc[(
        (df['formtype'] == '0001') & 
        (df['status'].isin(bad_statuses)) &
        (df['instance'] == 0 | pd.isnull(df['instance'])))
        , :]
    
    # Convert backdata column names from qXXX to XXX
    p = re.compile(r"q\d{3}")
    cols = [col for col in list(backdata.columns) if p.match(col)]
    to_rename = {col: col[1:] for col in cols}
    backdata = backdata.rename(columns=to_rename)

    backdata = run_apportionment(backdata)
    # Only pick up useful backdata
    backdata = backdata.loc[(backdata['status'].isin(good_statuses)), :]
    
    return to_impute_df, backdata

def carry_forwards(df, backdata, target_vars):
    """Carries forward values from the `target_vars` of `backdata`
    into `df` where reference is matched.

    Args:
        df (pd.DataFrame): Full responses DataFrame.
        backdata (pd.DataFrame): One year of backdata.
        target_vars ([string]): List of target variables to carry forwards.

    Returns:
        pd.DataFrame: `df` with `target_vars` carried forwards.
    """
    df = pd.merge(
        df,
        backdata,
        how='left',
        on='reference',
        suffixes=('', '_prev')
    )
    for var in target_vars:
        df[var] = df.loc[:, f'{var}_prev']
    
    return df

def calculate_links(df, backdata, target_vars):
    """_summary_

    Args:
        df (_type_): _description_
        backdata (_type_): _description_
    """
    df = df.copy().loc[df['status'].isin(good_statuses)]
    
    df = pd.merge(df,
                  backdata, 
                  on=['reference', 'imp_class'], 
                  how='inner',
                  suffixes=('', '_prev'))
    df['ratios'] = 1
    
    grouped_df = df.groupby('imp_class')

    for name, group in grouped_df:
        for var in target_vars:
            if len(group) >= 5:
                group[f'{var}_ratios'] = group[var]/group[f'{var}_prev']
                sorted_group = sort_df('ratios', group)
                trim_check = apply_trim_check(sorted_group, 'ratios')
                group = trim_bounds(trim_check, 'ratios')
                group[f'{var}_mor_link'] = group['ratios'].mean()
                # Do they have to respond to all questions?
                group['imputation_marker'] = 'MoR'
            else:
                group['ratios'] = 1
                group['imputation_marker'] = 'CF'
    
    qa_df = grouped_df.agg({'ratios': 'count', 'mor_link':'first'})
    
    return grouped_df, qa_df


def apply_links(df, target_vars):
    for var in target_vars:
        df[f'{var}_imputed'] = df[var]*df[f'{var}_mor_link']
    return df
    