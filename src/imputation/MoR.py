"""Functions for the Mean of Ratios (MoR) methods."""
import pandas as pd
import re

from src.imputation.tmi_imputation import apply_to_original
from src.imputation.apportionment import run_apportionment

good_statuses = ['Clear', 'Clear - overridden']
bad_statuses = ['Form sent out', 'Check needed']

def run_mor(
    df,
    backdata,
    target_vars
):
    to_impute_df, backdata = mor_preprocessing(df, backdata)
    carried_forwards_df = carry_forwards(to_impute_df, backdata, target_vars)
    carried_forwards_df['imp_marker'] = 'carried_forwards'
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