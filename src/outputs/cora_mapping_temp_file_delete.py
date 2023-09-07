import pandas as pd

from src.utils.wrappers import exception_wrap


@exception_wrap
def create_cora_status_col(df, mapper_df, main_col = "statusencoded"):
    """_summary_

    Args:
        df (pd.DataFrame): main data containing responses
        mapper_df (pd.DataFrame): mapper with cora status equivalents
        main_col (str, optional): Defaults to "statusencoded".

    Returns:
        df: main data with cora status column added
    """
    
    # convert mapper df to dictionary
    mapper_dict = dict(zip(mapper_df[main_col], mapper_df["form_status"]))
    
    # Create a new column by mapping values from main_col using the
    # mapper dictionary
    df['form_status'] = df[main_col].map(mapper_dict)
    return df
