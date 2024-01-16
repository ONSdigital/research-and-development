import pandas as pd
from pandas.api.types import is_numeric_dtype
import os
import logging

from src.imputation.imputation_helpers import get_imputation_cols

SitesApportionmentLogger = logging.getLogger(__name__)

# Colunm names redefined for convenience
ref_col = "reference"
instance_col = "instance"
period_col = "period"
form_col = "formtype"
postcode_col = "601" # "postcodes_harmonised"
percent_col = "602"
product_col = "201"
pg_num_col = "pg_numeric"
civdef_col = "200"

# Long and short form codes
short_code = "0006"
long_code = "0001"


def count_unique_codes_in_col(df: pd.DataFrame, code: str) -> pd.DataFrame:
    """Calculates the number of unique non-empty codes in a column.

    Args:
        df (pd.DataFrame): A dataframe containing all data
        code (str): Name of the column containing codes

    Returns:
        (pd.DataFrame): A copy of original dataframe with an additional column
        called the same as code with suffix "_count" countaining the number of
        unique non-empty codes
    """

    dfa = df.copy()

    # Select columns that we need
    cols_need = [ref_col, period_col, code]
    dfa = dfa[cols_need]
    dfa = dfa[dfa[code].str.len() > 0]
    dfa = dfa.drop_duplicates()
    dfb = dfa.groupby([ref_col, period_col]).agg("count").reset_index()
    dfb.rename({code: code + "_count"}, axis="columns", inplace=True)
    df = df.merge(dfb, on=[ref_col, period_col], how="left")
    return df

def clean_data(df: pd.DataFrame, percent: str, ins: str) -> pd.DataFrame:
    """
    Cleans the data by filling null values in the percent column with zero and creating a new column 'site_percent'.
    
    Parameters:
    df (pd.DataFrame): The input DataFrame.
    percent (str): The name of the percent column.
    ins (str): The name of the ins column.

    Returns:
    pd.DataFrame: The cleaned DataFrame.
    """
    df["site_percent"] = df[percent]
    df["site_percent"].fillna(0, inplace=True)
    df["site_percent"] = df["site_percent"] * df[ins].astype("bool")
    return df


def calculate_total_percent(df: pd.DataFrame, ref: str, period: str) -> pd.DataFrame:
    """
    Calculates the total percent for each reference and period and stores it in 
        a new column 'site_percent_total'.
    
    Parameters:
        df (pd.DataFrame): The input DataFrame.
        ref (str): The name of the reference column.
        period (str): The name of the period column.

    Returns:
        pd.DataFrame: The DataFrame with the total percent calculated.
    """
    df["site_percent_total"] = df.groupby([ref, period])["site_percent"].transform("sum")
    return df


def weights(df):
    """Calculates site weights based on the percents. Copies the precent value
    from its original location (question 602) to a new column "site_percent".
    If the percent value is Null, fills it with zero.

    Adds all percents per RU reference and period and calculates
    site_percent_total. If the total for a reference is zero, apportionment
    cannot be done, and this reference is removed.
    Re-noramalises the percent values by total, to compute site_weight. This
    deals with the case when the users entered percents incorrectly, so they
    don't sum up to 100.

    Args:
        df (pd.DataFrame): A dataframe containing all data

    Returns:
        (pd.DataFrame): A copy of original dataframe with an additional column
        called site_weight countaining the weights of each site, between 0 and 1
    """
    
    df = clean_data(df, percent_col, instance_col)

    # Calculate the total percent for each reference and period
    df = calculate_total_percent(df, ref_col, period_col)
    

    # Filter out the rows where total percent is zero
    dfc = dfc.copy()[dfc["site_percent_total"] != 0]

    # Compute weights
    dfc["site_weight"] = dfc["site_percent"] / dfc["site_percent_total"]

    # Remove unnecessary columns as they are no longer needed
    dfc = dfc.drop(columns=["site_percent", "site_percent_total"], axis=1)

    return dfc


def apportion_sites(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Apportion the numerical values for each product group across multiple sites.

    This is done using percents from question 602 to compute weights.
    The code selects the records long from references that have multiple non-empty
    postcodes, then splits the dataframe in two; one for codes and one for sites.

    Codes have reference, period, product group (unique combinations of product group,
    civil or defence and pg_numeric) and all relevant numeric columns.

    Sites have reference, period and all other fields except for product group keys and
    the relevant numerical values. Sites dataframe contains multiple instances, with
    instance 1 and higher having different postcodes and with percents for each site.
    For sites, weights are calculated using the percents.

    A Cartesian product of product groups and sites is created, and the weights of each
    site are applied to values of each product. Also, for short forms, percent is set
    to 100.

    Args:
        df (pd.DataFrame): Dataframe containing all input data.

    Returns:
        (pd.DataFrame): A dataframe with the same columns, with applied site
        apportionment.
    """
    # Clean "NONE" postcodes
    df.loc[df[postcode_col] == "NONE    ", postcode_col] = ""

    # Set short form percentages to 100
    df.loc[df[form_col] == short_code, percent_col] = 100

    # df_cols: original columns
    df_cols = list(df.columns)

    # Create a list of the value columns that we want to apportion
    # These are the same as the columns we impute so we use a function from imputation.
    value_cols = get_imputation_cols(config)

    # Calculate the number of unique non-blank postcodes
    df = count_unique_codes_in_col(df, postcode_col)

    # Condition of long forms, many sites, instance >=1, non-null postcodes
    cond = (df[form_col] == long_code) & (df[postcode_col + "_count"] > 1) & (df[instance_col] >= 1)

    # Dataframe dfm with many products - for apportionment and Cartesian product
    dfm = df.copy()[cond]
    dfm = dfm.drop(columns=[postcode_col + "_count"], axis=1)

    # Dataframe with everything else - save unchanged
    df_out = df[~cond]
    df_out = df_out.drop(columns=[postcode_col + "_count"], axis=1)

    # df_codes: dataframe with codes and numerical values
    group_cols = [ref_col, period_col]
    code_cols = [product_col, civdef_col, pg_num_col]
    df_codes = dfm.copy()[group_cols + code_cols + value_cols]

    # Remove blank products
    df_codes = df_codes[df_codes[product_col].str.len() > 0]

    # # De-duplicate by summation - possibly, not needed
    df_codes = df_codes.groupby(group_cols + code_cols).agg(sum).reset_index()

    # df_sites: dataframe with postcodes, percents, and everyting else
    site_cols = [x for x in df_cols if x not in (code_cols + value_cols)]
    df_sites = dfm.copy()[site_cols]

    # Remove instances that have no postcodes
    df_sites = df_sites[df_sites[postcode_col].str.len() > 0]

    # Check for postcode duplicates for QA
    df_sites["site_count"] = df_sites.groupby(group_cols + [postcode_col])[
        postcode_col
    ].transform("count")
    df_duplicate_sites = df_sites[df_sites["site_count"] > 1]
    num_duplicate_sites = df_duplicate_sites.shape[0]
    if num_duplicate_sites:
        SitesApportionmentLogger.info(
            f"There are {num_duplicate_sites} duplicate sites."
        )

    # Calculate weights
    df_sites = weights(df_sites)

    #  Merge codes to sites to create a Cartesian product
    df_cart = df_sites.merge(df_codes, on=group_cols, how="inner")

    # Apply weights
    for value_col in value_cols:
        df_cart[value_col] = df_cart[value_col] * df_cart["site_weight"]

    # Restore the original column order
    df_cart = df_cart[df_cols]

    # Append the apportionned data back to the remaining unchanged data
    df_out = df_out.append(df_cart, ignore_index=True)

    # Sort by period, ref, instance in ascending order.
    df_out = df_out.sort_values(by=[period_col, ref_col, instance_col], ascending=True).reset_index(
        drop=True
    )

    return df_out
