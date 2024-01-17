import pandas as pd
from pandas.api.types import is_numeric_dtype
from typing import Tuple, List
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
    # Group the DataFrame by 'ref' and 'period', calculate the sum of 'site_percent' for each group,
    # and assign the result to a new column 'site_percent_total'. The 'transform' function is used
    # to ensure that the output is the same shape as the input, allowing it to be added as a new column.
    df["site_percent_total"] = df.groupby([ref, period])["site_percent"].transform("sum")
    return df

def filter_zero_percent(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters out rows where the total percent is zero.
    
    Parameters:
    df (pd.DataFrame): The input DataFrame.

    Returns:
    pd.DataFrame: The DataFrame with zero percent rows filtered out.
    """
    rows_that_are_not_zero = df["site_percent_total"] != 0
    return df[rows_that_are_not_zero]


def calculate_weights(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the weight for each site and stores it in a new column 'site_weight'.
    
    Parameters:
    df (pd.DataFrame): The input DataFrame.

    Returns:
    pd.DataFrame: The DataFrame with the weights calculated.
    """
    df["site_weight"] = df["site_percent"] / df["site_percent_total"]
    return df

def clean_up_post_calc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops unnecessary columns after weight calculation.
    
    Parameters:
    df (pd.DataFrame): The input DataFrame.

    Returns:
    pd.DataFrame: The DataFrame with unnecessary columns dropped.
    """
    return df.drop(columns=["site_percent", "site_percent_total"], axis=1)


def calc_weights_for_sites(df: pd.DataFrame,
                           percent: str,
                           ins: str,
                           ref: str,
                           period: str) -> pd.DataFrame:
    """
    Entry point function for the process to calculate weights for geographic sites.
    
    The weights are calculated using the formula:
        weight = site_percent / site_percent_total
    
    The weights are then used to apportion expenditure of each reference across their sites.
    
    Parameters:
        df (pd.DataFrame): The input DataFrame.
    percent (str): The name of the percent column.
        ins (str): The name of the ins column.
        ref (str): The name of the reference column.
        period (str): The name of the period column.

    Returns:
        pd.DataFrame: The DataFrame with weights calculated for each site.
    """
    
    # Clean the data ready for site apportionment
    df = clean_data(df, percent_col, instance_col)

    # Calculate the total percent for each reference and period
    df = calculate_total_percent(df, ref_col, period_col)
    
    # Filter out the rows where total percent is zero
    df = filter_zero_percent(df)

    # Compute weights
    df = calculate_weights(df)

    # Remove unnecessary columns as they are no longer needed
    df = clean_up_post_calc(df)
    
    return df
    

# TODO: Move this to validation module and make it less specific to "NONE    "
def clean_postcodes(df: pd.DataFrame, postcode_col: str) -> pd.DataFrame:
    """
    Cleans "NONE" postcodes by replacing them with an empty string.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        postcode_col (str): The name of the postcode column.

    Returns:
        pd.DataFrame: The DataFrame with cleaned postcodes.
    """
    df.loc[df[postcode_col] == "NONE    ", postcode_col] = ""
    return df
    

def set_short_form_percentages(df: pd.DataFrame, form_col: str, short_code: str, percent_col: str) -> pd.DataFrame:
    """
    Sets the percent column to 100 for short form records.
    If the percent column for short forms is not blank, raises an error.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        form_col (str): The name of the form column.
        short_code (str): The code for short forms.
        percent_col (str): The name of the percent column.

    Returns:
        pd.DataFrame: The DataFrame with updated percentages for short forms.

    Raises:
        ValueError: If the percent column for short forms is not blank.
    """
    short_forms = df[df[form_col] == short_code]
    if not short_forms[percent_col].isna().all():
        raise ValueError("Percent column for short forms should be blank.")
    
    df.loc[df[form_col] == short_code, percent_col] = 100
    return df

def split_many_sites_df(df: pd.DataFrame, form_col: str, long_code: str, postcode_col: str, instance_col: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Splits the DataFrame into two based on certain conditions.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        form_col (str): The name of the form column.
        long_code (str): The code for long forms.
        postcode_col (str): The name of the postcode column.
        instance_col (str): The name of the instance column.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing two DataFrames.
    """
    # Condition of long forms, many sites, instance >=1, non-null postcodes
    cond = (df[form_col] == long_code) & (df[postcode_col + "_count"] > 1) & (df[instance_col] >= 1)

    # Dataframe dfm with many products - for apportionment and Cartesian product
    many_sites_df = df.copy()[cond]
    many_sites_df = many_sites_df.drop(columns=[postcode_col + "_count"], axis=1)

    # Dataframe with everything else - save unchanged
    df_out = df[~cond]
    df_out = df_out.drop(columns=[postcode_col + "_count"], axis=1)

    return many_sites_df, df_out

def spawn_column_lists(ref_col: str, period_col: str, product_col: str, civdef_col: str, pg_num_col: str) -> Tuple[List[str], List[str]]:
    """
    Creates lists of group columns and code columns.
    
    Args:
        ref_col (str): The name of the ref column.
        period_col (str): The name of the period column.
        product_col (str): The name of the product column.
        civdef_col (str): The name of the civdef column.
        pg_num_col (str): The name of the pg_num column.

    Returns:
        Tuple[List[str], List[str]]: A tuple containing the list of group columns and the list of code columns.
    """
    group_cols = [ref_col, period_col]
    code_cols = [product_col, civdef_col, pg_num_col]
    
    return group_cols, code_cols

def create_category_df(df: pd.DataFrame,
                    ref_col:str,
                    period_col: str,
                    civdef_col: str,
                    pg_num_col: str,
                    code_cols: str,
                    value_cols: list) -> pd.DataFrame:
    """
    Creates a DataFrame with codes and numerical values.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        group_col_name (str): The name of the group column.
        code_col_name (str): The name of the code column.
        value_col_name (str): The name of the value column.

    Returns:
        pd.DataFrame: The DataFrame with codes and numerical values.
    """
    
    group_cols, code_cols = spawn_column_lists(ref_col, period_col, product_col, civdef_col, pg_num_col)

    # Make the dataframe with columns of codes and numerical values
    category_df = df.copy()[group_cols + code_cols + value_cols]
    
    # Remove blank products
    category_df = category_df[category_df[product_col].str.len() > 0]
    
    # De-duplicate by summation - possibly, not needed
    category_df = category_df.groupby(group_cols + code_cols).agg(sum).reset_index()

    return category_df

def create_df_sites(df: pd.DataFrame, df_cols: List[str], code_cols: List[str], value_cols: List[str]) -> pd.DataFrame:
    """
    Creates a DataFrame with postcodes, percents, and everything else.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        df_cols (List[str]): The columns of the DataFrame.
        code_cols (List[str]): The code columns.
        value_cols (List[str]): The value columns.

    Returns:
        pd.DataFrame: The DataFrame with postcodes, percents, and everything else.
    """
    site_cols = [x for x in df_cols if x not in (code_cols + value_cols)]
    df_sites = df.copy()[site_cols]
    return df_sites


def count_duplicate_sites(sites_df: pd.DataFrame, group_cols: List[str], postcode_col: str) -> int:
    """
    Counts the number of duplicate sites in the DataFrame.

    Args:
        sites_df (pd.DataFrame): The input DataFrame.
        group_cols (List[str]): The list of group columns.
        postcode_col (str): The name of the postcode column.

    Returns:
        int: The number of duplicate sites.
    """
    sites_df["site_count"] = sites_df.groupby(group_cols + [postcode_col])[postcode_col].transform("count")
    df_duplicate_sites = sites_df[sites_df["site_count"] > 1]
    num_duplicate_sites = df_duplicate_sites.shape[0]
    
    if num_duplicate_sites:
        SitesApportionmentLogger.info(f"There are {num_duplicate_sites} duplicate sites.")
    
    return num_duplicate_sites

def create_cartesian_product(df_sites: pd.DataFrame, df_codes: pd.DataFrame, group_cols: List[str], dfcols: list) -> pd.DataFrame:
    """
    Creates a 'Cartesian product' of product groups and sites.
    
    Args:
        df_sites (pd.DataFrame): The DataFrame with sites.
        df_codes (pd.DataFrame): The DataFrame with codes.
        group_cols (List[str]): The columns to group by.

    Returns:
        pd.DataFrame: The DataFrame with the Cartesian product.
    
    Example:
        Suppose we have the following DataFrames:

        df_sites:
            site  group
            A     1
            B     1
            C     2

        df_codes:
            code  group
            X     1
            Y     1
            Z     2

        And we call `create_cartesian_product(df_sites, df_codes, ['group'])`.

        The resulting DataFrame would be:

            site  group  code
            A     1      X
            A     1      Y
            B     1      X
            B     1      Y
            C     2      Z
    """
    # Create a Cartesian product of product groups and sites
    df_cart = df_sites.merge(df_codes, on=group_cols, how="inner")
    
    # Restore the original column order
    df_cart = df_cart[dfcols]
    
    return df_cart


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
    df = clean_postcodes(df, postcode_col)

    # Set short form percentages to 100
    df = set_short_form_percentages(df, form_col, short_code, percent_col)

        # Calculate the number of unique non-blank postcodes
    df = count_unique_codes_in_col(df, postcode_col)

    # Split the dataframe into two based on certain conditions
    multiple_sites_df, df_out = split_many_sites_df(df, form_col, long_code, postcode_col, instance_col)

    # Create a list of the value columns that we want to apportion
    # These are the same as the columns we impute so we use a function from imputation.
    value_cols = get_imputation_cols(config)

    # df_codes: dataframe with codes and numerical values
    category_df = create_category_df(multiple_sites_df, ref_col, period_col, civdef_col, pg_num_col, code_cols, value_cols)

    # df_sites: dataframe with postcodes, percents, and everyting else
    df_cols = list(df.columns)
    group_cols, code_cols = spawn_column_lists(ref_col, period_col, product_col, civdef_col, pg_num_col)
    
    sites_df = create_df_sites(multiple_sites_df, df_cols, code_cols, value_cols)

    # Remove instances that have no postcodes
    sites_df = sites_df[sites_df[postcode_col].str.len() > 0]

    # Check for postcode duplicates for QA
    count_duplicate_sites(sites_df, group_cols, postcode_col)

    # Calculate weights
    sites_df = calc_weights_for_sites(sites_df)

    #  Merge codes to sites to create a Cartesian product
    df_cart = create_cartesian_product(sites_df, category_df, group_cols, df_cols)

    # Apply weights
    for value_col in value_cols:
        df_cart[value_col] = df_cart[value_col] * df_cart["site_weight"]

    # Restore the original column order
    

    # Append the apportionned data back to the remaining unchanged data
    df_out = df_out.append(df_cart, ignore_index=True)

    # Sort by period, ref, instance in ascending order.
    df_out = df_out.sort_values(by=[period_col, ref_col, instance_col], ascending=True).reset_index(
        drop=True
    )

    return df_out
