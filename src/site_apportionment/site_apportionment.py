import pandas as pd
from pandas.api.types import is_numeric_dtype
from typing import Tuple, List, Dict, Union
import logging

from src.imputation.imputation_helpers import get_imputation_cols

SitesApportionmentLogger = logging.getLogger(__name__)

# Column names redefined for convenience
ref_col: str = "reference"
instance_col: str = "instance"
period_col: str = "period"
form_col: str = "formtype"
postcode_col: str = "601"  
percent_col: str = "602"
product_col: str = "201"
pg_num_col: str = "pg_numeric"
civdef_col: str = "200"

groupby_cols: List[str] = [ref_col, period_col]
code_cols: List[str] = [product_col, civdef_col, pg_num_col]

# Long and short form codes
short_code: str = "0006"
long_code: str = "0001"


def set_short_form_percentages(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sets the percent column to 100 for short form records.
    If the percent column for short forms is not blank, raises an error.

    Args:
        df (pd.DataFrame): The input DataFrame.

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


def count_unique_postcodes_in_col(df: pd.DataFrame) -> pd.DataFrame:
    """Adds a column containing  the number of unique non-empty postcodes.

    Args:
        df (pd.DataFrame): A dataframe containing all data

    Returns:
        (pd.DataFrame): A copy of original dataframe with an additional column
        called the same as code with suffix "_count" countaining the number of
        unique non-empty postcodes
    """
    dfa = df.copy()

    dfa = dfa[groupby_cols + [postcode_col]]
    dfa = dfa[dfa[postcode_col].str.len() > 0]
    dfa = dfa.drop_duplicates()
    dfb = dfa.groupby(groupby_cols).agg("count").reset_index()
    dfb = dfb.rename({postcode_col: postcode_col + "_count"}, axis="columns")
    df = df.merge(dfb, on=groupby_cols, how="left")
    return df


def split_many_sites_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Splits the DataFrame into two based on certain conditions.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing two DataFrames.
    """
    # Condition of long forms, many sites, instance >=1
    cond = (
        (df[form_col] == long_code)
        & (df[postcode_col + "_count"] > 1)
        & (df[instance_col] >= 1)
    )

    # Dataframe many_sites_df with many products - for apportionment 
    many_sites_df = df.copy()[cond]
    many_sites_df = many_sites_df.drop(columns=[postcode_col + "_count"], axis=1)

    # Dataframe with everything else - save unchanged
    df_out = df.copy()[~cond]
    df_out = df_out.drop(columns=[postcode_col + "_count"], axis=1)

    return many_sites_df, df_out


def create_category_df(df: pd.DataFrame, value_cols: List[str]) -> pd.DataFrame:
    """
    Creates a DataFrame with codes and numerical values.

    Args:
        df (pd.DataFrame): The input DataFrame.
        value_cols (List[str]): List of columns containing numeric values.

    Returns:
        pd.DataFrame: The DataFrame with codes and numerical values.
    """
    # Make the dataframe with columns of codes and numerical values
    category_df = df.copy()[groupby_cols + code_cols + value_cols]

    #TODO: update to the following: 
    # # ensure all three elements of the codes are notnull
    # valid_code_cond = (
    #     ~df[product_col].isnull() & ~df[civdef_col].isnull() & ~df[pg_num_col].isnull()
    # )
    # category_df = category_df.loc[valid_code_cond]

    # Remove blank products
    category_df = category_df[category_df[product_col].str.len() > 0]

    # De-duplicate by summation - possibly, not needed
    category_df = category_df.groupby(groupby_cols + code_cols).agg(sum).reset_index()

    return category_df


def create_sites_df(
    df: pd.DataFrame, orig_cols: List[str], value_cols: List[str]
) -> pd.DataFrame:
    """
    Creates a DataFrame with postcodes, percents, and everything else.

    Args:
        df (pd.DataFrame): The input DataFrame.
        orig_cols (List[str]): The columns of the DataFrame.
        value_cols (List[str]): The value columns.

    Returns:
        pd.DataFrame: The DataFrame with postcodes, percents, and everything else.
    """
    site_cols = [x for x in orig_cols if x not in (code_cols + value_cols)]
    sites_df = df.copy()[site_cols]

    # Remove instances that have no postcodes
    sites_df = sites_df[sites_df[postcode_col].str.len() > 0]
    return sites_df


def count_duplicate_sites(sites_df: pd.DataFrame):
    """
    Counts the number of duplicate sites in the DataFrame.

    Args:
        sites_df (pd.DataFrame): The input DataFrame.
        group_cols (List[str]): The list of group columns.
        postcode_col (str): The name of the postcode column.

    Returns:
        int: The number of duplicate sites.
    """
    site_count_df = sites_df[groupby_cols + [postcode_col]].copy()
    site_count_df["site_count"] = site_count_df.groupby(groupby_cols + [postcode_col])[
        postcode_col
    ].transform("count")
    df_duplicate_sites = site_count_df[site_count_df["site_count"] > 1]
    num_duplicate_sites = df_duplicate_sites.shape[0]

    if num_duplicate_sites:
        SitesApportionmentLogger.info(
            f"There are {num_duplicate_sites} duplicate sites."
        )


def calc_weights_for_sites(df: pd.DataFrame, groupby_cols: List[str]) -> pd.DataFrame:
    """
    Calculate weights for geographic sites.

    The weights are calculated using the formula:
        weight = site_percent / site_percent_total

    The weights are then used to apportion expenditure of each reference across their sites.

    Parameters:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with weights calculated for each site.
    """
    # Create a new column with cleaned site percentage values
    df["site_percent"] = df[percent_col].fillna(0)
    df.loc[(df.instance == 0), "site_percent"] = 0

    # Calculate the total percent for each reference and period
    df["site_percent_total"] = df.groupby(groupby_cols)["site_percent"].transform(
        "sum"
    )

    # Filter out rows where the total percent is zero.
    df = df.copy().loc[df["site_percent_total"] != 0]

    # Compute weights
    df["site_weight"] = df["site_percent"] / df["site_percent_total"]

    # Remove unnecessary columns as they are no longer needed
    df = df.drop(columns=["site_percent", "site_percent_total"], axis=1)

    return df


def create_cartesian_product(
    sites_df: pd.DataFrame, category_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Creates a 'Cartesian product' of product classifications and sites.

    'product classifications' are defined as the unique combination of civ/def,
        product group (alpha) and product group (numeric)

    Args:
        sites_df (pd.DataFrame): The DataFrame with sites.
        category_df (pd.DataFrame): The DataFrame with with the unique combination
            of civ/def, product group (alpha) and product group (numeric)
        group_cols (List[str]): The columns to group by.

    Returns:
        pd.DataFrame: The DataFrame with the Cartesian product.

    Example:
        Suppose we have the following DataFrames:

        sites_df:
            ref     site
            1       A 
            1       B 
            2       C 

        category_df:
            ref     prod_class 
            1       X  
            1       Y  
            2       Z         

        And we call `create_cartesian_product(sites_df, category_df)`.

        The resulting DataFrame would be:

            ref     site    prod_class
            1       A       X
            1       A       Y
            1       B       X
            1       B       Y
            2       C       Z
    """
    # Create a Cartesian product of product groups and sites
    df_cart = sites_df.merge(category_df, on=["reference", "period"], how="inner")

    return df_cart


def weight_values(
    df: pd.DataFrame, value_cols: List[str], weight_col: str
) -> pd.DataFrame:
    """
    Multiplies the specified columns by the weight column.

    Args:
        df (pd.DataFrame): The input DataFrame.
        value_cols (List[str]): The columns to be weighted.
        weight_col (str): The column to use as weights.

    Returns:
        pd.DataFrame: The DataFrame with the weighted columns.
    """

    # George's original code:
    # for value_col in value_cols:
    #     df_cart[value_col] = df_cart[value_col] * df_cart["site_weight"]

    df[value_cols] = df[value_cols].multiply(df[weight_col], axis=0)
    return df


def sort_rows_order_cols(df: pd.DataFrame,  cols_in_order: List[str]) -> pd.DataFrame:
    """
    Sorts the DataFrame by the specified columns in ascending order.

    Args:
        df (pd.DataFrame): The DataFrame to sort.
        cols (List[str]): The columns to sort by.

    Returns:
        pd.DataFrame: The sorted DataFrame.
    """

    # Restore the original column order.
    cols_to_be_removed = set(df.columns) - set(cols_in_order)

    # Log what columns are being lost
    if len(cols_to_be_removed) > 0:
        SitesApportionmentLogger.debug(f"Removing {cols_to_be_removed} from df_cart")
    df = df[cols_in_order]

    # Sort the rows on values in chosen columns
    cols_to_sort_by: List[str] = [period_col, ref_col, instance_col]
    sorted_df = df.sort_values(by=cols_to_sort_by, ascending=True).reset_index(
        drop=True
    )

    return sorted_df


def run_apportion_sites(
    df: pd.DataFrame, config: Dict[str, Union[str, List[str]]]
) -> pd.DataFrame:
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
        config (Dict[str, Union[str, List[str]]]): Configuration dictionary.

    Returns:
        pd.DataFrame: A dataframe with the same columns, with applied site
        apportionment.
    """
    # Get the original columns set
    orig_cols: List[str] = list(df.columns)
    # Create a list of the value columns that we want to apportion
    # These are the same as the columns we impute so we use a function from imputation.
    value_cols: List[str] = get_imputation_cols(config)

    # Set short form percentages to 100
    df = set_short_form_percentages(df)

    # Calculate the number of unique non-blank postcodes
    df = count_unique_postcodes_in_col(df)

    # Split the dataframe into two based on whether there's more than one site (postcode)
    multiple_sites_df, df_out = split_many_sites_df(df)

    # category_df: dataframe with codes and numerical values
    category_df = create_category_df(multiple_sites_df, value_cols)

    sites_df = create_sites_df(multiple_sites_df, orig_cols, value_cols)

    # Check for postcode duplicates for QA
    count_duplicate_sites(sites_df)

    # Calculate weights
    sites_df = calc_weights_for_sites(sites_df, groupby_cols)

    #  Merge codes to sites to create a Cartesian product
    df_cart = create_cartesian_product(sites_df, category_df)

    # Apply weights
    df_cart = weight_values(df_cart, value_cols, "site_weight")

    # Append the apportionned data back to the remaining unchanged data
    df_out = df_out.append(df_cart, ignore_index=True)

    # Sort by period, ref, instance in ascending order.

    df_out = sort_rows_order_cols(df_out, orig_cols)

    return df_out
