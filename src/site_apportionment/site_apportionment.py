# Standard Library Imports
from typing import Tuple, List, Dict, Union
import logging

# Third Part Imports
import pandas as pd

# Local Imports
from src.imputation.imputation_helpers import get_imputation_cols
from src.site_apportionment.output_status_filtered import keep_good_markers

SitesApportionmentLogger = logging.getLogger(__name__)

# Column names redefined for convenience
ref_col: str = "reference"
instance_col: str = "instance"
period_col: str = "period"
form_col: str = "formtype"
postcode_col: str = "601"
postcodes_harmonised_col: str = "postcodes_harmonised"
percent_col: str = "602"
product_col: str = "201"
pg_num_col: str = "pg_numeric"
civdef_col: str = "200"
marker_col: str = "imp_marker"
status_col: str = "status"

groupby_cols: List[str] = [ref_col, period_col]
code_cols: List[str] = [product_col, civdef_col, pg_num_col]
site_cols: List[str] = [
    instance_col,
    postcode_col,
    percent_col,
    postcodes_harmonised_col,
]

# Long and short form codes
short_code: str = "0006"
long_code: str = "0001"


def set_percentages(df: pd.DataFrame) -> pd.DataFrame:
    """
    Set percentage column to 100 for short forms and single site long forms.

    The condtitions for the long forms needing 100 in the percentage column are:
    - long forms, exactly 1 site, instance >=1 and notnull postcode
    - long forms with status "Form sent out"

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with updated percentages.

    Raises:
        ValueError: If the percent column for short forms is not blank.
    """
    # If the percent column for short forms is not blank, raise an error.

    # TODO: This is not the case where a short form has been imputed from a LF in MoR
    # short_forms = df[df[form_col] == short_code]
    # if not short_forms[percent_col].isna().all():
    #     raise ValueError("Percent column for short forms should be blank.")
    # df.loc[df[form_col] == short_code, percent_col] = 100

    # Condition for long forms with status "Form sent out"
    # Note: those imputed by MoR will have had the postcode column imputed, so we check
    # for null in the postcode count column
    sent_out_condition = (
        (df[form_col] == long_code)
        & (df[status_col] == "Form sent out")
        & (df[postcode_col + "_count"].isna())
        & (df[postcode_col].isna())
    )
    # Update records matching the sent_out_condition with a postcode and count of 1
    df.loc[sent_out_condition, postcode_col] = df.loc[
        sent_out_condition, "postcodes_harmonised"
    ]
    df.loc[sent_out_condition, postcode_col + "_count"] = 1

    # Condition for long forms, exactly 1 site, instance >=1 and notnull postcode
    single_cond = (
        (df[form_col] == long_code)
        & (df[postcode_col + "_count"] == 1)
        & (df[instance_col] >= 1)
        & create_notnull_mask(df, postcode_col)
    )
    df.loc[single_cond, percent_col] = 100

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


def split_sites_df(
    df: pd.DataFrame, imp_markers_to_keep: List[str]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split dataframe into two based on whether there are sites or not.

    This will be the basis on whether or not records are included in site apportionment.

    All long form records that include postcodes in the postcode_col are used for site
    apportionment, and all orther records, including short forms, are included in a
    second dataframe.

    Args:
        df (pd.DataFrame): The input DataFrame.
        imp_markers_to_keep (List[str]): A list of markers to keep.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: One dataframe with records to be apportioned,
            and a second containing all other records.
    """

    # Condition for records to apportion: long forms, at least one site, instance >=1
    to_apportion_cond = (
        (df[form_col] == long_code)
        & (df[postcode_col + "_count"] >= 1)
        & (df[instance_col] >= 1)
    )

    # Dataframe to_apportion_df with many products - for apportionment
    to_apportion_df = df.copy()[to_apportion_cond]

    # Dataframe with everything else - save unchanged
    df_out = df.copy()[~to_apportion_cond]

    # Remove "bad" imputation markers from df_out
    # NOTE: Probably this isn't needed: can't think of a case where it would be.
    df_out = keep_good_markers(df_out, imp_markers_to_keep)

    return to_apportion_df, df_out


def create_notnull_mask(df: pd.DataFrame, col: str) -> pd.Series:
    """Return a mask for string values in column col that are not null."""
    return df[col].str.len() > 0


def deduplicate_codes_values(
    df, group_cols, value_cols, textual_cols, methods=["sum", "first"]
) -> pd.DataFrame:
    """Deduplicates a dataframe, so  it has only one unique combination of group cols.

    Numerical valies in value_cols are summed. From the
    textual_cols, we choose the first entry for deterministic behaviour.

    Args:
        df (pd.DataFrame): The input DataFrame.
        group_cols (List[str]): List of columns to group by.
        value_cols (List[str]): List of columns containing numeric values.
        textual_cols (List[str]): List of columns containing textual values.
        methods (List[str]): List of aggregation methods for values (0,
            default method is sum) and categorties (1, default method is first).

    Returns:
        pd.DataFrame: De-duplicated dataframe
    """
    agg_dict = {}

    for col in value_cols:
        agg_dict.update({col: methods[0]})

    for col in textual_cols:
        agg_dict.update({col: methods[1]})

    return df.groupby(group_cols).agg(agg_dict).reset_index()


def create_category_df(
    df: pd.DataFrame,
    imp_markers_to_keep: List[str],
    orig_cols: List[str],
    groupby_cols: List[str],
    code_cols: List[str],
    site_cols: List[str],
    value_cols: List[str],
) -> pd.DataFrame:
    """
    Creates a DataFrame with product group codes, numerical values and all other
    textual values, including imp_marker, exctept for instance, postcode and
    percentage.
    Removes rows that have Null product codes in 201 or in civil/defence
    columns.
    Removes "bad" imputation markers.
    De-duplicates, so there is just one unique combination of
    group codes for each reference and period.

    Args:
        df (pd.DataFrame): The input DataFrame.
        imp_markers_to_keep (List[str]): List of imputation markers to keep.
        orig_cols (List[str]): List of original columns.
        groupby_cols (List[str]): List of columns to group by.
        code_cols (List[str]): List of code columns.
        site_cols (List[str]): List of site column.
        value_cols (List[str]): List of columns containing numeric values.

    Returns:
        pd.DataFrame: The DataFrame with codes and numerical values.
    """
    # Make the dataframe with columns of all columns except for site columns
    category_cols = [x for x in orig_cols if x not in site_cols]
    category_df = df.copy()[category_cols]

    # ensure the product group and C or D codes are notnull
    valid_code_cond = create_notnull_mask(df, product_col) & create_notnull_mask(
        df, civdef_col
    )
    category_df = category_df.loc[valid_code_cond]

    # Remove "bad" imputation markers, "no_imputation" and "No mean found."
    category_df = keep_good_markers(category_df, imp_markers_to_keep)

    # De-duplicate - possibly, not needed
    # In addition to the textual columns that belong to the responses, the list  below
    # also includes the numeric 9000 cols not needed in the pipeline, and the columns
    # from the contributor data, originating in IDBR. These can also be aggregated using
    # "first" as they are the same for all instances.
    textual_cols = [
        x for x in category_cols if x not in (groupby_cols + code_cols + value_cols)
    ]
    category_df = deduplicate_codes_values(
        category_df,
        group_cols=groupby_cols + code_cols,
        value_cols=value_cols,
        textual_cols=textual_cols,
        methods=["sum", "first"],
    )

    return category_df


def count_duplicate_sites(sites_df: pd.DataFrame) -> int:
    """
    Counts the number of duplicate sites in the DataFrame.

    Args:
        sites_df (pd.DataFrame): The input DataFrame.

    Returns:
        int: The number of duplicate sites.
    """
    site_count_df = sites_df[groupby_cols + [postcode_col]].copy()
    site_count_df["site_count"] = site_count_df.groupby(groupby_cols + [postcode_col])[
        postcode_col
    ].transform("count")
    df_duplicate_sites = site_count_df[site_count_df["site_count"] > 1]
    num_duplicate_sites = len(
        df_duplicate_sites.groupby(["reference", "period", postcode_col])
    )

    if num_duplicate_sites:
        # display number of duplicate sites
        SitesApportionmentLogger.info(
            f"There are {num_duplicate_sites} duplicate sites."
        )
        # shows duplicate site df
        SitesApportionmentLogger.info(
            f"Duplicates sites DF: \n{df_duplicate_sites.to_string()}"
        )


def create_sites_df(
    df: pd.DataFrame, groupby_cols: List[str], site_cols: List[str]
) -> pd.DataFrame:
    """
    Creates a DataFrame with reference, period, instance, postcode and percent.

    Args:
        df (pd.DataFrame): The input DataFrame.
        groupby_cols (List[str]): Columns to group by: reference, period.
        site_cols (List[str]): Columns of sites (instance, postcode, percent).

    Returns:
        pd.DataFrame: The DataFrame with sites.
    """
    sites_df = df.copy()[groupby_cols + site_cols]

    # Remove instances that have no postcodes
    sites_df = sites_df[sites_df[postcode_col].str.len() > 0]

    # Check for postcode duplicates for QA
    count_duplicate_sites(sites_df)

    # De-duplicate by summing percents
    sites_df[percent_col] = sites_df[percent_col].fillna(0)
    agg_dict = {instance_col: "first", percent_col: "sum"}
    sites_df = (
        sites_df.groupby(groupby_cols + [postcode_col, postcodes_harmonised_col])
        .agg(agg_dict)
        .reset_index()
    )

    return sites_df


def calc_weights_for_sites(df: pd.DataFrame, groupby_cols: List[str]) -> pd.DataFrame:
    """
    Calculate weights for geographic sites.

    The weights are calculated using the formula:
        weight = site_percent / site_percent_total

    The weights are then used to apportion expenditure of each reference across
     their sites.

    Args:
        df (pd.DataFrame): The input DataFrame.
        groupby_cols (List[str]): List of columns to group by.

    Returns:
        pd.DataFrame: The DataFrame with weights calculated for each site.
    """
    # Create a new column with cleaned site percentage values
    df["site_percent"] = df[percent_col].fillna(0)

    # Calculate the total percent for each reference and period
    df["site_percent_total"] = df.groupby(groupby_cols)["site_percent"].transform("sum")

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

    Returns:
        pd.DataFrame: The DataFrame with the Cartesian product.

    Example:
        Suppose we have the following DataFrames:

        sites_df:
            ref     instance    site
            1       1           A
            1       2           B

        category_df:
            ref     prod_class
            1       X
            1       Y

        And we call `create_cartesian_product(sites_df, category_df)`.

        The resulting DataFrame would be:

            ref    instance site    prod_class
            1      1        A       X
            1      1        A       Y
            1      2        B       X
            1      2        B       Y
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

    df[value_cols] = df[value_cols].multiply(df[weight_col], axis=0)
    return df


def sort_rows_order_cols(df: pd.DataFrame, cols_in_order: List[str]) -> pd.DataFrame:
    """
    Sorts the DataFrame by the specified columns in ascending order.

    Args:
        df (pd.DataFrame): The DataFrame to sort.
        cols_in_order (List[str]): The columns to sort by.

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
    df: pd.DataFrame,
    imp_markers_to_keep: List[str],
    config: Dict[str, Union[str, List[str]]],
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
        imp_markers_to_keep (List[str]): A list of imputation markers to keep.
        config (Dict[str, Union[str, List[str]]]): Configuration dictionary.

    Returns:
        pd.DataFrame: A dataframe with the same columns, with applied site
            apportionment.
    """
    # Get the original columns set
    orig_cols: List[str] = list(df.columns)
    # Create a list of the value columns that we want to apportion
    # These are the same as the columns we impute so we use a function from
    # imputation.
    value_cols: List[str] = get_imputation_cols(config)

    # Calculate the number of unique non-blank postcodes
    df = count_unique_postcodes_in_col(df)

    # Set percentages to 100 in relevant rows
    df = set_percentages(df)

    # Split the dataframe in two based on whether there's one or more postcodes
    to_apportion_df, df_out = split_sites_df(df, imp_markers_to_keep)

    # category_df: dataframe with codes, textual and numerical values
    category_df = create_category_df(
        to_apportion_df,
        imp_markers_to_keep,
        orig_cols,
        groupby_cols,
        code_cols,
        site_cols,
        value_cols,
    )

    # sites_df: dataframe with sites, percents and everythung else
    sites_df = create_sites_df(to_apportion_df, groupby_cols, site_cols)

    # Calculate weights
    sites_df = calc_weights_for_sites(sites_df, groupby_cols)

    #  Merge codes to sites to create a Cartesian product
    df_cart = create_cartesian_product(sites_df, category_df)

    # Apply weights
    df_cart = weight_values(df_cart, value_cols, "site_weight")

    # Restore the original order of columns
    df_cart = df_cart[orig_cols]

    # Append the apportionned data back to the remaining unchanged data
    df_out = df_out.append(df_cart, ignore_index=True)

    # Sort by period, ref, instance in ascending order.
    df_out = sort_rows_order_cols(df_out, orig_cols)

    return df_out
