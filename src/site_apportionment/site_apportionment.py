import pandas as pd
from pandas.api.types import is_numeric_dtype
import os
import logging

SitesApportionmentLogger = logging.getLogger(__name__)

# Colunm names redefined for convenience
ref = "reference"
ins = "instance"
period = "period"
form = "formtype"
postcode = "postcodes_harmonised"
percent = "602"
product = "201"
pg_num = "pg_numeric"
civdef = "200"

# Long and short form codes
short_code = "0006"
long_code = "0001"


def count_unique_codes_in_col(df: pd.DataFrame, code: str) -> pd.DataFrame:
    """Calculates the number of unique non-empty codes in a column.

    Args:
        df (pd.DataFrame): A dataframe containing all data
        code (str): Name of the column containing codes

    Returns:
        (pd.DataFrame): A copy of originl dataframe with an additional column
        called the same as code with suffix "_count" countaining the number of
        unique non-empty codes
    """

    dfa = df.copy()

    # Select columns that we need
    cols_need = [ref, period, code]
    dfa = dfa[cols_need]
    dfa = dfa[dfa[code].str.len() > 0]
    dfa.drop_duplicates(inplace=True)
    dfb = dfa.groupby([ref, period]).agg("count").reset_index()
    dfb.rename({code: code + "_count"}, axis='columns', inplace=True)
    df = df.merge(
        dfb,
        on=[ref, period],
        how="left")
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
    dfc = df.copy()
    dfc["site_percent"] = dfc[percent]
    dfc["site_percent"].fillna(0, inplace=True)

    # Set the percent for instance 0 to be 0
    dfc["site_percent"] = dfc["site_percent"] * dfc[ins].astype("bool")

    # Calculate the total percent for each reference and period
    dfc["site_percent_total"] = (
        dfc.groupby([ref, period])["site_percent"].transform("sum"))

    # Filter out the rows where total percent is zero
    dfc = dfc[dfc["site_percent_total"] != 0]

    # Compute weights
    dfc["site_weight"] = dfc["site_percent"] / dfc["site_percent_total"]

    # Remove unnecessary
    dfc.drop(columns=['site_percent', 'site_percent_total'], inplace=True)

    return dfc


def apportion_sites(df: pd.DataFrame)-> pd.DataFrame:
    """Apportions the numerical values for each product group across multiple
    sites, using percents from question 602 to compute weights.
    Selects the records long from references that have multiple non-empty
    postcodes. Splits the dataframe in two, codes and sites. Codes have
    reference, period, product group (unique combinations of product, civil or
    defence and pg_numeric) and (all numeric columns. Sites have reference,
    period and all other fields except for product group keys and numerical
    values. Sites dataframe contains multiple instances, with instance 1 and
    higher having different postcodes and with percents for each site.
    For sites, weights are calculated using the percents. Then, a Cartesian
    product of product groups and sites is created, and the weights of each site
    are applied to values of each product. Also, for short forms, sets percent
    to 100.

    Args:
        df (pd.DataFrame): Dataframe containing all input data

    Returns:
        (pd.DataFrame): A dataframe with the same columns, with applied site
        apportionment
    """

    # Value columns that we want to apportion
    want_cals = [str(x) for x in range(202, 509)]

    # Clean "NONE" postcodes
    df.loc[df[postcode] == "NONE    ", postcode] = ""

    # Set short form percentages to 100
    df.loc[df[form] == short_code, percent] = 100

    # Calculate values column names
    # df_cols: original columns
    df_cols = list(df.columns)

    # exist_cols: the ones we want, which are present in the data
    exist_cols = [x for x in want_cals if x in df_cols]

    # value_cols: the ones we want and present and numeric
    value_cols = [x for x in exist_cols if is_numeric_dtype(df[x])]

    # Calculate the number of uniqie non-blank postcodes
    df = count_unique_codes_in_col(df, postcode)

    # Condition of long forms, many sites, instance >=1, non-null postcodes
    cond = (
        (df[form] == long_code) &
        (df[postcode + "_count"] > 1) &
        (df[ins] >= 1) &
        (df[postcode].str.len() > 0)
    )

    # Dataframe dfm with many products - for apportionment and Cartesian product
    dfm = df[cond]
    dfm.drop(columns=[postcode + "_count"], inplace=True)

    # Dataframe with everything else - save unchanged
    df_out = df[~cond]
    df_out.drop(columns=[postcode + "_count"], inplace=True)

    # df_codes: dataframe with codes and numerical values
    group_cols = [ref, period]
    code_cols = [product, civdef, pg_num]
    df_codes = dfm.copy()[group_cols + code_cols + value_cols]

    # Remove blank products
    df_codes = df_codes[df_codes[product].str.len() > 0]

    # De-duplicate by summation - possibly, not needed
    value_dict = {value_col: 'sum' for value_col in value_cols}
    df_codes = (
        df_codes.groupby(group_cols + code_cols).agg(value_dict).reset_index()
    )

    # df_stes: dataframe with postcodes, percents, and everyting else
    site_cols = [x for x in df_cols if x not in (code_cols + value_cols)]
    df_sites = dfm.copy()[site_cols]

    # Check for postcode duplicates for QA
    df_sites["site_count"] = (
        df_sites.groupby(group_cols + [postcode])[postcode].transform("count")
    )
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
    df_out.sort_values(by=[period, ref, ins], ascending=True, inplace=True)

    return df_out


if __name__ == "__main__":
    mydir = r"D:\data\res_dev\outputs\reg_apport"
    in_file = "outputs_df_before.pkl"
    out_file = "df_out.csv"
    mypath = os.path.join(mydir, in_file)
    df = pd.read_pickle(mypath)
    print(f"Input df is read. Dataframe shape:\n{df.shape}")

    df_out = apportion_sites(df)

    mypath = os.path.join(mydir, out_file)
    df_out.to_csv(mypath, index=None)
    print(f"Output is saved")
