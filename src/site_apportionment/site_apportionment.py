import pandas as pd
from pandas.api.types import is_numeric_dtype
import os


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


def count_unique_codes_in_col(df: pd.DataFrame, code) -> pd.DataFrame:
    """Calculates the number of unique non-empty codes"""

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
    """Calculates site weights based on the percents"""
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
    sites, using percents as weights.
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
    dfm = count_unique_codes_in_col(df, postcode)

    # Condition of long forms, many sites, instance 1 and above, non-null postcodes
    cond_mm = (
        (dfm[form] == long_code) &
        (dfm[postcode + "_count"] > 1) &
        (dfm[ins] >= 1) &
        (dfm[postcode].str.len() > 0)
    )

    # Dataframe witm many products - for apportionment and Cartesian product
    dfmm = dfm[cond_mm]
    dfmm.drop(columns=[postcode + "_count"], inplace=True)

    # Dataframe with everything else - save unchanged
    df_out = dfm[~cond_mm]
    df_out.drop(columns=[postcode + "_count"], inplace=True)


    # df_codes: dataframe with codes and numerical values
    group_cols = [ref, period]
    code_cols = [product, civdef, pg_num]
    df_codes = dfmm.copy()[group_cols + code_cols + value_cols]

    # Remove blank products
    df_codes = df_codes[df_codes[product].str.len() > 0]

    # De-duplicate by summation - possibly, not needed
    value_dict = {value_col: 'sum' for value_col in value_cols}
    df_codes = (
        df_codes.groupby(group_cols + code_cols).agg(value_dict).reset_index()
    )

    # df_stes: dataframe with postcodes, percents, and everyting else
    site_cols = [x for x in df_cols if x not in (code_cols + value_cols)]
    df_sites = dfmm.copy()[site_cols]

    # check for postcode duplicates
    df_sites["site_count"] = (
        df_sites.groupby(group_cols + [postcode])[postcode].transform("count")
    )
    df_duplicate_sites = df_sites[df_sites["site_count"] > 1]
    num_duplicate_sites = df_duplicate_sites.shape[0]
    if num_duplicate_sites:
        print(f"There are {num_duplicate_sites} duplicate sites")

    # Calculate weights
    df_sites = weights(df_sites)

    #  Merge codes to sites to create a Cartesian product
    df_cart = df_sites.merge(df_codes, on=group_cols, how="inner")

    # Apply weights
    for value_col in value_cols:
        df_cart[value_col] = df_cart[value_col] * df_cart["site_weight"]

    # Restore the original column order
    df_cart = df_cart[df_cols]

    # Append the columns back to the original df
    df_out = df_out.append(df_cart, ignore_index=True)

    # Order by period, ref, instance, ASC
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
