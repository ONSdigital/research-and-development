"""Module containing all functions relating to short form expansion.
"""

from typing import List, Union
import pandas as pd


formtype_long = "0001"
formtype_short = "0006"


def expansion_impute(
    group: pd.core.groupby.DataFrameGroupBy,
    master_col: str,
    break_down_cols: List[Union[str, int]],
    formtype_long,
    formtype_short,
) -> pd.DataFrame:
    """Calculate the expansion imputated values for short forms using long form data"""

    imp_class = group["imp_class"].values[0]
    print(imp_class)

    # Make cols into str just in case coming through as ints
    bd_cols = [str(col) for col in break_down_cols]

    # Make long and short masks
    long_mask = df["formtype"] == formtype_long
    short_mask = df["formtype"] == formtype_short

    # Sum the master column, e.g. "211" or "305" for the clear responders
    clear_statuses = ["Clear", "Clear - overridden"]
    responder_mask = group["status"].isin(clear_statuses)

    # Combination masks to select correct records for summing
    long_form_responder_mask = responder_mask & long_mask
    short_form_responder_mask = responder_mask & short_mask

    # Get long forms only for summing master_col
    sum_master_q_lng = group.loc[long_form_responder_mask][master_col].sum()  # scalar

    # Calculate the master returned value for each responder (will be a vector)
    master_return_vals = group[short_form_responder_mask][master_col].sum()  # vector

    # Calculate the imputation columns for the breakdown questions
    for bd_col in bd_cols:
        # Sum the breakdown q for the (clear) responders
        sum_breakdown_q = group.loc[long_form_responder_mask][bd_col].sum()

        # Make imputation col equal to original column
        group[f"{bd_col}_imputed"] = group[bd_col]

        # Update the imputation column for status encoded 100 and 201
        # i.e. for non-responders
        # unclear_statuses = ["Form sent out", "Check needed"]
        # unclear_mask = group["status"].isin(unclear_statuses)
        imputed_values = (sum_breakdown_q / sum_master_q_lng) * master_return_vals
        # Write imputed value to the non-responder records
        group.loc[short_form_responder_mask, f"{bd_col}_imputed"] = imputed_values

    # Returning updated group and updated QA dict
    return group


config = "dummy"

# make a dummy dataframe
df = pd.DataFrame([1, 2, 3, 4])

# Exclude the records from the reference list
refence_list = ["817"]
ref_list_excluded_df = df[~df.cellno.str.isin(refence_list)]

# Groupby imputation class
grouped_by_impclass = ref_list_excluded_df.groupby("imp_class")

# Get the breakdown qs from the config
breakdown_qs_2xx = config["breakdowns"]["2xx"]

grouped_by_impclass.apply(expansion_impute, "211", breakdown_qs_2xx)
