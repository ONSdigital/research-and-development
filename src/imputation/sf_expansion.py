"""Module containing all functions relating to short form expansion.
"""

from typing import List, Union
import pandas as pd


def evaluate_imputed_ixx(
    group: pd.core.groupby.DataFrameGroupBy,
    master_col: str,
    break_down_cols: List[Union[str, int]],
) -> pd.DataFrame:
    """Evaluate the imputed 2xx or 3xx as the sum of all 2xx or 3xx
    over the sum of all 211 or 305 values, multiplied by the imputed 211."""

    imp_class = group["imp_class"].values[0]
    print(imp_class)

    # Make cols into str just in case coming through as ints
    bd_cols = [str(col) for col in break_down_cols]

    # Sum the master column, e.g. "211" or "305" for the clear responders
    clear_statuses = ["Clear", "Clear - overridden"]
    clear_mask = group["status"].isin(clear_statuses)
    sum_master_q = group.loc[clear_mask][master_col].sum()  # scalar

    # Calculate the imputation columns for the breakdown questions
    for bd_col in bd_cols:
        # Sum the breakdown q for the (clear) responders
        sum_breakdown_q = group.loc[clear_mask][bd_col].sum()

        # Master col imputed, e.g. 211_imputed or 305_imputed
        # Sum the breakdown master q imputed for TMI records
        TMI_mask = group["imp_marker"] == "TMI"
        master_imp_val = group.loc[TMI_mask][f"{master_col}_imputed"]
        if master_imp_val.values.any():
            master_imp_val = master_imp_val.values[0]  # get a single value
        else:
            master_imp_val = float("nan")  # assign nan where there are no vals

        # Make imputation col equal to original column
        group[f"{bd_col}_imputed"] = group[bd_col]

        # Update the imputation column for status encoded 100 and 201
        # i.e. for non-responders
        unclear_statuses = ["Form sent out", "Check needed"]
        unclear_mask = group["status"].isin(unclear_statuses)
        imputed_value = (sum_breakdown_q / sum_master_q) * master_imp_val
        # Write imputed value to the non-responder records
        group.loc[unclear_mask, f"{bd_col}_imputed"] = imputed_value

    # Returning updated group and updated QA dict
    return group
