import pandas as pd
from src.staging.pg_conversion import run_pg_conversion
from src.staging.validation import flag_no_rand_spenders


def form_output_prep(
    estimated_df: pd.DataFrame,
    weighted_df: pd.DataFrame,
    ni_full_responses: pd.DataFrame,
    pg_num_alpha: pd.DataFrame,
    sic_pg_alpha: pd.DataFrame,
):

    """Prepares the data for the outputs.

    Args:
        estimated_df (pd.DataFrame): The main dataset containing
        short and long form output
        weighted_df (pd.DataFrame): Dataset with weights computed but not applied
        ni_full_responses(pd.DataFrame): Dataset with all NI data
        pg_num_alpha (pd.DataFrame): Mapper for product group conversions (num to alpha)
        sic_pg_alpha (pd.DataFrame): Mapper for product group conversions (SIC to alpha)

    Returns:
        ni_full_responses (pd.DataFrame): If available, prepared NI data
        outputs_df (pd.DataFrame): estimated GB data
        tau_outputs_df (pd.DataFrame): weighted UK data
        filtered_output_df (pd.DataFrame): data noot used in outputs

    """

    imputed_statuses = ["TMI", "CF", "MoR", "constructed"]

    to_keep = estimated_df["imp_marker"].isin(imputed_statuses) | (
        estimated_df["imp_marker"] == "R"
    )

    # Deal with "No" in 604, also eliminating spenders
    flag_no_rand_spenders(estimated_df, "warn")
    no_rnd_spenders_filter = ~(
        (estimated_df["604"] == "No") & (estimated_df["211"] > 0)
    )
    estimated_df = estimated_df.copy().loc[no_rnd_spenders_filter]

    # filter estimated_df and weighted_df to only include clear or imputed statuses
    outputs_df = estimated_df.copy().loc[to_keep]
    tau_outputs_df = weighted_df.copy().loc[to_keep]

    # filter estimated_df for records not included in outputs
    filtered_output_df = estimated_df.copy().loc[~to_keep]

    if ni_full_responses is not None:
        # Add required columns to NI data
        ni_full_responses = ni_full_responses.rename(
            columns={"period_year": "period", "foc": "ultfoc"}
        )
        ni_full_responses["a_weight"] = 1
        ni_full_responses["604"] = "Yes"
        ni_full_responses["form_status"] = 600
        ni_full_responses["602"] = 100
        ni_full_responses["formtype"] = "0003"
        ni_full_responses = run_pg_conversion(
            ni_full_responses, pg_num_alpha, sic_pg_alpha, target_col="201"
        )

        # outputs_df = pd.concat([outputs_df, ni_full_responses])
        tau_outputs_df = pd.concat([tau_outputs_df, ni_full_responses])

        # change the value of the status column to 'imputed' for imputed statuses
        condition = outputs_df["status"].isin(imputed_statuses)
        outputs_df.loc[condition, "status"] = "imputed"

        return ni_full_responses, outputs_df, tau_outputs_df, filtered_output_df

    else:

        # change the value of the status column to 'imputed' for imputed statuses
        condition = outputs_df["status"].isin(imputed_statuses)
        outputs_df.loc[condition, "status"] = "imputed"

        # create an empty ni_responses dataframe
        ni_full_responses = pd.DataFrame()

        return ni_full_responses, outputs_df, tau_outputs_df, filtered_output_df