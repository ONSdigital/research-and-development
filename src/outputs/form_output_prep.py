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
    # Deal with "No" in 604, also eliminating spenders
    flag_no_rand_spenders(estimated_df, "error")
    no_rnd_spenders_filter = ~(
        (estimated_df["604"] == "No") & (estimated_df["211"] > 0)
    )
    outputs_df = estimated_df.copy().loc[no_rnd_spenders_filter]
    tau_outputs_df = weighted_df.copy().loc[no_rnd_spenders_filter]

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

        return ni_full_responses, outputs_df, tau_outputs_df

    else:
        # create an empty ni_responses dataframe
        ni_full_responses = pd.DataFrame()

        return ni_full_responses, outputs_df, tau_outputs_df
