import pandas as pd
from src.staging.validation import flag_no_rand_spenders
from src.estimation.apply_weights import apply_weights
from src.outputs.outputs_helpers import create_period_year


def form_output_prep(
    weighted_df: pd.DataFrame,
    ni_full_responses: pd.DataFrame,
    config: dict,
):
    """Prepares the data for the outputs.

    Args:
        weighted_df (pd.DataFrame): Dataset with weights computed but not applied
        ni_full_responses(pd.DataFrame): Dataset with all NI data
        config (dict): The configuration settings.

    Returns:
        ni_full_responses (pd.DataFrame): If available, prepared NI data
        outputs_df (pd.DataFrame): estimated GB data
        tau_outputs_df (pd.DataFrame): UK data without estimation weights applied
    """
    # Deal with "No" in 604, also eliminating spenders
    flag_no_rand_spenders(weighted_df, "error")
    no_rnd_spenders_filter = ~((weighted_df["604"] == "No") & (weighted_df["211"] > 0))
    instance_0_filter = weighted_df["instance"] != 0

    tau_outputs_df = weighted_df.copy().loc[no_rnd_spenders_filter & instance_0_filter]
    tau_outputs_df = create_period_year(tau_outputs_df)

    # Now that the tau outputs have been created, we can apply the weights to the
    # weighted_df to get the estimated values.
    estimated_df = apply_weights(weighted_df, config, for_qa=False)

    outputs_df = estimated_df.copy().loc[no_rnd_spenders_filter]
    outputs_df = create_period_year(outputs_df)

    if ni_full_responses is not None:
        # outputs_df = pd.concat([outputs_df, ni_full_responses])
        tau_outputs_df = pd.concat([tau_outputs_df, ni_full_responses])

        return ni_full_responses, outputs_df, tau_outputs_df

    else:
        # create an empty ni_responses dataframe
        ni_full_responses = pd.DataFrame()

        return ni_full_responses, outputs_df, tau_outputs_df
