import logging

spp_processing_logger = logging.getLogger(__name__)


def full_responses(contributors, responses):

    """Merges contributor and response data together into a dataframe that is in a
    format allowing for easier manipulation later in pipeline - notably through
    having each questioncode as its own column.

    Arguments:
        contributors -- DataFrame containing contributor data for BERD
                        from SPP Snapshot file
        responses -- DataFrame containing response data for BERD from SPP Snapshot file

    Returns:
        full_responses -- DataFrame containing both response and contributor data
    """

    drop_cols = ["createdby", "createddate", "lastupdatedby", "lastupdateddate"]

    unique_id_cols = ["reference", "period", "survey"]

    contributors_dropped = contributors.drop(drop_cols, axis=1)
    responses_dropped = responses.drop(drop_cols + ["adjustedresponse"], axis=1)

    merged_df = contributors_dropped.merge(responses_dropped, on=unique_id_cols)

    contextual_df = merged_df.drop(
        ["questioncode", "response"], axis=1
    ).drop_duplicates()

    response_df = merged_df.pivot_table(
        index=unique_id_cols, columns="questioncode", values="response", aggfunc="first"
    ).reset_index()

    full_responses = response_df.merge(contextual_df, on=unique_id_cols)

    return full_responses


def response_rate(contributors, responses):

    """Generates a response rate based on the contributor and response data
    from the SPP Snapshot file.

    Arguments:
        contributors -- DataFrame containing contributor data for BERD
                        from SPP Snapshot file
        responses -- DataFrame containing response data for BERD from SPP Snapshot file

    Returns:
        response_rate -- Float representing proportion of contributors who responded
    """

    no_responses = len(responses["reference"].unique())
    no_contributors = len(contributors["reference"].unique())

    response_rate = no_responses / no_contributors

    spp_processing_logger.info(f"The SPP response rate is {round(response_rate,2)}%")

    return response_rate
