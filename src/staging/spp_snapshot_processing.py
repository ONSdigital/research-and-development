from src.utils.wrappers import validate_dataframe_not_empty
from typing import List
import pandas as pd
import logging

spp_processing_logger = logging.getLogger(__name__)


def create_response_dataframe(
    df: pd.DataFrame, unique_id_cols: List[str]
) -> pd.DataFrame:
    """Create a response dataframe using pivot_table to reshape the data.

    Arguments:
        df -- DataFrame to create the response dataframe from
        unique_id_cols -- List of column names that uniquely identify the data

    Returns:
        response_df -- Response DataFrame
    """
    response_df = df.pivot_table(
        index=unique_id_cols, columns="questioncode", values="response", aggfunc="first"
    ).reset_index()
    response_df = response_df.astype({"instance": "Int64"})
    return response_df


def create_contextual_dataframe(
    df: pd.DataFrame, unique_id_cols: List[str]
) -> pd.DataFrame:
    """Create a contextual dataframe by dropping 'questioncode' and 'response' columns
    and removing duplicates.

    Arguments:
        df -- DataFrame to create the contextual dataframe from
        unique_id_cols -- List of column names that uniquely identify the data

    Returns:
        contextual_df -- Contextual DataFrame
    """
    cols_to_drop = ["questioncode", "response"]
    contextual_df = df.drop(cols_to_drop, axis=1).drop_duplicates()
    return contextual_df


@validate_dataframe_not_empty
def full_responses(contributors: pd.DataFrame, responses: pd.DataFrame) -> pd.DataFrame:

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

    drop_cols = ["createdby", "createddate", "lastupdatedby"]

    unique_id_cols = ["reference", "instance"]

    contributors_dropped = contributors.drop(drop_cols, axis=1)
    responses_dropped = responses.drop(drop_cols + ["lastupdateddate", "adjustedresponse"], axis=1)

    responses_dropped = responses_dropped.astype({"instance": "Int64"})

    merged_df = contributors_dropped.merge(
        responses_dropped, on=["reference", "survey", "period"], how="outer"
    )
    # Create contextual df by dropping "questioncode" and "response" cols. Remove dupes
    contextual_df = create_contextual_dataframe(merged_df, unique_id_cols)

    # Create a response dataframe using pivot_table to reshape the data
    response_df = create_response_dataframe(merged_df, unique_id_cols)

    full_responses = response_df.merge(contextual_df, on=unique_id_cols, how="outer")

    return full_responses


@validate_dataframe_not_empty
def response_rate(contributors: pd.DataFrame, responses: pd.DataFrame) -> float:

    """Generates a response rate based on the contributor and response data
    from the SPP Snapshot file.

    Arguments:
        contributors -- DataFrame containing contributor data for BERD
                        from SPP Snapshot file
        responses -- DataFrame containing response data for BERD from SPP Snapshot file

    Returns:
        response_rate -- Float representing proportion of contributors who responded
    """
    # Determine num of responses
    response_count = len(responses["reference"].unique())
    # Determine the number of contributors
    contributor_count = len(contributors["reference"].unique())

    response_rate = response_count / contributor_count

    rounded_resp_rate = round(response_rate, 2)

    spp_processing_logger.info(f"The response rate is {rounded_resp_rate}%")

    return response_rate
