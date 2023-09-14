"""Map the missing columns that are required for the outputs"""

import pandas as pd
import numpy as np


def join_fgn_ownership(main_df: pd.DataFrame, mapper_df: pd.DataFrame) -> pd.DataFrame:
    """
    Combine two DataFrames using a left join based on specified columns.

    Args:
        main_df (pd.DataFrame): The main DataFrame.
        mapper_csv_path (pd.DataFrame): The mapper DataFrame.

    Returns:
        pd.DataFrame: The combined DataFrame resulting from the left join.
    """
    try:
        # Perform left join
        combined_df = main_df.merge(
            mapper_df, how="left", left_on="reference", right_on="ruref"
        )
        combined_df.drop(columns=["ruref"], inplace=True)

        return combined_df

    except Exception as e:
        raise ValueError(
            "An error occurred while combining main_df and mapper_df: " + str(e)
        )


def map_sizebands(
    df: pd.DataFrame,
):
    """Generate sizebands from the frozen employent column

    Args:
        df (pd.DataFrame): The original dataframe

    Returns:
        (pd.DataFrame): The dataframe with the sizebands column added
    """
    # Create a dictionary of sizeband parameters
    sizeband_dict = {
        1: {"min": 0, "max": 9},
        2: {"min": 10, "max": 99},
        3: {"min": 20, "max": 49},
        4: {"min": 50, "max": 99},
        5: {"min": 100, "max": 249},
        6: {"min": 250, "max": np.inf},
    }

    # Create empty column
    df["sizeband"] = np.nan

    # Create conditions for sizebands
    conditions = [
        (sizeband_dict[key]["min"] <= df["frozenemployment"])
        & (df["frozenemployment"] <= sizeband_dict[key]["max"])
        for key in sizeband_dict.keys()
    ]
    decisions = [key for key in sizeband_dict.keys()]

    # Apply the sizebands
    for i in range(len(conditions)):
        df.loc[conditions[i], "sizeband"] = decisions[i]

    # Convert datatype to int
    df["sizeband"] = df["sizeband"].astype("Int64")

    return df


def create_cora_status_col(df, mapper_df, main_col="statusencoded"):
    """_summary_

    Args:
        df (pd.DataFrame): main data containing responses
        mapper_df (pd.DataFrame): mapper with cora status equivalents
        main_col (str, optional): Defaults to "statusencoded".

    Returns:
        df: main data with cora status column added
    """

    # Create hardcoded dictionary for mapping if csv is not used
    cora_dict = {
        "statusencoded": [100, 101, 102, 200, 201, 210, 211, 302, 303, 304, 309],
        "form_status": [200, 100, 1000, 400, 500, 600, 800, 1200, 1300, 900, 1400],
    }

    # convert mapper df to dictionary
    mapper_dict = dict(zip(mapper_df[main_col], mapper_df["form_status"]))

    # Create a new column by mapping values from main_col using the
    # mapper dictionary
    df["form_status"] = df[main_col].map(mapper_dict)

    return df
