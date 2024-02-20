"""Map the missing columns that are required for the outputs"""

import pandas as pd
import numpy as np


def join_pg_numeric(
    main_df: pd.DataFrame, mapper_df: pd.DataFrame, cols_pg: list = ["201"]
) -> pd.DataFrame:
    """
    Add a new column with numeric PD using a mapper.

    Args:
        main_df (pd.DataFrame): The main DataFrame.
        mapper_df (pd.DataFrame): The mapper DataFrame.
        cols_pg (list): PG clumns to be converted from alpha to numeric

    Returns:
        pd.DataFrame: The combined DataFrame resulting from the left join.
    """
    for mycol in cols_pg:
        try:
            # Perform left join
            combined_df = main_df.merge(
                mapper_df, how="left", left_on=mycol, right_on="pg_alpha"
            )
            combined_df.rename(columns={"pg_numeric": mycol + "_numeric"}, inplace=True)
            combined_df = combined_df.drop(columns=["pg_alpha"])

        except Exception as e:
            raise ValueError(
                "An error occurred while combining main_df and mapper_df: " + str(e)
            )
    return combined_df


def join_fgn_ownership(
    main_df: pd.DataFrame, mapper_df: pd.DataFrame, formtype: list = ["0001", "0006"]
) -> pd.DataFrame:
    """
    Combine two DataFrames using a left join based on specified columns.

    Args:
        main_df (pd.DataFrame): The main DataFrame.
        mapper_csv_path (pd.DataFrame): The mapper DataFrame.
        formtype (list): List of the formtypes to run through function

    Returns:
        pd.DataFrame: The combined DataFrame resulting from the left join.
    """

    try:
        to_keep = main_df["formtype"].isin(formtype)
        # filter for long and short forms only
        filtered_df = main_df.copy().loc[to_keep]

        # the remainder of the dataframe is the NI data
        ni_df = main_df.copy().loc[~to_keep]

        # Perform left join on filtered dataframe
        combined_df = filtered_df.merge(
            mapper_df, how="left", left_on="reference", right_on="ruref"
        )
        combined_df.drop(columns=["ruref"], inplace=True)
        
        old_cols = list(main_df.columns)
        new_cols = list(combined_df.columns)
        if "ultfoc_y" in new_cols:
            combined_df = combined_df.rename(columns={"ultfoc_y": "ultfoc"})
            combined_df = combined_df[old_cols]
         

        main_df = pd.concat([combined_df, ni_df]).reset_index(drop=True)

        # If foreign ownership is empty, we fill it with "GB" for long, short
        # and NI
        main_df["ultfoc"] = main_df["ultfoc"].fillna("GB")

        return main_df

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
        (sizeband_dict[key]["min"] <= df["employment"])
        & (df["employment"] <= sizeband_dict[key]["max"])
        for key in sizeband_dict.keys()
    ]
    decisions = [key for key in sizeband_dict.keys()]

    # Apply the sizebands
    for i in range(len(conditions)):
        df.loc[conditions[i], "sizeband"] = decisions[i]

    # Convert datatype to int
    df["sizeband"] = df["sizeband"].astype("Int64")

    return df


def create_cora_status_col(df, main_col="statusencoded"):
    """Creates a new column named form_status by mapping
    the statusencoded column using a hardcoded dictionary.

    Args:
        df (pd.DataFrame): main data containing responses
        main_col (str, optional): Defaults to "statusencoded".

    Returns:
        df: main data with cora status column added
    """
    # Create hardcoded dictionary for mapping
    status_before = ["100", "101", "102", "200", "201", "210", "211", "302", "303", "304", "309"]
    status_after = ["200", "100", "1000", "400", "500", "600", "800", "1200", "1300", "900", "1400"]

    cora_dict = dict(zip(status_before, status_after))

    # Create a new column, if required, and map values from main_col
    # using the cora_dict.  NI already have form_status,
    # so it only deals with rows with a value in the main col
    if "form_status" not in df.columns:
        df["form_status"] = None
    df.loc[df["form_status"].isnull(), "form_status"] = df.loc[
        df["form_status"].isnull(), main_col
    ].map(cora_dict)

    return df


def join_itl_regions(
    df: pd.DataFrame,
    postcode_mapper: pd.DataFrame,
    postcode_col="postcodes_harmonised",
    formtype: list = ["0001", "0006"],
):
    """Joins the itl regions onto the full dataframe using the mapper provided

    Args:
        df (pd.DataFrame): Full dataframe
        postcode_mapper (pd.DataFrame): Mapper containing postcodes and regions
        formtype (list): List of the formtypes to run through function

    Returns:
        df: Dataframe with column "ua_county" for regions
    """
    try:
        to_keep = df["formtype"].isin(formtype)

        # filter for long and short forms only
        filtered_df = df.copy().loc[to_keep]

        # the remainder of the dataframe is the NI data
        ni_df = df.copy().loc[~to_keep]

        # Perform left join on filtered dataframe
        df = filtered_df.merge(
            postcode_mapper, how="left", left_on=postcode_col, right_on="pcd2"
        )
        df.drop(columns=["pcd2"], inplace=True)

        ni_df["itl"] = "N92000002"

        complete_df = pd.concat([df, ni_df]).reset_index(drop=True)

        return complete_df

    except Exception as e:
        raise ValueError(
            "An error occurred while combining df and postcode_mapper: " + str(e)
        )


def map_to_numeric(df: pd.DataFrame):
    """Map q713 and q714 in dataframe from letters to numeric format
    Yes is mapped to 1
    No is mapped to 2
    Unanswered is mapped to 3
    Others are mapped to None

    Args:
        df (pd.DataFrame): The original dataframe

    Returns:
        df: Dataframe with numeric values for q713/714
    """

    df = df.astype({"713": "object", "714": "object"})
    # Map the actual responses to the corresponding integer
    mapper_dict = {"Yes": 1, "No": 2, "": 3}

    df["713"] = df["713"].map(mapper_dict)
    df["714"] = df["714"].map(mapper_dict)

    # Convert all nulls to unanswered (map to 3)
    df.loc[df["713"].isnull(), "713"] = 3
    df.loc[df["714"].isnull(), "714"] = 3

    # Return columns as integers
    df = df.astype({"713": "Int64", "714": "Int64"})
    return df
