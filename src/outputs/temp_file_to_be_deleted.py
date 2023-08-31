import pandas as pd

from src.utils.wrappers import exception_wrap


@exception_wrap
def combine_dataframes(
    main_df: pd.DataFrame, mapper_csv_path: pd.DataFrame
) -> pd.DataFrame:
    """
    Combine two DataFrames using a left join based on specified columns.

    Args:
        main_df (pandas.DataFrame): The main DataFrame.
        mapper_csv_path (str): The path to the CSV file for the mapper DataFrame.

    Returns:
        pandas.DataFrame: The combined DataFrame resulting from the left join.
    """
    try:
        # Read the mapper DataFrame from CSV
        mapper_df = pd.read_csv(mapper_csv_path)

        # Perform left join
        combined_df = main_df.merge(
            mapper_df, how="left", left_on="reference", right_on="ruref"
        )

        return combined_df

    except Exception as e:
        raise ValueError(
            "An error occurred while combining main_df and mapper_df: " + str(e)
        )


# TODO PLACEHOLDER UNTIL I FIND THE LOCATION TO PLUG IN
main_csv_path = "main_dataframe.csv"
mapper_csv_path = "path_to_mapper_dataframe.csv"

# Read the main DataFrame from CSV
main_df = pd.read_csv(main_csv_path)

# Call the combine_dataframes function
combined_df = combine_dataframes(main_df, mapper_csv_path)

print(combined_df)
