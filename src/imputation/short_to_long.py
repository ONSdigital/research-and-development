import pandas as pd


def short_to_long(df, civil_column, defence_column, new_column):
    """Convert short form columns into long form columns

    Args:
        df (pd.DataFrame): Input full responses DataFrame
        civil_column (string): Short form column referencing civil spending.
        defence_column (string): Short form column referencing defence spending.
        new_column (string): Combined long form column where values correspond to either
                             civil or defence (indicated by 'C' or 'D' in column '200'.)
    """
    civil_df = create_instance_df(df.copy(), civil_column, new_column, "C", 1)
    defence_df = create_instance_df(df.copy(), defence_column, new_column, "D", 2)

    return pd.concat([df, civil_df, defence_df], ignore_index=True)


def create_instance_df(df, old_column, new_column, type, instance_no):
    """Create a sub_df where:

        - values from `old_column` have been copied into `new_column`
        - column 200 has been updated to reflect the type of spending
        - instance column has been updated to reflect the instance number

    Args:
        df (pd.DataFrame): DataFrame containing short form responses
        old_column (string): The old column to be converted
        new_column (string): The new column we are converting to
        type (string): The type of spending - either 'C' or 'D'
        instance_no (int): The instance number that these will be
    """
    df.loc[
        df["formtype"] == "0006",
    ]
    df[new_column] = df[old_column]
    df["200"] = type
    df["instance"] = instance_no
    return df
