"""Return a dictionary for the cell_no mapper"""
import pandas as pd


def clean_thousands_comma(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Remove commas from numbers in columns of a dataframe and convert to integer."""
    for col in columns:
        df[col] = df[col].str.replace(",", "").astype(int)

    return df


def join_cellno_mapper(df: pd.DataFrame, cellno_df: pd.DataFrame) -> pd.DataFrame:
    """Add a column for universe count to shortfrom responses, joining on cellnumber."""
    mapper = cellno_df.copy()[["cell_no", "UNI_Count"]]
    mapper = mapper.rename(columns={"UNI_Count", "uni_count"})

    # condition for shorot form responses
    sf_cond = df[df.formtype == "0006"]

    # merge the mapper to the dataframe
    df.loc[sf_cond] = df.loc[sf_cond].merge(
        cellno_df, left_on="cellnumber", right_on="cell_no"
    )

    return df
