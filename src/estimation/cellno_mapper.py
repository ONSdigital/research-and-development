"""Return a dictionary for the cell_no mapper"""
import pandas as pd


def cellno_unit_dict(cellno_df: pd.DataFrame) -> dict:
    """To creted dictioanry from The berd_2022_cellno_coverage.xlsx
    going to be use for mapping


    Args:
        cellno_df (pd.DataFrame):cellno coverage data frame

    Returns:
        dict: Dictionary contains cell_no as key, UNI_Count as values
    """

    # Filtering object columns then Convert to object columns as integer
    object_col = [col for col in cellno_df.columns if cellno_df[col].dtypes == "object"]
    for col in object_col:
        cellno_df[col] = cellno_df[col].str.replace(",", "").astype(int)

    # Creating dictionary cell_no and UNI_count
    cell_unit_dict = cellno_df.set_index("cell_no").to_dict()["UNI_Count"]

    return cell_unit_dict
