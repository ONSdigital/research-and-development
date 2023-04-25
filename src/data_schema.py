import pandas as pd


def read_xlsx(excel_file) -> pd.DataFrame:
    """_summary_

    Arguments:
        excel_file -- _description_

    Returns:
        _description_
    """
    xl_dataframe = pd.read_excel(excel_file, "Sheet1")
    return xl_dataframe


# def convert_dataFrame(pdFrame)->dict:
#    """_summary_
#
#    Arguments:
#        pdFrame -- _description_
#
#    Returns:
#        _description_
#    """
#    return
#
#
# def create_toml(pdDict)->str:
#    """_summary_
#
#    Arguments:
#        pdDict -- _description_
#
#    Returns:
#        _description_
#    """
#    return
#
