import pandas as pd
from typing import Tuple


def parse_snap_data(snapdata: dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Loads the data from the survey via the SPP snapshot. The data is supplied as dict
        and is parsed into dataframes, one for survey contributers (company details)
        and another one for their responses.

    Args:
        snapdata (dict, optional): The data from the SPP snapshot. Defaults to snapdata.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: The contributers and responders dataframes
    """
    # Load the dicts!
    contributordict = snapdata["contributors"]
    responsesdict = snapdata["responses"]

    # Make dataframes
    contributors_df = pd.DataFrame(contributordict)
    responses_df = pd.DataFrame(responsesdict)

    return contributors_df, responses_df
