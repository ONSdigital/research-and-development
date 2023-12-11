import pandas as pd
from numpy import random


def postcode_topup(mystr: str, target_len: int = 8) -> str:
    """Regulates the number of spaces between the first and the second part of
    a postcode, so that the total length is 8 characters.
    Brings all letters to upper case, in line with the mapper.
    Splits the postcode string into parts, separated by any number of spaces.
    If there are two or more parts, the first two parts are used.
    The third and following parts, if present, are ignored.
    Calculates how many spaces are needed so that the total length is 8.
    If the total length of part 1 and part 2 is already 8, no space will be
    inserted.
    If their total length is more than 8, joins part 1 and part 2 without
    spaces and cuts the tail on the right.
    If there is only one part, keeps the first 8 characters and tops it up with
    spaces on the right if needed.
    Empty input string would have zero parts and will return a string of
    eight spaces.

    Args:
        mystr (str): Input postcode.
        target_len (int): The desired length of the postcode after topping up.

    Returns:
        str: The postcode topped up to the desired number of characters.
    """
    if pd.notna(mystr):
        mystr = mystr.upper()
        parts = mystr.split()

        if len(parts) == 1:
            mystr = mystr.strip()
            part1 = mystr[:-3]
            part2 = mystr[-3:]

            num_spaces = target_len - len(part1) - len(part2)
            if num_spaces >= 0:
                return part1 + " " * num_spaces + part2
            else:
                return (part1 + part2)[:target_len]

        elif len(parts) >= 2:
            part1 = parts[0]
            part2 = parts[1]

            num_spaces = target_len - len(part1) - len(part2)
            if num_spaces >= 0:
                return part1 + " " * num_spaces + part2
            else:
                return (part1 + part2)[:target_len]

        else:
            return mystr[:target_len].ljust(target_len, " ")



def fix_anon_data(responses_df, config):
    """
    Fixes anonymised snapshot data for use in the DevTest environment.

    This function adds an "instance" column to the provided DataFrame, and populates
    it with zeros. It also adds a "selectiontype" column with random values of "P",
    "C", or "L", and a "cellnumber" column with random values from the "seltype_list"
    in the configuration.

    This fix is necessary because the anonymised snapshot data currently used in the
    DevTest environment does not include the "instance" column. This fix should be
    removed when new anonymised data is provided.

    Args:
        responses_df (pandas.DataFrame): The DataFrame containing the anonymised
        snapshot data.
        config (dict): A dictionary containing configuration details.

    Returns:
        pandas.DataFrame: The fixed DataFrame with the added "instance",
        "selectiontype", and "cellnumber" columns.
    """
    responses_df["instance"] = 0
    col_size = responses_df.shape[0]
    random.seed(seed=42)
    responses_df["selectiontype"] = random.choice(["P", "C", "L"], size=col_size)
    cellno_list = config["devtest"]["seltype_list"]
    responses_df["cellnumber"] = random.choice(cellno_list, size=col_size)
    return responses_df


def update_ref_list(full_df: pd.DataFrame, ref_list_df: pd.DataFrame) -> pd.DataFrame:
    """
    Update long form references that should be on the reference list.

    For the first year (processing 2022 data) only, several references
    should have been designated on the "reference list", ie, should have been
    assigned cellnumber = 817, but were wrongly assigned a different cellnumber.

    Args:
        full_df (pd.DataFrame): The full_responses dataframe
        ref_list_df (pd.DataFrame): The mapper containing updates for the cellnumber
    Returns:
        df (pd.DataFrame): with cellnumber and selectiontype cols updated.
    """
    ref_list_filtered = ref_list_df.loc[
        (ref_list_df.formtype == 1) & (ref_list_df.cellnumber != 817)
    ]
    df = pd.merge(
        full_df,
        ref_list_filtered[["reference", "cellnumber"]],
        how="outer",
        on="reference",
        suffixes=("", "_new"),
        indicator=True,
    )
    # check no items in the reference list mapper are missing from the full responses
    missing_refs = df.loc[df["_merge"] == "right_only"]
    if not missing_refs.empty:
        msg = (
            "The following references in the reference list mapper are not in the data:"
        )
        raise ValueError(msg + str(missing_refs.reference.unique()))

    # update cellnumber and selectiontype where there is a match
    match_cond = df["_merge"] == "both"
    df = df.copy()
    df.loc[match_cond, "cellnumber"] = 817
    df.loc[match_cond, "selectiontype"] = "L"

    df = df.drop(["_merge", "cellnumber_new"], axis=1)

    return df
