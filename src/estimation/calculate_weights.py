import pandas as pd
import logging

CalcWeights_Logger = logging.getLogger(__name__)


def create_weights_filter(df: pd.DataFrame) -> pd.Series:
    """Return a boolean mask for the rows that the weights should be applied to.

    Args:
        df (pd.DataFrame): The input dataframe which contains survey data.

    Returns:
        pd.Series: A boolean mask for the conditions needed to calculate weights.
    """
    prn_only_mask = df.selectiontype == "P"
    shortform_only_mask = df.formtype == "0006"
    weights_filter = prn_only_mask & shortform_only_mask
    return weights_filter


def create_estimation_filter(df: pd.DataFrame) -> pd.Series:
    """Return a boolean mask for the conditions needed to calculate estimation weights.

    Args:
        df (pd.DataFrame): The input dataframe which contains survey data.

    Returns:
        pd.Series: A boolean mask for the conditions needed to calculate weights.
    """
    estimation_filter = create_weights_filter(df)
    status_cond = df["status"].isin(["Clear", "Clear - overridden"])
    instance_cond = df["instance"] == 0
    valid_cond = df["709"].notnull()

    estimation_filter = estimation_filter & status_cond & valid_cond & instance_cond
    return estimation_filter


def calc_lower_n(df: pd.DataFrame) -> int:
    """Calculates 'n' which is a number of
    unique RU references in the filtered dataset.

    Args:
        df (pd.DataFrame): The input dataframe which contains survey data,
            including expenditure data
    Returns:
        int: The number of unique references.
    """
    # Count the records
    n = df["reference"].nunique()

    return n


def calc_lower_e(df: pd.DataFrame) -> int:
    """Calculates 'e' which is a sum of
    IDBR employment data in the filtered dataset.

    Args:
        df (pd.DatatFrame): The input dataframe which contains survey data,
            including IDBR employment data.
    Returns:
        int: The sum of IDBR employment of sampled.
    """
    # Sum employment for each cellnumber
    e = df["employment"].sum()

    return e


def calc_lower_s(df: pd.DataFrame) -> int:
    """Calculates 's' which identifies the sum of outliers for a cell group.

    Args:
        df (pd.DataFrame): The input dataframe which contains survey data.

    Returns:
        int: Calculated value of s.
    """
    # Filter where outliers bool = true
    df = df.loc[df.outlier]

    # If there are no outliers, return 0
    if df.empty:
        s = 0
    else:
        # Sum the employment column
        s = df["employment"].sum()

    return s


def calculate_weighting_factors(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Calculate the weighting factor 'a' for each cell in the survery data

    Note: A 'cell' is a group of businesses.

    Args:
        df (pd.DataFrame): The input df containing survey data
        cellno_dict (dict): dictionary of cellnumbers and UNI_counts
        exp_col (str, optional): The column that is used to calculate n.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]:
        1) Returns the full dataframe with the added
        new column "a_weight".
        2) Returns a QA dataframe of all variables used in the calculation
    """
    cols = set(df.columns)
    if not ("outlier" in cols):
        raise ValueError("The column essential 'outlier' is missing.")
    df["outlier"] = df["outlier"].fillna(False)

    # Convert 709 column to numeric
    df["709"] = pd.to_numeric(df["709"], errors="coerce")

    # Default a and g-weights to 1 for all entries
    df["a_weight"] = 1.0
    df["g_weight"] = 1.0

    df = df.groupby("cellnumber", group_keys=False).apply(calc_a_weight)
    df = df.groupby("cellnumber", group_keys=False).apply(calc_g_weight)

    # Create a QA dataframe
    qa_frame = create_weights_qa_df(df)
    df = df.drop(columns=["N", "n", "o", "E", "e", "s"])

    # Apply the outlier weights
    df = outlier_weights(df)

    return df, qa_frame


def calc_a_weight(cell_group: pd.DataFrame) -> pd.DataFrame:
    """Calculate the 'a' weighting factor for a cell group.

    The calculation here is:

    a = (N-o) / (n-o)

    Where:
        - N is the total number of businesses in the cell
        - n is the number of businesses in sample for that cell
        - o is the number of outliers in the cell

    'o' is calculated in this function by summing all the `True` values
        because `True` == 1

    Args:
        cell_group (pd.DataFrame): The dataframe grouped by cellnumber.

    Returns:
        pd.DataFrame: The dataframe with the 'a' weighting factor calculated.
    """
    if cell_group.empty:
        return cell_group

    N = cell_group["uni_count"].iloc[0]

    estimation_filter = create_estimation_filter(cell_group)
    filtered_group = cell_group.copy().loc[estimation_filter]

    n = calc_lower_n(filtered_group)

    # Count the outliers for this group (will count all the `True` values)
    outlier_count = filtered_group["outlier"].sum()

    # Calculate 'a' for this group
    if n > 0:
        a_weight = (N - outlier_count) / (n - outlier_count)
    else:
        a_weight = 1.0

    cell_group["N"] = N
    cell_group["n"] = n
    cell_group["o"] = outlier_count

    weights_filter = create_weights_filter(cell_group)
    cell_group.loc[weights_filter, "a_weight"] = a_weight

    return cell_group


def calc_g_weight(cell_group: pd.DataFrame) -> pd.DataFrame:
    """Calculate the 'g' weighting factor for a cell group.

    The calculation for the g-weight is:

    g = (E - s) / a * (e - s)

    Where:
        - E is the sum of IDBR employment for all businesses in a cell
        - e is the sum of IDBR employment for all sampled, valid responses in the cell
        - s is the sum of IDBR employment for all outliered sampled, valid responses
        - a is the 'a' weighting factor for the cell

    Args:
        cell_group (pd.DataFrame): The dataframe grouped by cellnumber.

    Returns:
        pd.DataFrame: The dataframe with the 'a' weighting factor calculated.
    """
    if cell_group.empty:
        return cell_group

    estimation_filter = create_estimation_filter(cell_group)
    filtered_group = cell_group.copy().loc[estimation_filter]

    if filtered_group.empty:
        return cell_group

    E = filtered_group["uni_employment"].iloc[0]
    a = filtered_group["a_weight"].iloc[0]

    e = calc_lower_e(filtered_group)
    s = calc_lower_s(filtered_group)

    # Calculate 'g' for this group
    if (e - s) > 0:
        g_weight = (E - s) / (a * (e - s))
    else:
        g_weight = 1.0

    cell_group["E"] = E
    cell_group["e"] = e
    cell_group["s"] = s

    weights_filter = create_weights_filter(cell_group)
    cell_group.loc[weights_filter, "g_weight"] = g_weight

    return cell_group


def create_weights_qa_df(df: pd.DataFrame) -> pd.DataFrame:
    """Create a QA dataframe for the weight calculation.

    Args:
        df (pd.DataFrame): The dataframe containing the weights columns.

    Returns:
        pd.DataFrame: The QA dataframe.
    """
    est_filter = create_estimation_filter(df)

    qa_cols_list = ["cellnumber", "N", "n", "o", "E", "e", "s", "a_weight", "g_weight"]
    qa_frame = df[qa_cols_list].loc[est_filter].groupby("cellnumber").first()
    qa_frame = qa_frame.reset_index()
    qa_frame = qa_frame.rename(
        columns={
            "cellnumber": "Cell Number",
            "N": "N - uni_count",
            "n": "n - num clear records in cell",
            "o": "o - num outliers in cell",
            "E": "E - uni_employment",
            "e": "e - sum of employment in cell",
            "s": "s - sum of employment outliers in cell",
        }
    )

    return qa_frame


def outlier_weights(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate weights for outliers.

    If a reference has been flagged as an outlier,
    the 'a weight' value is set to 1.0

    Args:
        df (pd.DataFrame): The dataframe weights are calculated for.

    Returns:
        pd.DataFrame: The dataframe with the a_weights set to 1.0 for outliers.
    """
    df.loc[df["outlier"], "a_weight"] = 1.0
    df.loc[df["outlier"], "g_weight"] = 1.0
    return df
