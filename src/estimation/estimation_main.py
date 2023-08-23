"""Main file for the estimation module."""
from src.estimation import cellno_mapper as cmap
import src.estimation.calculate_weights as calcw


def run_estimation(df, cellno_df):
    cell_unit_dict = cmap.cellno_unit_dict(cellno_df)

    # Check overall outliers - create "is_outlier" col
    df = calcw.check_outliers(df)

    # Calculate weighting factor
    df, qa_df = calcw.calculate_weighting_factor(df, cell_unit_dict)

    print(df.sample(20))
    print(qa_df.sample(20))
