"""Main file for the estimation module."""
from src.estimation import cellno_mapper as cmap


def run_estimation(df, cellno_df):
    cell_unit_dict = cmap.cellno_unit_dict(cellno_df)
