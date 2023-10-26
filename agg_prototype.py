# -*- coding: utf-8 -*-
"""
Created on Wed Oct 25 16:56:55 2023

@author: zoring
"""
import pandas as pd
def aggregate_output(
        df: pd.DataFrame,
        key_cols: list,
        value_cols: list,
        agg_method: str = "sum",) -> pd.DataFrame:

    """Groups the datadrame by key columns and aggregates the value columns
    using a specified aggregation method.

    Args:
        df (pd.DataFrame): Dataframe containing all columns
        key_cols (list): List of key column names
        value_cols (list): List of value column names


    Returns:
        df_agg (pd.DataFrame): A dataframe containing key columns and aggregated values
    """

    # Check what columns are available
    available_cols = df.columns.tolist()
    my_keys = [c for c in key_cols if c in available_cols]
    my_values = [c for c in value_cols if c in available_cols]

    # Dictionary for aggregation
    agg_dict = {x: agg_method for x in my_values}

    # Groupby and aggregate
    df_agg = df.groupby(my_keys).agg(agg_dict).reset_index()

    return df_agg
#%% 
# For debugging - begin
mypath = r"D:\data\res_dev\estimation\estimated_df_short.csv"
df = pd.read_csv(mypath)
# For debugging - end
#%% Group by PG and aggregate intram
key_col = "201"
value_col = "211"
agg_method = "sum"

df_agg = aggregate_output(df, [key_col], [value_col], agg_method)

#%% Create Total and concatinate it to df_agg
value_tot = df_agg[value_col].sum()
df_tot = pd.DataFrame({key_col: ["total"], value_col: value_tot})
df_agg = pd.concat([df_agg, df_tot])

# Merge with labels and ranks
# For debugging - begin
mypath = r"R:\BERD Results System Development 2023\DAP_emulation\mappers\pg_detailed.csv"
pg_detailed = pd.read_csv(mypath)
#%% For debugging - end
df_merge = pg_detailed.merge(df_agg, how="left", left_on="pg_alpha", right_on=key_col)
df_merge[value_col] = df_merge[value_col].fillna(0)


# Sort by rank
df_merge.sort_values("rank", axis=0, ascending=True)

# Select and rename the correct coluns
detail = "Detailed product groups (Alphabetical product groups A-AH)"
notes = "Notes"
value_title = "2023 (Current period)"
df_merge = df_merge[[detail, value_col, notes]].rename(columns={value_col: value_title})




# Outputting the CSV file with timestamp and run_id
#tdate = datetime.now().strftime("%Y-%m-%d")
#filename = f"output_intram_by_pg_{tdate}_v{run_id}.csv"
#write_csv(f"{output_path}/output_intram_by_pg/{filename}", df_agg)
