"""The main file for the Intram by PG output."""
import pandas as pd

from src.outputs.outputs_helpers import aggregate_output

#%% Inoputs
df=pd.read_csv(r"D:\data\res_dev\estimation\estimated_df.csv")

postcode_itl_mapper = pd.read_csv(r"R:/BERD Results System Development 2023/DAP_emulation/ONS_Postcode_Reference/postcodes_pcd2_itl.csv")

itl_mapper = pd.read_csv(r"R:/BERD Results System Development 2023/DAP_emulation/mappers/2023/itl.csv")

itl1_detailed  = pd.read_csv(r"R:\BERD Results System Development 2023\DAP_emulation\mappers\itl1_detailed.csv")




#%% Running

# Group by PG and aggregate intram
key_col = "201"
value_col = "211"
agg_method = "sum"

df_agg = aggregate_output(df, [key_col], [value_col], agg_method)

# Create Total and concatinate it to df_agg
value_tot = df_agg[value_col].sum()
df_tot = pd.DataFrame({key_col: ["total"], value_col: value_tot})
df_agg = pd.concat([df_agg, df_tot])

# Merge with labels and ranks
df_merge = pg_detailed.merge(
    df_agg,
    how="left",
    left_on="pg_alpha",
    right_on=key_col)
df_merge[value_col] = df_merge[value_col].fillna(0)

# Sort by rank
df_merge.sort_values("ranking", axis=0, ascending=True)

# Select and rename the correct columns
detail = "Detailed product groups (Alphabetical product groups A-AH)"
notes = "Notes"
value_title = "2023 (Current period)"
df_merge = df_merge[[detail, value_col, notes]].rename(
    columns={value_col: value_title})

# Outputting the CSV file with timestamp and run_id
tdate = datetime.now().strftime("%Y-%m-%d")
filename = f"output_intram_by_pg_{tdate}_v{run_id}.csv"
write_csv(f"{output_path}/output_intram_by_pg/{filename}", df_merge)
