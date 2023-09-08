import pandas as pd  
import random

#import string # To create an alphabet 

import math
def _normal_round(n):
    f = math.floor(n)
    if n - f < 0.5:
        return f
    else:
        return f + 1


# Filtering function
def filter_valid(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    return df.loc[df[value_col] > 0].copy()

# Arguments 

N=10

upper_clip = 0.05

lower_clip = 0.00

value_col = 'value'
group_cols = ['cell_no', 'year']
ruref_col = "ref"
 

# Prepare sample data 

mydata = {'ref': ['A' + str(i) for i in range(N)] * 1, 
          'value': [x for x in range(1*N)], #[x**2 for x in range(2*N)], 
          'year': [2022 for i in range(N)], # + [2023 for i in range(N)], 
          'cell_no': [100 for i in range(1*N)]} 

df = pd.DataFrame(mydata) 

#%% Filter valid
filtered_df = filter_valid(df, value_col)


#%% Add group count
filtered_df['group_count'] = filtered_df.groupby(group_cols)[value_col].transform('count')

#%% Compute rank margins
filtered_df['high'] = filtered_df['group_count'] * upper_clip
filtered_df['high_rounded']= filtered_df.apply(lambda row: _normal_round(row['high']), axis=1)
filtered_df['upper_band'] = filtered_df['group_count'] - filtered_df['high_rounded']

filtered_df['low']=filtered_df['group_count'] * lower_clip
filtered_df['lower_band']= filtered_df.apply(lambda row: _normal_round(row['low']), axis=1)



#%% Compute row numbers per group
#sort_cols = group_cols + [value_col]
#sort_dirs = [True for x in sort_cols]
filtered_df["group_rank"] = (filtered_df
  .groupby(group_cols)[value_col]
  .rank(method="first", ascending=True))

#%% Apply outliers
outlier_cond = (
        (filtered_df["group_rank"] > filtered_df.upper_band) | 
        (filtered_df["group_rank"] <= filtered_df.lower_band))  # noqa

    # create boolean auto_outlier col based on conditional logic
    #filter_cond = (df.selectiontype == "P") & (df[value_col] > 0)
    #status_cond = df.statusencoded.isin(["210", "211"])
filtered_df[f"{value_col}_outlier_flag"] = outlier_cond

filtered_df = filtered_df.drop([
        "group_count", 
        "high",
        "high_rounded",
        "upper_band",
        "low",
        "lower_band",
        "group_rank"
        ], axis=1)

#%% Select and join back to main df
cols_sel = group_cols + [ruref_col, f"{value_col}_outlier_flag"]
filtered_df = filtered_df[cols_sel]

#%% merge back to the original df
df = df.merge(
        filtered_df,
        how='left',
        on=group_cols + [ruref_col])
df[f"{value_col}_outlier_flag"] = df[f"{value_col}_outlier_flag"].fillna(False)
