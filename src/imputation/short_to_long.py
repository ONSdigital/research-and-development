import pandas as pd

from src.outputs.short_form_out import create_headcount_cols


def run_short_to_long(df, fte_civil="706", fte_defence="707", hc_total="705"):

    df = create_headcount_cols(df, fte_civil, fte_defence, hc_total)

    convert_short_to_long = [
        ("701", "702", "211"),
        ("703", "704", "305"),
        ("706", "707", "Employment total"),
        ("headcount_civil", "headcount_defence", "headcount_total"),
    ]

    civil_df = df.loc[
        df["formtype"] == "0006",
    ].copy()
    defence_df = civil_df.copy()

    civil_df.loc[:, "200"] = "C"
    civil_df.loc[:, "instance"] = 1

    defence_df.loc[:, "200"] = "D"
    defence_df.loc[:, "instance"] = 2

    for each in convert_short_to_long:
        civil_df[each[2]] = civil_df[each[0]]
        defence_df[each[2]] = defence_df[each[1]]

    df = pd.concat([df, civil_df, defence_df], ignore_index=True)

    df = df.drop(["headcount_civil", "headcount_defence"], axis=1)

    return df
