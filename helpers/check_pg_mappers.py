import pandas as pd

# Read in mappers from local drive
pg_num_alpha = pd.read_csv(
    "R:/BERD Results System Development 2023/DAP_emulation/mappers/pg_num_alpha.csv"
)
sic_pg_alpha = pd.read_csv(
    "R:/BERD Results System Development 2023/DAP_emulation/mappers/sic_pg_alpha.csv"
)
sic_pg_utf = pd.read_csv(
    "R:/BERD Results System Development 2023/DAP_emulation/mappers/SIC_to_PG_UTF-8.csv"
)


def check_pg_mapper(
    pg_num_alpha,
    sic_pg_alpha,
    sic_pg_utf,
):

    # Change column headings to keep column source clear
    pg_num_alpha = pg_num_alpha.rename(
        columns={"pg_numeric": "pg_num_alpha_NUM", "pg_alpha": "pg_num_alpha_ALPHA"}
    )

    sic_pg_alpha = sic_pg_alpha.rename(
        columns={"sic": "sic_pg_alpha_SIC", "pg_alpha": "sic_pg_alpha_ALPHA"}
    )

    cols_needed = ["SIC 2007_CODE", "2016 > Form PG", "2016 > Pub PG"]
    sic_pg_num = sic_pg_utf[cols_needed]

    sic_pg_num = sic_pg_num.rename(
        columns={
            "SIC 2007_CODE": "sic_pg_num_SIC",
            "2016 > Form PG": "sic_pg_num_NUM",
            "2016 > Pub PG": "sic_pg_num_ALPHA",
        }
    )

    # Merge mappers to check for nulls/inconsistencies
    mapper_check = pd.merge(
        pg_num_alpha,
        pd.merge(
            sic_pg_alpha,
            sic_pg_num,
            left_on="sic_pg_alpha_SIC",
            right_on="sic_pg_num_SIC",
            how="outer",
        ),
        left_on="pg_num_alpha_NUM",
        right_on="sic_pg_num_NUM",
        how="outer",
    )

    # Check for nulls
    nulls_to_check = mapper_check[mapper_check.isnull().any(axis=1)]
    if nulls_to_check.empty:
        print("\nNo nulls in checks")
    else:
        print("\nNulls to check:")
        print(nulls_to_check)

    # Check for inconsistencies
    mismatches = mapper_check[
        (mapper_check["pg_num_alpha_NUM"] != mapper_check["sic_pg_num_NUM"])
        | (mapper_check["sic_pg_alpha_SIC"] != mapper_check["sic_pg_num_SIC"])
        | (mapper_check["pg_num_alpha_ALPHA"] != mapper_check["sic_pg_alpha_ALPHA"])
        | (mapper_check["sic_pg_alpha_ALPHA"] != mapper_check["sic_pg_num_ALPHA"])
    ]

    if mismatches.empty:
        print("\nNo mismatches in checks")
    else:
        print("\nMismatches to check:")
        print(mismatches)


check_pg_mapper(pg_num_alpha, sic_pg_alpha, sic_pg_utf)
