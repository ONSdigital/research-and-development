def get_amendments(main_snapshot, secondary_snapshot):
    """Get amended records from secondary snapshot.

    Get all records that are present in both the main snapshot and the updated
    snapshot, and have matching keys.
    """
    key_cols = ["reference", "year", "instance"]
    numeric_cols = [
        "219",
        "220",
        "242",
        "243",
        "244",
        "245",
        "246",
        "247",
        "248",
        "249",
        "250",
    ]
    numeric_cols_new = [f"{i}_updated" for i in numeric_cols]
    numeric_cols_diff = [f"{i}_diff" for i in numeric_cols]

    # Inner join on keys to select only records present in both snapshots
    amendments_df = pd.merge(
        main_snapshot,
        secondary_snapshot,
        on=key_cols,
        how="inner",
        suffixes=("_original", "_updated"),
    )

    # If there are any records to amend, calculate differences
    if amendments_df.shape[0] > 0:

        for each in numeric_cols:
            amendments_df[f"{each}_diff"] = (
                amendments_df[f"{each}_updated"] - amendments_df[f"{each}_original"]
            )
            amendments_df.loc[
                amendments_df[f"{each}_diff"] > 0.00001, f"is_{each}_diff_nonzero"
            ] = True

        # ? I think this is the way to do it:
        # ?     Take a slice of the df which is just the cols ending with _diff_nonzero
        # ?     Do a column-wise any() on this slice, which returns a series where the
        #       value is True if any of the *_diff_nonzero cols in that row were True
        # ?     Add that series as a column to the original df
        # ?     Remove any rows from the df where is_any_diff_nonzero is False
        # ! Can't test this without a real secondary snapshot file
        amendments_df["is_any_diff_nonzero"] = amendments_df[
            amendments_df.columns[amendments_df.columns.str.endswith("_diff_nonzero")]
        ].any(axis="columns")
        amendments_df = amendments_df.loc[amendments_df["is_any_diff_nonzero"]]

        # Select only the keys, updated value, difference, and postcode
        # TODO Would be easier for users if the numberic cols alternated
        select_cols = [
            "reference",
            "year",
            "instance",
            *numeric_cols_new,
            *numeric_cols_diff,
            "postcodes_harmonised",
        ]
        amendments_df = amendments_df[select_cols]

        # Add markers
        amendments_df["is_constructed"] = True
        amendments_df["accept_changes"] = False

        return amendments_df
    else:
        freezing_logger.info("No amendments found.")
        return None


def get_additions(main_snapshot, secondary_snapshot):
    """Get added records from secondary snapshot.

    Get all records that are present in the updated snapshot but not the main snapshot
    """
    key_cols = ["reference", "year", "instance"]

    # To do a right anti-join, we need to do a full outer join first, then
    # take only rows that were marked as right-only by the indicator. After
    # that, there will be copies of every column in both, but for the
    # right-only rows the columns from the left df will be null, so they're
    # all dropped afterwards.
    outer_join = pd.merge(
        main_snapshot,
        secondary_snapshot,
        on=key_cols,
        how="outer",
        suffixes=("_old", ""),
        indicator=True,
    )
    additions_df = outer_join[(outer_join._merge == "right_only")].drop(
        "_merge", axis=1
    )
    additions_df = additions_df[
        additions_df.columns[~additions_df.columns.str.endswith("_old")]
    ]

    if additions_df.shape[0] > 0:
        additions_df["is_constructed"] = True
        additions_df["accept_changes"] = False
        return additions_df
    else:
        freezing_logger.info("No additions found.")
        return None


def output_freezing_files(amendments_df, additions_df, config, write_csv, run_id):
    """Save CSVs of amendments and additions for user approval."""
    # Prepare output paths
    network_or_hdfs = config["global"]["network_or_hdfs"]
    paths = config[f"{network_or_hdfs}_paths"]
    tdate = datetime.now().strftime("%y-%m-%d")
    survey_year = config["years"]["survey_year"]
    freezing_folder = paths["freezing_path"]
    amendments_filename = os.path.join(
        freezing_folder,
        "auto_freezing",
        f"{survey_year}_freezing_amendments_{tdate}_v{run_id}.csv",
    )
    additions_filename = os.path.join(
        freezing_folder,
        "auto_freezing",
        f"{survey_year}_freezing_additions_{tdate}_v{run_id}.csv",
    )

    # Check if the dataframes are empty before writing
    if amendments_df is not None:
        write_csv(f"{amendments_filename}", amendments_df)
    if additions_df is not None:
        write_csv(f"{additions_filename}", additions_df)

    return True
