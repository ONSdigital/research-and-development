def apply_construction(main_df, config, check_file_exists, read_csv, write_csv, run_id):
    """Read user-edited construction files and apply them to the main snapshot."""
    # Prepare filepaths to read from
    network_or_hdfs = config["global"]["network_or_hdfs"]
    paths = config[f"{network_or_hdfs}_paths"]
    amendments_filepath = paths["construction_amend_path"]
    additions_filepath = paths["construction_add_path"]

    # Check if the construction files exist
    amendments_exist = check_file_exists(amendments_filepath)
    additions_exist = check_file_exists(additions_filepath)

    # If each file exists, read it and call the function to apply them
    if (not amendments_exist) and (not additions_exist):
        construction_logger.info("No amendments or additions to apply, skipping...")
        return main_df

    if amendments_exist:
        try:
            amendments_df = read_csv(amendments_filepath)
            constructed_df = apply_amendments(main_df, amendments_df)
        except pd.errors.EmptyDataError:
            construction_logger.warning(
                f"Amendments file {amendments_filepath} is empty, skipping..."
            )

    if additions_exist:
        try:
            additions_df = read_csv(additions_filepath)
            additions_df["instance"] = additions_df["instance"].astype("Int64")
            constructed_df = apply_additions(main_df, additions_df)
        except pd.errors.EmptyDataError:
            construction_logger.warning(
                f"Additions file {additions_filepath} is empty, skipping..."
            )

    # Save the constructed dataframe as a CSV
    tdate = datetime.now().strftime("%y-%m-%d")
    survey_year = config["years"]["survey_year"]
    construction_output_filepath = os.path.join(
        paths["root"],
        "construction",
        f"{survey_year}_constructed_snapshot_{tdate}_v{run_id}.csv",
    )
    write_csv(construction_output_filepath, constructed_df)
    return constructed_df


def apply_amendments(main_df, amendments_df):
    """Apply amendments to the main snapshot."""
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

    accepted_amendments_df = amendments_df.drop(
        amendments_df[~amendments_df.accept_changes].index
    )

    if accepted_amendments_df.shape[0] == 0:
        construction_logger.info(
            "Amendments file contained no records marked for inclusion"
        )
        return main_df

    # Drop the diff columns
    accepted_amendments_df = accepted_amendments_df.drop(
        columns=[col for col in accepted_amendments_df.columns if col.endswith("_diff")]
    )

    # Join the amendments onto the main snapshot
    amended_df = pd.merge(main_df, accepted_amendments_df, how="left", on=key_cols)

    # Drop the old numeric cols and rename the amended cols
    amended_df = amended_df.drop(columns=numeric_cols)
    cols_to_rename = dict(zip(numeric_cols_new, numeric_cols))
    amended_df = amended_df.rename(columns=cols_to_rename, errors="raise")

    construction_logger.info(
        f"{accepted_amendments_df.shape[0]} records amended during construction"
    )

    return amended_df


def apply_additions(main_df, additions_df):
    """Apply additions to the main snapshot."""
    # Drop records where accept_changes is False and if any remain, add them to main df
    accepted_additions_df = additions_df.drop(
        additions_df[~additions_df["accept_changes"]].index
    )
    if accepted_additions_df.shape[0] > 0:
        added_df = pd.concat([main_df, accepted_additions_df], ignore_index=True)
        construction_logger.info(
            f"{accepted_additions_df.shape[0]} records added during construction"
        )
    else:
        construction_logger.info(
            "Additions file contained no records marked for inclusion"
        )
        return main_df

    return added_df
