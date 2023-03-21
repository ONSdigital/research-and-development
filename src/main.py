"""The main pipeline"""


def run_pipeline(start):
    """The main pipeline.

    Args:
        start (float): The time when the pipeline is launched
        generated from the time module using time.time()
    """

    conf_obj = Config_settings()
    config = conf_obj.config_dict
    global_config = config["global"]

    runlog_obj = runlog.RunLog(config, version)

    logger = logger_creator(global_config)
    logger.info("Launching Pipeline .......................")
    # Pipeline functions are located here:
    df = create_dummy_df()
    df = manipulate_df(df)
    print(df)
    logger.info("Finshing Pipeline .......................")
    # Runlog metadata is recorded and saved here:
    runlog_obj.retrieve_pipeline_logs()

    run_time = round(time.time() - start, 5)
    runlog_obj._record_time_taken(run_time)

    runlog_obj._create_runlog_dicts()
    runlog_obj._create_runlog_dfs()
    runlog_obj.create_runlog_files()
    runlog_obj._write_runlog()
