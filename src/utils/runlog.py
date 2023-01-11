"""Generate run log output."""

import logging
import os

import vat_returns.utils.helpers as hlp
from vat_returns._version import __version__ as version

LOGGER = logging.getLogger(__name__)


class RunLog:
    """Read configuration settings and setup runlog table.

    Parameters
    ----------
    spark : SparkSession
        The spark session to use.
    config : Dict
        Dict containing settings for all pipelines.

    """

    def __init__(self, spark, timestamp, run_id, config):
        """Initialise, read and validate config settings."""
        self.spark = spark
        self.timestamp = timestamp
        self.run_id = run_id
        self.config = config

    def name_config_output(self, name):
        """Output all config settings for 'name' as a string."""
        conf_settings = self.config.config_setup(name)
        settings_str = ""
        for key, item in conf_settings.items():
            settings_str += (f"  {key}: {item}")

        return settings_str

    def log_output(self, module, dependant_id):
        """Create single row dataframe entry for the runlog.

        The dataframe columns for each of the config sections
        will contain a string as a config summary
        """
        gcf = self.config.config_setup("global")

        cols = ["run_id", 
                "dependant_id",
                "module", 
                "user",
                "version",
                "datetime",
                "settings"]

        output_list = [self.run_id,
                       dependant_id,
                       module,
                       os.environ["HADOOP_USER_NAME"],
                       version,
                       self.timestamp,
                       self.name_config_output(module)]

        runlog = [(* output_list,)]
        db = gcf["database"]
        runlog_df = self.spark.createDataFrame(runlog, cols)
        hlp.append_to_hive(self.spark, runlog_df, db, "vat_runlog")
