"""The main file for the Apportionment to sites module."""
import logging
import pandas as pd
from typing import Callable, Dict, Any
from datetime import datetime

import src.site_apportionment.site_apportionment as sap
import src.site_apportionment.output_status_filtered as osf

SitesMainLogger = logging.getLogger(__name__)


def run_site_apportionment(
    df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
    output_type: str,
) -> pd.DataFrame:
    """Run the apportionment to sites module.

    For short forms, the percentage is set to 100% on all instances.
    For long forms, the percentage is used to apportion the variables from
    instance 0 to all other instances.
    Same percentages are used for each product group.

    Args:
        config (dict): The pipeline configuration
        df (pd.DataFrame): Main dataset before the outputs
        write_csv (Callable): Function to write to a csv file.
            This will be the hdfs or network version depending on settings.
        run_id (int): The current run id
        output_type (str): The type of output being processed, either "estimated_df"
            or "weighted_df". Needed for the QA file naming.
    Returns:
        df_out (pd.DataFrame): Percentages filled in for short forms and applied
            to apportion  for long forms
    """
    # Create variable for output of QA apportionment file
    qa_path = config["apportionment_paths"]["qa_path"]

    imp_markers_to_keep: list = ["R", "TMI", "CF", "MoR", "constructed"]

    # Conditionally output the records to be removed
    if config["global"]["output_status_filtered"]:
        osf.output_status_filtered(df, imp_markers_to_keep, config, write_csv, run_id)

    SitesMainLogger.info("Starting apportionment to sites...")
    df_out = sap.run_apportion_sites(df, imp_markers_to_keep, config)

    # Output QA files
    if config["global"]["output_apportionment_qa"]:
        SitesMainLogger.info("Outputting Apportionment files.")
        tdate = datetime.now().strftime("%y-%m-%d")
        survey_year = config["years"]["survey_year"]
        filename = f"{survey_year}_{output_type}_apportioned_{tdate}_v{run_id}.csv"
        write_csv(f"{qa_path}/{filename}", df_out)

    SitesMainLogger.info("Finished apportionment to sites.")
    return df_out
