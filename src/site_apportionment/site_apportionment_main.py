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
    output_file=False,
) -> pd.DataFrame:
    """Run the apportionment to sites module.

    For short forms, the percentage is set to 100% on all instances.
    For long forms, the percentage is used to apportion the variables from
    instance 0 to all other instances.
    Same percentages are used for each product group.

    When running on the local network,

    Args:
        config (dict): The pipeline configuration
        df (pd.DataFrame): Main dataset before the outputs
    Returns:
        df_out (pd.DataFrame): Percentages filled in for short forms and applied
        to apportion  for long forms
    """
    # Create apport_path variable for output of QA apportionment file
    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    imp_path = config[f"{NETWORK_OR_HDFS}_paths"]["apportionment_path"]

    imp_markers_to_keep: list = ["R", "TMI", "CF", "MoR", "constructed"]

    # Conditionally output the records to be removed
    if config["global"]["output_status_filtered"]:
        osf.output_status_filtered(df, imp_markers_to_keep, config, write_csv, run_id)

    # Check if this module needs to be applied
    if config["global"]["apportion_sites"]:
        SitesMainLogger.info("Starting apportionment to sites...")
        df_out = sap.run_apportion_sites(df, imp_markers_to_keep, config)

        # Output QA files
        if config["global"]["output_apportionment_qa"] & output_file:
            SitesMainLogger.info("Outputting Apportionment files.")
            tdate = datetime.now().strftime("%Y-%m-%d")
            filename = f"estimated_df_apportioned_{tdate}_v{run_id}.csv"
            write_csv(f"{imp_path}/apportionment_qa/{filename}", df_out)

        SitesMainLogger.info("Finished apportionment to sites.")
        return df_out

    else:
        SitesMainLogger.info("Apportionment to sites disabled, skipped")
        # Remove records that are neither clear nor imputed, based on imputation marker.
        filtered_df = sap.keep_good_markers(df)
        return filtered_df
