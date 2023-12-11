"""The main file for the Apportionment to sites module."""
import logging
import pandas as pd
import src.site_apportionment.site_apportionment as sap

SitesMainLogger = logging.getLogger(__name__)


def run_site_apportionment(config: dict, df: pd.DataFrame) -> pd.DataFrame:
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

    # Check if this module needs to be applied
    if config["global"]["apportion_sites"]:
        SitesMainLogger.info("Starting apportionment to sites...")
        return sap.apportion_sites(df)
    else:
        SitesMainLogger.info("PApportionment to sites disabled, skipp")
        return df
