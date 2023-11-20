"""The main file for the Outputs module."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any


OutputMainLogger = logging.getLogger(__name__)

def output_status_filtered(
    df: pd.DataFrame,
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
):
    """Run the outputs module.

    Args:
        df (pd.DataFrame): Dataframe containing records filtered out from outputs.
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id


    """

    NETWORK_OR_HDFS = config["global"]["network_or_hdfs"]
    paths = config[f"{NETWORK_OR_HDFS}_paths"]
    output_path = paths["output_path"]

    tdate = datetime.now().strftime("%Y-%m-%d")
    filename = f"status_filtered_qa_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/status_filtered_qa/{filename}", df)
