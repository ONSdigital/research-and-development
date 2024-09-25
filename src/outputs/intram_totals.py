"""The main file for the frozen group output."""

import logging
from typing import Callable, Dict, Any

import pandas as pd
from datetime import datetime

OutputMainLogger = logging.getLogger(__name__)


def output_intram_totals(
    intram_tot_dict: Dict[str, int],
    config: Dict[str, Any],
    write_csv: Callable,
    run_id: int,
) -> None:
    """Output the intramural totals.

    Args:
        intram_tot_dict (dict): Dictionary with the intramural totals.
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
            This will be the hdfs or network version depending on settings.
        run_id (int): The current run id

    Returns:
        None
    """

    output_path = config["outputs_paths"]["outputs_master"]
    OutputMainLogger.info("Starting Intramural totals output...")

    # create a dataframe from the intramural totals dictionary
    intram_tot_df = pd.DataFrame(intram_tot_dict.items(), columns=["output", "total"])

    # format the dataframe so values are integers with commas for thousands
    intram_tot_df["total"] = intram_tot_df["total"].map("{:,}".format)

    # output the dictionary to the logger:
    OutputMainLogger.info("Intramural totals:")
    OutputMainLogger.info(intram_tot_df)

    tdate = datetime.now().strftime("%y-%m-%d")
    survey_year = config["years"]["survey_year"]
    filename = f"{survey_year}_intram_totals_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_intram_totals/{filename}", intram_tot_df)
