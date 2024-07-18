"""The main file for the BERD total FTE output."""
import logging
import pandas as pd
from datetime import datetime
from typing import Callable, Dict, Any


OutputMainLogger = logging.getLogger(__name__)


def qa_output_total_fte(
    df: pd.DataFrame, config: Dict[str, Any], write_csv: Callable, run_id: int
):
    """Run the outputs module.

    Args:
        df (pd.DataFrame): The main dataset with weights not applied
        config (dict): The configuration settings.
        write_csv (Callable): Function to write to a csv file.
         This will be the hdfs or network version depending on settings.
        run_id (int): The current run id

    """
    output_path = config["outputs_paths"]["outputs_master"]

    totals_names = ["emp_total", "emp_researcher", "emp_technician", "emp_other"]
    totals_values = [
        df["emp_total"].sum(),
        df["emp_researcher"].sum(),
        df["emp_technician"].sum(),
        df["emp_other"].sum(),
    ]
    qa_total_fte_df = pd.DataFrame(
        list(zip(totals_names, totals_values)), columns=["Column", "Total"]
    )

    # Outputting the CSV file with timestamp and run_id
    tdate = datetime.now().strftime("%y-%m-%d")
    survey_year = config["years"]["survey_year"]
    filename = f"{survey_year}_total_fte_qa_{tdate}_v{run_id}.csv"
    write_csv(f"{output_path}/output_fte_total_qa/{filename}", qa_total_fte_df)
