"""Functions relating to loading historic data"""
from typing import Generator, List
import logging
import re
import os

history_loader_logger = logging.getLogger(__name__)


def history_years(current: int, back_history: int) -> Generator[int, None, None]:
    """Gets the year or years to load historic data for"""
    if back_history > 0:
        # create a generator to yield the years to load
        for i in range(back_history):
            yield current - 1
            current -= 1
    else:
        print("No historic data to load")
        return None


def hist_paths_to_load(hist_folder: str, history_years: Generator[int, None, None]) -> List[str]:
    """Creates a list of paths to load historic data from"""
    # Create a list of paths to load
    hist_paths = []
    for year in history_years:
        hist_path = hist_folder + "qv_BERD_" + str(year) + "12_qv6_reformatted.csv"
        hist_paths.append(hist_path)
    return hist_paths


def load_history(year_generator: Generator[int, None, None], hist_folder_path: str, read_csv_func: callable) -> None:
    if year_generator is None:
        history_loader_logger.info("No historic data to load for this run.")
    else:
        history_loader_logger.info("Loading historic data...")
        hist_paths_load_list = hist_paths_to_load(hist_folder_path, year_generator)
        # Load each historic csv into a dataframe
        dfs_dict = {}
        for path in hist_paths_load_list:
            df = read_csv_func(path)
            # Use regex to extract the BERD survey addition
            key = re.search(r"(BERD_202\d+)", path).group(1)
            if key is None:
                # If regex didn't work use basename
                key = str(os.path.basename(path))
            dfs_dict[key] = df
        history_loader_logger.info("Historic data loaded.")
