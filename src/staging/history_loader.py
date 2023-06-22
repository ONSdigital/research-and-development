"""Functions relating to loading historic data"""


def history_years(current, back_history):
    """Gets the year or years to load historic data for"""

    if back_history > 0:
        # create a generator to yield the years to load
        for i in range(back_history):
            yield current - 1
            current -= 1
    else:
        print("No historic data to load")
        return None


def hist_paths_to_load(hist_folder, history_years):
    """Creates a list of paths to load historic data from"""

    # create a list of paths to load
    hist_paths = []
    for year in history_years:
        hist_path = hist_folder + "qv_BERD_" + str(year) + "12_qv6_reformatted.csv"
        hist_paths.append(hist_path)

    return hist_paths
