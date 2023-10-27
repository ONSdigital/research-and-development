"""TODO."""

import logging
import pandas as pd


construction_logger = logging.getLogger(__name__)


def run_construction(main_snapshot, updated_snapshot, config, write_csv, run_id):
    """Define placeholder construction main function."""
    # ! For now, we add the year column since neither file has it
    main_snapshot["year"] = 2022
    updated_snapshot["year"] = 2022

    pass
