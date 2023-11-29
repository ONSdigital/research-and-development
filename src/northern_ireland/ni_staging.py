"""Stage and Validate Northern Ireland BERD data."""
import logging
from typing import Callable, Tuple
from datetime import datetime
import pandas as pd
import os

from src.staging import validation as val

NIStagingLogger = logging.getLogger(__name__)

print("hello NI")