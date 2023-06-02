import pytest
import os

hdfs_skip = pytest.mark.skipif(
    os.environ.get("USER") == "cdsw", reason="HDFS cannot be accessed from Jenkins"
)
