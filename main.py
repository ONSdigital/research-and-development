from importlib import reload
import time
import os

import src.pipeline as src

# reload the pipeline module to implement any changes
reload(src)
config_path = os.path.join("src", "developer_config.yaml")

start = time.time()
src.run_pipeline(start, config_path)
