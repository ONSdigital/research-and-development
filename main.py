from importlib import reload
import time

import src.pipeline as src

# reload the pipeline module to implement any changes
reload(src)

# set the config file path
config_path = r"src\developer_config.yaml"

start = time.time()
src.run_pipeline(start, config_path)
