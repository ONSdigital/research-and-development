from importlib import reload
import time

import src.pipeline as src

# reload the pipeline module to implement any changes
reload(src)
config_path = r"D:\repos\research-and-development\src\developer_config.yaml"

start = time.time()
src.run_pipeline(start, config_path)
