from importlib import reload
import time
import os
import postcodes_uk
import src.pipeline as src

# reload the pipeline module to implement any changes
reload(src)
config_path = os.path.join("src", "developer_config.yaml")

start = time.time()
run_time = src.run_pipeline(start, config_path)

minutes, seconds = divmod(run_time, 60)
print("Run time: {} minutes, {:.2f} seconds".format(int(minutes), seconds))
