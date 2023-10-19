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

min_secs = divmod(round(run_time), 60)

print(f"Time taken for pipeline: {min_secs[0]}mins and {min_secs[1]}seconds")
