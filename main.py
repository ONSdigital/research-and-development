from importlib import reload
import time
import os

start_dir = os.getcwd()
if start_dir.startswith("/home"):
    os.chdir("research-and-development")
print(f"Current directory is {os.getcwd()}")
import src.pipeline as src

# reload the pipeline module to implement any changes
reload(src)

user_path = os.path.join("src", "user_config.yaml")
dev_path = os.path.join("src", "dev_config.yaml")

start = time.time()
run_time = src.run_pipeline(user_path, dev_path)

min_secs = divmod(round(run_time), 60)

print(f"Time taken for pipeline: {min_secs[0]} mins and {min_secs[1]} seconds")
