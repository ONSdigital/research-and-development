from importlib import reload
import time

import src.pipeline as src

# reload the pipeline module to implement any changes
reload(src)

start = time.time()
src.run_pipeline(start)
