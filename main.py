from importlib import reload
import time

import src.main as src

reload(src)

start = time.time()
src.run_pipeline(start)
# commenting to check commits in cdsw