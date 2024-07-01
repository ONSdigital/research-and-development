import os

os.system("cd ..")
os.system("pip3 install coverage readme-coverage-badger")
OMIT_STR = (
    "--omit=src/utils/hdfs_mods.py,src/utils/wrappers.py,src/utils/runlog.py,src/_ver"
    "sion.py,src/pipeline.py,src/*_main.py"#
)
os.system(f"python -m coverage run -m --source=src {OMIT_STR} pytest tests")
os.system("readme-cov")
