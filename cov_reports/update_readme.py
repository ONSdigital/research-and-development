import os

os.system("cd ..")
os.system("pip3 install coverage readme-coverage-badger")
os.system("python -m coverage run -m pytest")
os.system("readme-cov")
