"""Setup script for creating package from code."""
from setuptools import setup, find_packages
import re

# Specify and open the version file
VERSION_FILE = "src/_version.py"
verstrline = open(VERSION_FILE, "rt").read()
print(verstrline)

# Automatically detect the package version from VERSION_FILE
VERSION_REGEX = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(VERSION_REGEX, verstrline, re.M)
if mo:
    version_string = mo.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." % (VERSION_FILE,))

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="research-and-development",
    version=version_string,
    description="Public Sector local Python downloads and preprocessing package",
    url="https://github.com/ONSdigital/research-and-development",
    packages=find_packages(),
    package_data={"": ["*.toml", "*.r", "*.R", "*.pem"]},
    include_package_data=True,
    zip_safe=False,
    install_requires=requirements,
)
