import subprocess


subprocess.run(["python", "-m", "coverage", "run", "-m", "pytest"])
subprocess.run(["python", "-m", "readme-cov"])
