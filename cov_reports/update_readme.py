from subprocess import run

run(["python", "-m", "coverage", "run", "-m", "pytest"])
run(["readme-cov"])
