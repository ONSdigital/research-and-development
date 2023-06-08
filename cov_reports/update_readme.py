from subprocess import run

run(["python", "-m", "coverage", "run", "-m", "pytest"])
run(["readme-cov"])
run(["git", "add", "README.md"])
run(["git", "commit", "-m", "Auto-updating readme coverage badge"])
