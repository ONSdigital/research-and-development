from subprocess import run, DEVNULL


omit_files = """--omit=src/utils/hdfs_mods.py,
src/utils/wrappers.py,src/utils/runlog.py,
src/_version.py,src/pipeline.py,
/home/cdsw/.local/lib/python3.6/site-packages/*.py"""

# Run the tests & generate the coverage report
command = ["coverage", "report", f"{omit_files}"]  # no qa

# Define cover report file
output_file = "cov_reports/coverage_report.txt"
with open(output_file, "w") as f:
    run(command, stdout=f, universal_newlines=True)


# Read the contents of the report file
with open(output_file, "r") as f:
    report_lines = f.readlines()

# Extract the coverage percentage
coverage_percentage = report_lines[-1][-5:].strip()
print(f"Coverage is: {coverage_percentage}")

# readme-cov generates the coverage badge and updates the README.md file
run(["readme-cov"])

# The following process only runs if there are changes in the coverage level
# Check for changes in README.md
result = run(["git", "diff", "--quiet", "--exit-code", "README.md"], stdout=DEVNULL)

if result.returncode == 0:
    print("No changes in README.md")
else:
    # Add README.md to the staging area
    run(["git", "add", "README.md"])

    # Auto-commit the changes
    run(["git", "commit", "-m", "Auto-updating readme file"])
    print("Changes in README.md committed")
