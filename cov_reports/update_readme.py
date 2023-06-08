from subprocess import run, DEVNULL

# Run the tests and generate the coverage report
command = [
    'coverage',
    'run',
    '--omit=src/utils/hdfs_mods.py,src/utils/wrappers.py,src/utils/runlog.py,src/_version.py,src/pipeline.py',
    '-m',
    'pytest'
]

# Generate the coverage report
run(['coverage', 'report'])

# Run the coverage report command and redirect the output to a file
output_file = 'coverage_report.txt'
with open(output_file, 'w') as f:
    run(['coverage', 'report', '--fail-under=80'], stdout=f, universal_newlines=True)

# Read the contents of the output file
with open(output_file, 'r') as f:
    report_lines = f.readlines()

# Extract the coverage percentage
coverage_percentage = report_lines[-1].split()[3]
print(f"Coverage: {coverage_percentage}%")


# readme-cov generates the coverage badge and updates the README.md file
run(["readme-cov"])

# The following process only runs if there are changes in the coverage level
# Check for changes in README.md
result = run(['git', 'diff', '--quiet', '--exit-code', 'README.md'], stdout=DEVNULL)

if result.returncode == 0:
    print("No changes in README.md")
else:
    # Add README.md to the staging area
    run(['git', 'add', 'README.md'])

    # Commit the changes
    run(['git', 'commit', '-m', 'Auto-updating readme file'])
    print("Changes in README.md committed")
