import yaml

# yaml = ruamel.yaml.YAML()
yml_env = yaml.safe_load(open("./environment.yml"))

requirements = []
for dep in yml_env["dependencies"]:
    if isinstance(dep, str):
        if "=" in dep:
            package, package_version = dep.split("=")
            if package == "python":
                requirements.append(package + "==3.6.2")
            else:
                requirements.append(package + "==" + package_version)
        else:
            requirements.append(dep)
    elif isinstance(dep, dict):
        for preq in dep.get("pip", []):
            requirements.append(preq)
    print(requirements)

with open("./requirements.txt", "w") as fp:
    for requirement in requirements:
        print(requirement, file=fp)
