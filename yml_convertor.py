import yaml
import os


def yml_dependencies(yml="./environment.yml") -> list:
    """_summary_

    Keyword Arguments:
        yml -- _description_ (default: {"./environment.yml"})

    Returns:
        _description_
    """
    yml_env = yaml.safe_load(open(yml))
    yml_dep = yml_env["dependencies"]
    return yml_dep


def yml_conda_dependencies(dep_list) -> list:
    """_summary_

    Arguments:
        dep_list -- _description_

    Returns:
        _description_
    """
    yml_conda = dep_list[:-1]
    yml_conda.sort()
    return yml_conda


def yml_pip_dependencies(dep_list) -> list:
    """_summary_

    Arguments:
        dep_list -- _description_

    Returns:
        _description_
    """
    yml_pip = dep_list[-1]["pip"]
    yml_pip.sort()
    return yml_pip


def deps_combnd(conda_deps, pip_deps) -> list:
    """_summary_

    Arguments:
        conda_deps -- _description_
        pip_deps -- _description_

    Returns:
        _description_
    """
    full_deps = conda_deps + pip_deps
    full_deps.sort()
    return full_deps


def req_check(req="./requirements.txt") -> bool:
    """_summary_

    Keyword Arguments:
        req -- _description_ (default: {"./requirements.txt"})

    Returns:
        _description_
    """
    isFile = os.path.isfile(req)
    return isFile


def req_create(req) -> bool:
    """_summary_

    Arguments:
        req -- _description_

    Returns:
        _description_
    """
    if not req:
        f = open(req, "x")
        f.close()
        return True
    else:
        return False


def req_compare(dep_file, dep_list) -> list:
    """_summary_

    Arguments:
        dep_file -- _description_
        dep_list -- _description_

    Returns:
        _description_
    """
    f = open(dep_file, "w+")
    req_existing = f.read()
    req_list = req_existing.split("\n")
    req_list.sort()

    unique_deps = list(set(dep_list) - set(req_list))
    unique_deps.sort()

    for line in unique_deps:
        f.write(f"{line}\n")
    f.close()

    return unique_deps


yml_file = "./environment.yml"
ydep = yml_dependencies()
y_condadep = yml_conda_dependencies(ydep)
y_pipdep = yml_pip_dependencies(ydep)
dependencies = deps_combnd(y_condadep, y_pipdep)
req_file = "./requirements.txt"
req_exist = req_check(req_file)
check = req_create(req_file)
test = req_compare(req_file, dependencies)
