import yaml
import os


def yml_dependencies(yml="../../environment.yml") -> list:
    """Loads an environment.yml file into a list. The values
    of the 'dependencies' dictionary, an entry in the safe_load
    list, are separated into their own list and returned by the
    function.

    Keyword Arguments:
        yml -- .yml environment file to be passed to function
        (default: {"../../environment.yml"})

    Returns:
        A list: list containing the values of the 'dependencies'
        dictionary.
    """
    yml_env = yaml.safe_load(open(yml))
    yml_dep = yml_env["dependencies"]
    return yml_dep


def yml_conda_dependencies(dep_list) -> list:
    """Takes the total list of dependencies from the environment.yml
    file (returned by yml_dependecies()) and returns only those that
    are conda specific.

    Arguments:
        dep_list -- return value of yml_dependencies(). Total list
        of dependencies from the environment.yml file.

    Returns:
        A list: sorted list containing dependencies unique to conda.
    """
    yml_conda = dep_list[:-1]
    yml_conda.sort()
    return yml_conda


def yml_pip_dependencies(dep_list) -> list:
    """akes the total list of dependencies from the environment.yml
    file (returned by yml_dependecies()) and returns only those that
    are pip specific.

    Arguments:
        dep_list -- return value of yml_dependencies(). Total list
        of dependencies from the environment.yml file.

    Returns:
        A list: sorted list containing dependencies unique to pip.
    """
    yml_pip = dep_list[-1]["pip"]
    yml_pip.sort()
    return yml_pip


def deps_combnd(conda_deps, pip_deps) -> list:
    """Combines the conda and pip dependencies lists into a single sorted
    list.

    Arguments:
        conda_deps -- list containing dependencies unique to conda
        pip_deps -- list containing dependencies unique to pip

    Returns:
        A list: sorted list containing all dependencies from environment.yml
    """
    full_deps = conda_deps + pip_deps
    full_deps.sort()
    return full_deps


def req_check(req="../../requirements.txt") -> bool:
    """Checks if the requirements.txt file already exists or not.

    Keyword Arguments:
        req -- relative path to the requirements.txt file.
        (default: {"../../requirements.txt"})

    Returns:
        A bool: boolean value indicating if file exists or not.
    """
    isFile = os.path.isfile(req)
    return isFile


def req_create(req="../../requirements.txt") -> bool:
    """Create a requirements.txt file if one doesn't exist, otherwise
    do nothing.

    Arguments:
        req -- relative path to the requirements.txt file.
        (default: {"../../requirements.txt"})

    Returns:
        A bool: boolean value, if True then file has been created, else False.
    """
    if not req:
        f = open(req, "x")
        f.close()
        return True
    else:
        return False


def req_compare(dep_list, dep_file="../../requirements.txt") -> tuple:
    """Function to compare dependencies from environment.yml and
    existing requirements.txt files. The differences in dependencies
    between the two files is returned.

    Arguments:
        dep_list -- full list of dependencies from the environment.yml file.
        dep_file -- relative path to the requirements.txt file.
        (default: {"../../requirements.txt"})


    Returns:
        A tuple: tuple containing two lists. The first list contains the
        differences between the environment.yml dependencies and those in
        requirements.txt. List two contains the reverse.
    """
    f = open(dep_file, "r")
    req_existing = f.read()
    req_list = req_existing.split("\n")
    req_list.sort()
    f.close()

    unique_deps_1 = list(set(dep_list) - set(req_list))
    unique_deps_1.sort()
    unique_deps_2 = list(set(req_list) - set(dep_list))
    unique_deps_2.sort()

    return unique_deps_1, unique_deps_2


def req_write(dep_list, dep_file="../../requirements.txt") -> list:
    """Function to compare dependencies from environment.yml and
    existing requirements.txt files. If there are differences in the
    environment.yml file then the requirements.txt is updated accordingly

    Arguments:
        dep_list -- full list of dependencies from the environment.yml file.
        dep_file -- relative path to the requirements.txt file.
        (default: {"../../requirements.txt"})


    Returns:
        A list: list of differences between the environment.yml dependencies
        and those in requirements.txt.
    """

    diff = req_compare(dep_list, dep_file)

    if not diff[0]:
        msg = "No unique dependencies in environment.yml compared to requirements.txt."
        return msg
    else:
        f = open(dep_file, "r+")
        req_existing = f.read()
        req_list = req_existing.split("\n")
        sorted_req_list = sorted(req_list, key=str.casefold)
        print(f"Req list = {sorted_req_list} \n")

        unique_deps = list(set(dep_list) - set(req_list))

        total_deps = sorted_req_list[1:] + unique_deps
        sorted_total_deps = sorted(total_deps, key=str.casefold)

        f.seek(0)

        for line in sorted_total_deps:
            f.write(f"{line}\n")
        f.close()

        msg = f"Difference in environment.yml and requirements.txt is {unique_deps}."
        return msg


# Lines for testing the above code
yml_file = "../../environment.yml"
ydep = yml_dependencies()
y_condadep = yml_conda_dependencies(ydep)
y_pipdep = yml_pip_dependencies(ydep)
dependencies = deps_combnd(y_condadep, y_pipdep)
req_file = "../../requirements.txt"
req_exist = req_check(req_file)
check = req_create(req_file)
test = req_write(dependencies, req_file)
