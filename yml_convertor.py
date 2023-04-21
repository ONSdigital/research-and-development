import yaml


yml_file = "./environment.yml"


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


ydep = yml_dependencies()


def yml_conda_dependencies(dep_list) -> list:
    """_summary_

    Arguments:
        dep_list -- _description_

    Returns:
        _description_
    """
    yml_conda = dep_list[:-1]
    return yml_conda


y_condadep = yml_conda_dependencies(ydep)
y_condadep.sort()


def yml_pip_dependencies(dep_list) -> list:
    """_summary_

    Arguments:
        dep_list -- _description_

    Returns:
        _description_
    """
    yml_pip = dep_list[-1]["pip"]
    return yml_pip


y_pipdep = yml_pip_dependencies(ydep)
y_pipdep.sort()
