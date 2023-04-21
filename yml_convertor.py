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
