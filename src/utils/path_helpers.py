from typing import Tuple


def get_paths(config: dict) -> dict:
    """ "Return either network_paths or hdfs_paths despending on the environment."""
    network_or_hdfs = config["global"]["network_or_hdfs"]
    return config[f"{network_or_hdfs}_paths"]


def get_root_paths(config: dict) -> Tuple[dict, dict]:
    """Create a dictionary for the root paths."""
    year = config["years"]["survey_year"]
    paths = get_paths(config)
    root_dict = {
        "root_path": paths["root"],
        "berd_path": f"{paths['root']}survey_{year}/BERD/",
    }
    return root_dict


def create_staging_paths_dict(config: dict) -> dict:
    """Create a dictionary with all the paths needed for the staging module.

    Args:
        config (dict): The pipeline configuration.

    Returns:
        dict: A dictionary with all the paths needed for the staging module.
    """
    roots = get_root_paths(config)
    paths = get_paths(config)
    root_path = roots["root_path"]
    staging_config = config["staging_paths"]
    wanted_keys = list(staging_config.keys())[1:]
    folder_path = f"{roots['berd_path']}{config['staging_paths']['folder']}"

    # set up the staging paths dictionary
    staging_dict = {key: f"{folder_path}/{staging_config[key]}" for key in wanted_keys}
    staging_dict["snapshot_path"] = f"{root_path}/{paths['snapshot_path']}"
    ss_path = f"{root_path}/{paths['secondary_snapshot_path']}"
    staging_dict["secondary_snapshot_path"] = f"{root_path}/{ss_path}"
    staging_dict["postcode_masterlist"] = f"{root_path}/{paths['postcode_masterlist']}"

    return staging_dict


def create_mapping_paths_dict(config: dict) -> dict:
    """Create a dictionary with all the paths needed for the mapping module.

    Args:
        config (dict): The pipeline configuration.

    Returns:
        dict: A dictionary with all the paths needed for the mapping module.
    """
    roots = get_root_paths(config)
    root_path = roots["root_path"]

    year = config["years"]["survey_year"]
    year_mapper_dict = config[f"{year}_mappers"]

    mapper_folder = f"{root_path}mappers/{year_mapper_dict['mappers_version']}/"

    mapping_dict = {mapper_folder + value for value in year_mapper_dict.values()}

    return mapping_dict
