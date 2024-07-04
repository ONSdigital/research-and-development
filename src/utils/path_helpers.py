"""This module contains helper functions for creating paths."""


def get_paths(config: dict) -> dict:
    """Return either network_paths or hdfs_paths despending on the environment."""
    network_or_hdfs = config["global"]["network_or_hdfs"]
    paths = config[f"{network_or_hdfs}_paths"]
    paths["year"] = config["years"]["survey_year"]
    paths["berd_path"] = f"{paths['root']}{paths['year']}_surveys/BERD/"
    return paths


def create_staging_paths_dict(config: dict) -> dict:
    """Create a dictionary with all the paths needed for the staging module.

    Args:
        config (dict): The pipeline configuration.

    Returns:
        dict: A dictionary with all the paths needed for the staging module.
    """
    paths = get_paths(config)
    root_path = paths["root"]
    staging_config = config["staging_paths"]
    wanted_keys = list(staging_config.keys())[1:]
    berd_path = paths["berd_path"]
    folder_path = f"{berd_path}{config['staging_paths']['folder']}"

    # set up the staging paths dictionary
    staging_dict = {key: f"{folder_path}/{staging_config[key]}" for key in wanted_keys}
    staging_dict["snapshot_path"] = f"{root_path}{paths['snapshot_path']}"
    ss_path = f"{root_path}{paths['secondary_snapshot_path']}"
    staging_dict["secondary_snapshot_path"] = ss_path
    staging_dict["postcode_masterlist"] = f"{root_path}{paths['postcode_masterlist']}"
    staging_dict["manual_outliers_path"] = f"{berd_path}{paths['manual_outliers_path']}"
    staging_dict["manual_imp_trim_path"] = f"{berd_path}{paths['manual_imp_trim_path']}"

    return staging_dict


def create_mapping_paths_dict(config: dict) -> dict:
    """Create a dictionary with all the paths needed for the mapping module.

    Args:
        config (dict): The pipeline configuration.

    Returns:
        dict: A dictionary with all the paths needed for the mapping module.
    """
    paths = get_paths(config)
    root_path = paths["root"]

    year = paths["year"]
    year_mapper_dict = config[f"{year}_mappers"]

    paths["mappers"] = f"{paths['root']}{paths['year']}_surveys/mappers/"

    version = year_mapper_dict["mappers_version"]
    map_folder = f"{root_path}{year}_surveys/mappers/{version}/"

    wanted_keys = list(year_mapper_dict.keys())[1:]
    mapping_dict = {k: f"{map_folder}{year_mapper_dict[k]}" for k in wanted_keys}

    return mapping_dict


def create_imputation_paths_dict(config: dict) -> dict:
    """Create a dictionary with all the paths needed for the imputation module.

    Args:
        config (dict): The pipeline configuration.

    Returns:
        dict: A dictionary with all the paths needed for the imputation module.
    """
