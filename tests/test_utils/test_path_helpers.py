"""Tests for path_helpers.py """
import pytest

from src.utils.path_helpers import (
    get_paths,
    create_staging_paths_dict,
    create_mapping_paths_dict,
)


@pytest.fixture(scope="module")
def config():
    config = {
        "global": {"network_or_hdfs": "network"},
        "network_paths": {
            "root": "R:/DAP_emulation/",
            "snapshot_path": "snapshot_path/snap.csv",
            "secondary_snapshot_path": "secondary_snapshot_path/snap2.csv",
            "postcode_masterlist": "postcode_masterlist_path/postcode.csv",
            "manual_imp_trim_path": "06_imputation/man_trim/trim_qa.csv",
            "manual_outliers_path": "07_outliers/man_out/man_out.csv",
        },
        "years": {"survey_year": 2022},
        "staging_paths": {
            "folder": "01_staging",
            "feather_output": "feather",
        },
        "2022_mappers": {
            "mappers_version": "v1",
            "postcodes_mapper": "postcodes_2022.csv",
            "itl_mapper_path": "itl_2022.csv",
        },
    }
    return config


def test_get_paths(config):
    """Test get_paths function."""
    expected_network_paths = {
        "root": "R:/DAP_emulation/",
        "snapshot_path": "snapshot_path/snap.csv",
        "secondary_snapshot_path": "secondary_snapshot_path/snap2.csv",
        "postcode_masterlist": "postcode_masterlist_path/postcode.csv",
        "manual_outliers_path": "07_outliers/man_out/man_out.csv",
        "manual_imp_trim_path": "06_imputation/man_trim/trim_qa.csv",
        "year": 2022,
        "berd_path": "R:/DAP_emulation/2022_surveys/BERD/",
    }
    network_paths = get_paths(config)

    assert network_paths == expected_network_paths


def test_create_staging_paths_dict(config):
    """Test create_staging_paths_dict function."""

    expected_staging_dict = {
        "feather_output": "R:/DAP_emulation/2022_surveys/BERD/01_staging/feather",
        "snapshot_path": "R:/DAP_emulation/snapshot_path/snap.csv",
        "secondary_snapshot_path": "R:/DAP_emulation/secondary_snapshot_path/snap2.csv",
        "postcode_masterlist": "R:/DAP_emulation/postcode_masterlist_path/postcode.csv",
        "manual_outliers_path": (  # noqa E501
            "R:/DAP_emulation/2022_surveys/BERD/07_outliers/man_out/man_out.csv"
        ),
        "manual_imp_trim_path": (  # noqa E501
            "R:/DAP_emulation/2022_surveys/BERD/06_imputation/man_trim/trim_qa.csv"
        ),
    }
    staging_dict = create_staging_paths_dict(config)

    assert staging_dict == expected_staging_dict


def test_create_mapping_paths_dict(config):
    """Test create_mapping_paths_dict function."""

    expected_mapping_dict = {
        "postcodes_mapper": (  # noqa E501
            "R:/DAP_emulation/2022_surveys/mappers/v1/postcodes_2022.csv"
        ),
        "itl_mapper_path": "R:/DAP_emulation/2022_surveys/mappers/v1/itl_2022.csv",
    }
    mapping_dict = create_mapping_paths_dict(config)

    assert mapping_dict == expected_mapping_dict
