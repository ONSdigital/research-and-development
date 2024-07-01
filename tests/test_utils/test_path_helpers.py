"""Tests for path_helpers.py """

from src.utils.path_helpers import get_root_paths, create_staging_paths_dict


def test_get_root_paths():
    config = {
        "global": {"network_or_hdfs": "network"},
        "network_paths": {"root": "R:/DAP_emulation/"},
        "years": {"survey_year": 2022},
    }

    expected_root_dict = {
        "root_path": "R:/DAP_emulation/",
        "berd_path": "R:/DAP_emulation/survey_2022/BERD/",
    }

    root_dict, paths = get_root_paths(config)

    assert root_dict == expected_root_dict
    assert paths == config["network_paths"]


def test_create_staging_paths_dict():
    config = {
        "global": {"network_or_hdfs": "network"},
        "network_paths": {
            "root": "R:/DAP_emulation/",
            "snapshot": "snapshot_path/snap.csv",
            "secondary_snapshot": "secondary_snapshot_path/snap2.csv",
        },
        "years": {"survey_year": 2022},
        "staging_paths": {
            "folder": "01_staging",
            "feather_output": "feather",
        },
    }

    expected_staging_dict = {
        "feather_output": "R:/DAP_emulation/survey_2022/BERD/01_staging/feather",
        "snapshot_path": "R:/DAP_emulation/snapshot/snap.csv",
        "secondary_snapshot_path": "R:/DAP_emulation/secondary_snapshot/snap2.csv",
        "feather_path": "R:/DAP_emulation/survey_2022/BERD/01_staging/feather",
    }
