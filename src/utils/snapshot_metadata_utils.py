"""
Functions to obtain information about spp snapshots from metadata manifest files.

These scripts are designed to run in DAP S3 environment only.
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime

from src.utils.s3_mods import rd_load_json, rd_list_manifest_files

if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO)

MetadataLogger = logging.getLogger(__name__)


@dataclass
class SPPMetadata:
    # include default values to check against
    mani_filename: str
    mani_last_modified_date: str
    spp_filename: str
    spp_created_date: str
    version: int
    description: str = "SPP BERD snapshot files"
    iterationL1: str = "spp_snapshots"


def filter_manifest_files(
    files_dict: dict[str, datetime], target_date: str = "", wanted_str: str = ""
) -> dict[str, str]:
    """Filter manifest files by target date and/ or filename substring.

    Args:
        files_dict (dict): Dictionary of {filename: last_modified_date}.
        target_date (str): The target date in 'YYYY-MM-DD' format.
        wanted_str (str): Substring that should be present in the filename.

    Returns:
        dict: Filtered dictionary of {filename: last_modified_date}.
    """
    if wanted_str:
        filtered_files = {
            filename: last_mod_date.strftime("%Y-%m-%d")
            for filename, last_mod_date in files_dict.items()
            if wanted_str in filename
        }
    if target_date:
        filtered_files = {
            filename: last_mod_date.strftime("%Y-%m-%d")
            for filename, last_mod_date in files_dict.items()
            if last_mod_date.strftime("%Y-%m-%d") == target_date
        }
    return filtered_files


def get_spp_file_info_from_manifest(
    mani_filename: str, mani_last_modified_date: str
) -> SPPMetadata:
    """Get SPP file information from a manifest file as SPPMetadata.

    Required fields are stored in a SPPMetadata data class for clarity.

    Args:
        mani_filename (str): The manifest filename.
        mani_last_modified_date (str): The last modified date of the manifest file.

    Returns:
        SPPMetadata: The SPPMetadata object with information from the manifest.
    """
    manif_file_dict = rd_load_json(mani_filename)

    metadata = SPPMetadata(
        mani_filename=mani_filename,
        mani_last_modified_date=mani_last_modified_date,
        spp_filename=manif_file_dict["files"][0]["name"],
        spp_created_date=str(manif_file_dict["files"][0]["scanFileUploadTime"])[:10],
        version=manif_file_dict.get("version", 1),
        description=manif_file_dict.get("description", ""),
        iterationL1=manif_file_dict.get("iterationL1", ""),
    )
    return metadata


def check_metadata(metadata: SPPMetadata, target_date: str) -> dict:
    """Check the created date in metadata matches the target date and other validation.

    Also check description and iterationL1 fields to ensure data is as expected.

    Args:
        metadata (SPPMetadata): The SPPMetadata object to check.
        target_date (str): The target date in 'YYYY-MM-DD' format.

    Returns:
        dict: Dict of mismatched metadata entries, empty if all match.
    """
    # remove the path and .mani extension from the manifest filename for the spp name
    exp_spp_filename = os.path.basename(metadata.mani_filename).replace(".mani", "")
    # if no target date is given, check whether the created date is the same as
    # the manifest last modified date
    if target_date == "":
        target_date = metadata.mani_last_modified_date

    check_dict = {
        "spp_filename": exp_spp_filename,
        "spp_created_date": target_date,
    }

    error_dict = {
        k: {"expected": v, "found": getattr(metadata, k, None)}
        for k, v in check_dict.items()
        if getattr(metadata, k, None) != v
    }

    if error_dict:
        msg = (
            f"Metadata check failed for target date {target_date}. "
            f"Mismatched entries: {error_dict}"
        )
        MetadataLogger.error(msg)
        return error_dict
    return {}


def check_files(mani_files_dict: dict, target_date: str = "") -> list[SPPMetadata]:
    """Check filtered manifest file dict for metadata validity and (optionally) date.

    If a target date is provided, the files dict only contain files from that date.

    If no target date is provided, then the function will check the newest files first
    until a valid file is found (the dictionary has been sorted by last modified date).

    However, if the `spp_created_date` is earlier than the modified date, the function
    will continue checking further files to find the latest creation date.

    Args:
        mani_files_dict (dict): {manifest_filename: last_modified_date}
        target_date (str): Target date in 'YYYY-MM-DD' format. If empty, no date check.

    Returns:
        list: List with candidate file information if metadata matches target date.
    """
    candidate_file_list = []
    for mani_filename, mani_mod_date in mani_files_dict.items():
        spp_metadata = get_spp_file_info_from_manifest(mani_filename, mani_mod_date)
        error_dict = check_metadata(spp_metadata, target_date)
        if not error_dict:
            new_candidate = SPPMetadata(
                mani_filename=mani_filename,
                mani_last_modified_date=mani_mod_date,
                spp_filename=spp_metadata.spp_filename,
                spp_created_date=spp_metadata.spp_created_date,
                version=spp_metadata.version,
            )
            candidate_file_list.append(new_candidate)
        # if the only error is created date < modified date, continue checking-
        # elif "spp_created_date" in error_dict and not "spp_filename" in error_dict:

    return candidate_file_list


def get_most_recent_file(file_list: list[SPPMetadata]) -> tuple[str, str]:
    """Get the filename of the most recently modified file from a dictionary.

    Args:
        file_dict (dict): {filename: last_modified_date}

    Returns:
        tuple: The filename of the most recently modified file with last modified date.
    """
    if len(file_list) > 1:
        # sort the list by last modified date and get the most recent
        sorted_files = sorted(
            file_list, key=lambda x: x.mani_last_modified_date, reverse=True
        )
        most_recent_file = (sorted_files[0].spp_filename,)
        most_recent_date = sorted_files[0].mani_last_modified_date
    else:
        most_recent_file = file_list[0].spp_filename
        most_recent_date = file_list[0].mani_last_modified_date
    return most_recent_file, most_recent_date


def get_lastest_version_file(
    file_list: list[SPPMetadata],
) -> tuple[str, int, str]:
    """Get filename of the highest version file from a list where filenames the same.

    Args:
        file_list (list): List of SnapshotCandidateFileInfo objects.

    Returns:
        tuple: The filename of the highest version file and its version number.
    """
    if len(file_list) > 1:
        # sort the list by version and get the highest version unless only one file
        file_list = sorted(file_list, key=lambda x: x.version, reverse=True)
    highest_version_file = file_list[0].spp_filename
    corresponding_version = file_list[0].version
    corresponding_created_date = file_list[0].spp_created_date

    return highest_version_file, corresponding_version, corresponding_created_date


def get_snapshot_name(
    prefix: str, survey_year: str, spp_date: str = ""
) -> tuple[str, int, str]:
    """Return the name, version and created date of an spp snapshot.

    s3_client code exists to list manifest filenames with their last modified dates.
    However, it is necessary to read the manifest files to get the snapshot created date
    and version number.

    This code will start by filtering for the required

    Args:
        survey_year (str): The survey year the snapshot belongs to.
        spp_date (str): The date the snapshot was last modified (YYYY-MM-DD).

    Returns:
        tuple: The name of the spp snapshot, its version, and created date.
    """
    # get a dictionary of all manifest files with the given prefix,
    # sorted by last modified date
    files_dict = rd_list_manifest_files(prefix)
    # filter the manifest files for the given date (if given) and wanted string
    wanted_str = f"snapshot-{survey_year}12-002-"
    filtered_mani_file_list = filter_manifest_files(files_dict, spp_date, wanted_str)
    # check filtered files for metadata matching the target date and metadata validity
    candidate_file_list = check_files(filtered_mani_file_list, spp_date)

    # logical condition for whether to look for latest file rather than target date
    return_latest = spp_date == ""

    if not candidate_file_list and not return_latest:
        msg = (
            f"No valid SPP snapshot found for date {spp_date}.\n"
            "Now looking for latest snapshot."
        )
        MetadataLogger.info(msg)
        # if we checked against a target date but no candidate files are found,
        # try filtering only by wanted string and no target date.
        return_latest = True
        filtered_mani_file_list = filter_manifest_files(
            files_dict, wanted_str=wanted_str
        )
        candidate_file_list = check_files(filtered_mani_file_list, spp_date)

    if not candidate_file_list:
        raise FileNotFoundError(
            f"No valid SPP snapshot manifest files found with prefix {prefix}."
        )

    # find the most recent file if multiple candidates are found
    snapshot_name, most_recent_date = get_most_recent_file(candidate_file_list)
    # check whether snapshot filename is unique, if not get the highest version
    duplicate_files = [
        file for file in candidate_file_list if file.spp_filename == snapshot_name
    ]
    snapshot_name, version, created_date = get_lastest_version_file(duplicate_files)

    # log returned values to screen
    if return_latest:
        MetadataLogger.info(
            f"The latest SPP snapshot is: {snapshot_name},"
            f"version {version},"
            f"created date {created_date}."
        )
    else:
        MetadataLogger.info(
            f"The SPP snapshot for date {spp_date} is: {snapshot_name},"
            f"version {version},"
            f"created date {created_date}."
        )
    return snapshot_name, version, created_date
