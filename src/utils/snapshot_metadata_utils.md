Main Goal:
- Find the correct SPP snapshot file from S3 manifest metadata, based on date, version,
  and filename pattern.

1. Get all manifest files from S3 with a given prefix (filepath).
   - Use S3 client to list all files ending with ".mani" under the prefix.
   - Store as {filename: last_modified_date}, sorted by date descending.

2. Filter manifest files by substring
   - Use substring search to obtain files relating to the correct survey and survey year

3. If required, get the most recent modified date and check files modified this date.
   - Read metadata for all files modified this date
   - Check whether the spp created date is the same  for each file
   - Check whether other metadata is valid for each file
   - Create a list of candidate files with their metadata

4. If a specified date was given, check all files modified this date.
   - Read metadata for all files modified this date
   - Check whether the spp created date is the same date for each file
   - Check whether other metadata is valid for each file
   - Create a list of candidate files with their metadata

5. If the created date for a file is earlier than it's modified date (it can't be later)
   - Save the created date in a "most recent created" variable
   - Continue the loop through files to the next most recent modified file and onwards
   - If a created date is found that is more recent than that in the "most recent created" varible, and the modified date is the same, that is the candidate.
   - When the next most recent modified file is modified after the "most recent
     created" date, the "most recent created" date is the candidate.
   - All files created on a "most recent created" date are candidates.

6. From candidate files:
   - If multiple, select the most recent by last_modified_date.
   - If multiple with same filename, select the one with the highest version.

7. Return the chosen snapshot filename, version, and created date.
   - Conditionally return file info for the most recent date
   - Conditionally return file info for specified date

Helper Functions:
- filter_manifest_files(files_dict, target_date, wanted_str)
- get_spp_file_info_from_manifest(mani_filename, mani_last_modified_date)
- check_metadata(metadata, target_date)
- check_files(mani_files_dict, target_date)
- get_most_recent_file(file_list)
- get_lastest_version_file(file_list)
