"""Script that creates all directories"""
import os

# Change to the project repository location
my_wd = os.getcwd()
my_repo = "research-and-development"
if not my_wd.endswith(my_repo):
    os.chdir(my_repo)

from src.utils.singleton_boto import SingletonBoto

config = {
    "s3": {
        "ssl_file": "/etc/pki/tls/certs/ca-bundle.crt",
        "s3_bucket": "onscdp-dev-data01-5320d6ca"
    }
}

boto3_client = SingletonBoto.get_client(config)
import src.utils.s3_mods as mods


if __name__ == "__main__":

    my_path = "/bat/res_dev/project_data/2023_surveys/BERD/01_staging/staging_qa/full_responses_qa/2023_staged_BERD_full_responses_24-10-02_v20.csv"
#   to_delete_path = "/bat/res_dev/project_data/2023_surveys/BERD/01_staging/staging_qa/full_responses_qa/2023_staged_BERD_full_responses_test_to_delete.csv"
    my_dir = "/bat/res_dev/project_data/2023_surveys/BERD/01_staging/staging_qa/full_responses_qa/"
#     # Checking that file exists
    my_size = mods.rd_file_size(my_path)
    print(f"File size is {my_size}")

     # Calculating md5sum
    my_sum = mods.rd_md5sum(my_path)
    expected_output = "ea94424aceecf11c8a70d289e51c34ea"
    print(type(my_sum))
    if expected_output == my_sum:
        print("Same md5sum")

     # Calculating rd_isdir
    mydir = "/bat"
    response = mods.rd_isdir(mydir)

    print("Got response")
    print(response)

     # Checking rd_isfile
    response = mods.rd_isfile(my_path)
    print(response)

    # Checking that rd_stat_size works for files and directories
    file_size =  mods.rd_stat_size(my_path)
    print(f"File {my_path} size is {file_size} bytes.")

    dir_size =  mods.rd_stat_size(my_dir)
    print(f"Directory {my_dir} size is {dir_size} bytes.")

    # Testing rd_read_header 
    response = mods.rd_read_header(my_path)
    print(response)

    # Testing rd_write_string_to_file
    out_path = "/bat/res_dev/project_data/new_write_string_test_2.txt"
    content = "New content"
    mods.rd_write_string_to_file(content.encode(encoding="utf-8"), out_path)
    print("all done")

    # Testing rd_copy_file
    src_path = "/bat/res_dev/project_data/new_write_string_test_2.txt"
    dst_path = "/bat/res_dev/"
    success = mods.rd_copy_file(src_path, dst_path)
    if success:
        print("File copied successfully")
    else:
        print("File not copied successfully")
      

     # Testing rd_move_file
    src_path = "/bat/res_dev/new_write_string_test_2.txt"
    dst_path = "/bat/res_dev/project_data/"
    success = mods.rd_move_file(src_path, dst_path)
    if success:
       print("File moved successfully")
    else:
        print("File not moved successfully")
      

    # Testing rd_search_file
    dir_path = "bat/res_dev/project_data/2023_surveys/BERD/01_staging/staging_qa/full_responses_qa/"
    ending = "24-10-02_v20.csv"

    found_file = mods.rd_search_file(dir_path, ending)
    print(f"Found file: {found_file}")

     # Deleting a file
#    status = mods.rd_delete_file(my_path)
#    if status:
#        print(f"File {to_delete_path} successfully deleted")

    # Testing read_excel
#    my_path = "bat/res_dev/project_data/test_excel_gz.xlsx"
#    df = mods.read_excel(my_path)
#    print(df.head())