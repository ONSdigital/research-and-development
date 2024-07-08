'''This script produces a synthetic small sample of SPP snapshot.
It also creates a postcodes mapper that only has the postcodes we need.'''

# Configuration hardcoded - input arguments
config  = {
    "global": {
        "create_schemas": True,
    },
    "network_paths": {
        "input_dir": r"D:\data\res_dev\synthetic\inputs",
        "output_dir": r"D:\data\res_dev\synthetic\outputs",
        "schemas_dir": r"D:\data\res_dev\synthetic\schemas",
        "input_snapshot": r"staged_BERD_full_responses_2024-06-03_v16.csv",
    },
}
# Run everything
if __name__ == "__main__":
    print(config)