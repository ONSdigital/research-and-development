import json
import toml
import jsonschema
import pandas as pd


def validate_json_shcema(file_path: str, schema: str):
    """
    This function contains  reads a json file into a pandas data frame,
    validating it against a schema imported from toml files
    (config/contributors_schema.toml and config/responses_schema.toml),
    and casting column data types.

    Args:
        file_path (str): The path to the Json file.
        schema (str): The path to the schema file

    Raises:
        FileNotFoundError: If the file or schema does not exist
        jsonschema.ValidationError: If the Json data fails schema validation
    """

    try:
        # read json file
        with open(file_path) as f:
            json_df = json.load(f)

        # load schema from toml
        with open(schema) as f:
            schema_dict = toml.load(f)

        jsonschema.validate(json_df, schema_dict)

        data = pd.DataFrame(json_df)

        for column, dtype in schema_dict.items():
            data[column] = data[column].astype(dtype)

        return data

    except FileNotFoundError:
        print(f"The file {file_path}  or {schema} does not exist")
        raise SystemExit(1)

    except jsonschema.ValidationError as e:
        print(f"Schema validation failed for {file_path} ")
        print(e)
        raise SystemExit(1)
