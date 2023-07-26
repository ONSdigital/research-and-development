import toml
import pandas as pd
import logging


# from src.utils.wrappers import time_logger_wrap, exception_wrap

datatype_logger = logging.getLogger(__name__)


def validate_json_shcema(data: pd.DataFrame, schema: str):

    try:

        with open(schema) as f:
            schema_dict = toml.load(f)

        # print(data.dtypes)
        # print(data.info())
    except FileNotFoundError:
        print(f"The file  {schema} does not exist")
        raise SystemExit(1)

    except Exception as nested_exception:
        print(f"Nested Exception: {nested_exception}")

        data_types_dict = {
            column_nm: schema_dict[column_nm]["Deduced_Data_Type"]
            for column_nm in schema_dict.keys()
        }
        # data.astype(data_types_dict)
        for column in data.columns:
            try:
                data[column].astype(data_types_dict[column])
                print(data.dtypes)

            except toml.TomlDecodeError as e:
                print(f"tomldecode error:{e}")
