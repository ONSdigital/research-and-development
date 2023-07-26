# import toml
# import pandas as pd
# import logging

# # from src.utils.wrappers import time_logger_wrap, exception_wrap

# datatype_logger = logging.getLogger(__name__)


# def validate_json_shcema(data: pd.DataFrame, schema: str):

#     try:

#         with open(schema) as f:
#             schema_dict = toml.load(f)

#         print(data.dtypes)
#         # print(data.info())

#         for column, dtype in schema_dict.items():
#             # print(f'column{column}:{dtype}')
#             d_type = dtype.get("Deduced_Data_Type")
#             print(d_type)
#             if column not in data.columns:
#                 raise ValueError(f"column{column} not in data frame")

#             d_type = dtype.get("Deduced_Data_Type")

#             if d_type == "None":
#                 data[column] = data[column].replace({"None": pd.NA})

#             elif d_type == "Datetime":
#                 data[column] = pd.to_datetime(data[column], errors="coerce")

#             elif d_type == "pandas.NA":
#                 data[column] = data[column].replace({"pandas.NA": pd.NA})

#             elif d_type == "int":
#                 data[column] = pd.to_numeric(data[column], errors="coerce").astype(
#                     "Int64"
#                 )

#             elif d_type == "float":
#                 data[column] = pd.to_numeric(data[column], errors="coerce")

#             elif d_type == "category":
#                 data[column] = data[column].astype(str)

#             elif d_type == "str":
#                 data[column] = data[column].astype(str)

#             else:

#                 raise ValueError(
#                     f"Invalid data type {dtype}, specifed col {column}, in schema"
#                 )

#         return data

#     except FileNotFoundError:
#         print(f"The file  {schema} does not exist")
#         raise SystemExit(1)

#     except Exception as e:
#         print("Schema validation failed for")
#         print(e)
#         raise SystemExit(1)
