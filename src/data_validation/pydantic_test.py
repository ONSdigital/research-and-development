from src.data_validation.validation import data_types, create_data_dict
from pydantic import BaseModel
from pydantic.types import StrictStr  # , StrictInt, StrictBool, StrictFloat
from typing import Union, List

type_dict = data_types()
data_dict = create_data_dict("/ons/rdbe_dev/Frozen_Test_Data_multi-row_Matching.csv")


class DataTypes(BaseModel):

    cell_id: List[Union[int, str]] = []
    civ_or_def: List[Union[int, str]] = []
    current_sic: List[Union[int, str]] = []
    data_source: List[Union[int, str]] = []
    emp_other: List[None] = []
    emp_research: List[None] = []
    emp_technician: List[None] = []
    emp_total: List[None] = []
    employee_count: List[int] = []
    freeze_id: List[StrictStr] = []
    period: List[Union[int, str]] = []

    class Config:
        extra = "forbid"


for dKey, dVal in data_dict.items():
    if isinstance(dVal, list):
        try:
            test = DataTypes(**dict({dKey: dVal}))
            # print(f"First value: {dVal[0]}. Second value: {dVal[1]}.")
        except ValueError as e:
            print(f"{e} \n")

# print(type_dict)
# test = DataTypes(**data_dict)
