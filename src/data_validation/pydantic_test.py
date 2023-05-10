from src.data_validation.validation import create_data_dict
from pydantic import BaseModel, ValidationError
from pydantic.types import StrictStr, StrictInt, StrictBool
from typing import Union, List


class DataTypes(BaseModel):

    cell_id: List[Union[StrictInt, StrictStr]] = []
    civ_or_def: List[Union[StrictInt, StrictStr]] = []
    current_sic: List[Union[StrictInt, StrictStr]] = []
    data_source: List[Union[StrictInt, StrictStr]] = []
    emp_other: List[None] = []
    emp_researcher: List[None] = []
    emp_technician: List[None] = []
    emp_total: List[None] = []
    employee_count: List[StrictInt] = []
    foreign_owner: List[Union[StrictInt, StrictStr]] = []
    form_status: List[Union[StrictInt, StrictStr]] = []
    form_type: List[Union[StrictInt, StrictStr]] = []
    freeze_id: List[StrictStr] = []
    headcount_oth_f: List[StrictInt] = []
    headcount_oth_m: List[StrictInt] = []
    headcount_res_f: List[StrictInt] = []
    headcount_res_m: List[StrictInt] = []
    headcount_tec_f: List[StrictInt] = []
    headcount_tec_m: List[StrictInt] = []
    headcount_total: List[StrictInt] = []
    period: List[Union[StrictInt, StrictStr]] = []
    period_contributor_id: List[Union[StrictInt, StrictStr]] = []
    period_year: List[Union[StrictInt, StrictStr]] = []
    product_group: List[Union[StrictInt, StrictStr]] = []
    ru_ref: List[Union[StrictInt, StrictStr]] = []
    sizeband: List[Union[StrictInt, StrictStr]] = []
    wowentref: List[Union[StrictInt, StrictStr]] = []
    q202: List[StrictInt] = []
    q203: List[StrictInt] = []
    q204: List[StrictInt] = []
    q205: List[StrictInt] = []
    q206: List[StrictInt] = []
    q207: List[StrictInt] = []
    q208: List[StrictInt] = []
    q209: List[StrictInt] = []
    q210: List[StrictInt] = []
    q211: List[StrictInt] = []
    q212: List[StrictInt] = []
    q213: List[StrictInt] = []
    q214: List[StrictInt] = []
    q215: List[StrictInt] = []
    q216: List[StrictInt] = []
    q217: List[StrictInt] = []
    q218: List[StrictInt] = []
    q219: List[StrictInt] = []
    q220: List[StrictInt] = []
    q221: List[StrictInt] = []
    q222: List[StrictInt] = []
    q223: List[StrictInt] = []
    q224: List[None] = []
    q225: List[StrictInt] = []
    q226: List[StrictInt] = []
    q227: List[StrictInt] = []
    q228: List[StrictInt] = []
    q229: List[StrictInt] = []
    q230: List[StrictInt] = []
    q231: List[StrictInt] = []
    q232: List[StrictInt] = []
    q233: List[StrictInt] = []
    q234: List[StrictInt] = []
    q235: List[StrictInt] = []
    q236: List[StrictInt] = []
    q237: List[StrictInt] = []
    q238: List[None] = []
    q239: List[StrictInt] = []
    q240: List[StrictInt] = []
    q241: List[StrictInt] = []
    q242: List[StrictInt] = []
    q243: List[StrictInt] = []
    q244: List[StrictInt] = []
    q245: List[StrictInt] = []
    q246: List[StrictInt] = []
    q247: List[StrictInt] = []
    q248: List[StrictInt] = []
    q249: List[StrictInt] = []
    q250: List[StrictInt] = []
    q251: List[StrictBool] = []
    q252: List[None] = []
    q253: List[None] = []
    q254: List[None] = []
    q255: List[None] = []
    q256: List[None] = []
    q257: List[None] = []
    q258: List[None] = []
    q302: List[StrictBool] = []
    q303: List[StrictInt] = []
    q304: List[StrictInt] = []
    q305: List[StrictInt] = []
    q307: List[StrictBool] = []
    q308: List[StrictBool] = []
    q309: List[StrictBool] = []
    q713: List[StrictBool] = []
    q714: List[StrictBool] = []

    class Config:
        extra = "forbid"


# type_dict = data_types()
# data_dict = create_data_dict("/ons/rdbe_dev/Frozen_Test_Data_multi-row_Matching.csv")


def check_data_types(
    dataFile: str = "/ons/rdbe_dev/Frozen_Test_Data_multi-row_Matching.csv",
):

    data_dict = create_data_dict(dataFile)

    for dKey, dVal in data_dict.items():
        if isinstance(dVal, list):
            try:
                DataType_obj = DataTypes(**dict({dKey: dVal}))
                # print(f"First value: {dVal[0]}. Second value: {dVal[1]}.")
            except ValidationError as e:
                print(f"{e} \n")
        else:
            raise TypeError(f"Value for {dKey} is not of <class 'list'> type.")

    return DataType_obj


check = check_data_types()

# print(type_dict)
# test = DataTypes(**data_dict)
