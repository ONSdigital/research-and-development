from src.data_validation.validation import create_data_dict
from pydantic import BaseModel, ValidationError
from pydantic.types import StrictStr, StrictInt, StrictFloat
from typing import List
from datetime import datetime
import pandas as pd


class DataTypes(BaseModel):

    snapshot_id: List[None] = []
    reference: List[StrictInt] = []
    period: List[StrictInt] = []
    survey: List[StrictStr] = []
    formid: List[StrictInt] = []
    status: List[StrictStr] = []
    statusencoded: List[StrictInt] = []
    receiptdata: List[datetime] = []
    lockedby: List[pd.NA] = []
    lockeddate: List[pd.NA] = []
    formtype: List[StrictStr] = []
    checkletter: List[StrictStr] = []
    frozensicoutdated: List[StrictInt] = []
    rusicoutdated: List[StrictInt] = []
    frozensic: List[StrictInt] = []
    rusic: List[StrictInt] = []
    frozenemployees: List[StrictInt] = []
    employees: List[StrictInt] = []
    frozenemployment: List[StrictInt] = []
    employment: List[StrictInt] = []
    frozenfteemployment: List[StrictFloat] = []
    fteemployment: List[StrictFloat] = []
    frozenturnover: List[StrictInt] = []
    turnover: List[StrictInt] = []
    enterprisereference: List[StrictInt] = []
    wowenterprisereference: List[StrictInt] = []
    cellnumber: List[StrictInt] = []
    currency: List[StrictStr] = []
    vatreference: List[StrictStr] = []
    payereference: List[StrictStr] = []
    companyregistrationnumber: List[StrictStr] = []
    numberlivelocalunits: List[StrictInt] = []
    numberlivevat: List[StrictInt] = []
    numberlivepaye: List[StrictInt] = []
    legalstatus: List[StrictInt] = []
    reportingunitmarker: List[StrictStr] = []
    region: List[StrictStr] = []
    birthdate: List[datetime] = []
    referencename: List[StrictStr] = []
    referencepostcode: List[StrictStr] = []
    tradingstyle: List[StrictStr] = []
    selectiontype: List[StrictStr] = []
    inclusionexclusion: List[pd.NA] = []
    createdby: List[StrictStr] = []
    createddate: List[datetime] = []
    lastupdatedby: List[StrictStr] = []
    lastupdateddate: List[datetime] = []

    class Config:
        extra = "forbid"


def check_data_types(
    dataFile: str = "/ons/rdbe_dev/Frozen_Test_Data_multi-row_Matching.csv",
):
    """Takes the data from a csv file, parses that into a dictionary with
    key: value pairs for each variable. The values for each key should be
    a list, each element corresponding to a row in the csv file. A
    TypeError is raise if a value from the key: value pairs is not of the
    <class 'list'> type. An instance of the user defined 'DataTypes' class
    is returned with attributes populated by the data. Validation errors
    are raised if data does match the allows types given in the 'DataTypes'
    class. Validation errors are also raised if data passed to the function
    contains column headers not defined in the 'DataTypes' class.

    Keyword Arguments:
        dataFile -- path to data file
        (default: {"/ons/rdbe_dev/Frozen_Test_Data_multi-row_Matching.csv"})

    Raises:
        TypeError: If the value in the 'data_dict' is not of <class 'list'> type
        the a TypeError is raised. To check data the values must be passed as
        lists in the data_dict object.

    Returns:
        An object of user-defined <class 'DataTypes'>.
    """
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
