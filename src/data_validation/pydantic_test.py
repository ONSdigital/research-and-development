from datetime import date
from pydantic import BaseModel, ValidationError
from pydantic.types import StrictStr, StrictInt, StrictFloat

from src.data_validation.validation import create_data_dict
from src.utils.wrappers import exception_wrap


class DataTypes(BaseModel):

    snapshot_id: None = []
    reference: StrictInt = []
    period: StrictInt = []
    survey: StrictStr = []
    formid: StrictInt = []
    status: StrictStr = []
    statusencoded: StrictInt = []
    receiptdata: date = []
    lockedby: None = []
    lockeddate: None = []
    formtype: StrictStr = []
    checkletter: StrictStr = []
    frozensicoutdated: StrictInt = []
    rusicoutdated: StrictInt = []
    frozensic: StrictInt = []
    rusic: StrictInt = []
    frozenemployees: StrictInt = []
    employees: StrictInt = []
    frozenemployment: StrictInt = []
    employment: StrictInt = []
    frozenfteemployment: StrictFloat = []
    fteemployment: StrictFloat = []
    frozenturnover: StrictInt = []
    turnover: StrictInt = []
    enterprisereference: StrictInt = []
    wowenterprisereference: StrictInt = []
    cellnumber: StrictInt = []
    currency: StrictStr = []
    vatreference: StrictStr = []
    payereference: StrictStr = []
    companyregistrationnumber: StrictStr = []
    numberlivelocalunits: StrictInt = []
    numberlivevat: StrictInt = []
    numberlivepaye: StrictInt = []
    legalstatus: StrictInt = []
    reportingunitmarker: StrictStr = []
    region: StrictStr = []
    birthdate: date = []
    referencename: StrictStr = []
    referencepostcode: StrictStr = []
    tradingstyle: StrictStr = []
    selectiontype: StrictStr = []
    inclusionexclusion: None = []
    createdby: StrictStr = []
    createddate: date = []
    lastupdatedby: StrictStr = []
    lastupdateddate: date = []

    class Config:
        extra = "forbid"


@exception_wrap
def check_data_types():
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

    Raises:
        TypeError: If the value in the 'data_dict' is not of <class 'list'> type
        the a TypeError is raised. To check data the values must be passed as
        lists in the data_dict object.

    Returns:
        An object of user-defined <class 'DataTypes'>.
    """
    # Fetch the data. Returns a list with each element containing a dictionary
    # Each dictionary represents a 'row' in the data.
    data_list = create_data_dict()

    # Initialise an index to iterate over
    index = 0
    error_count = 0

    # Loop over the list containing the data
    while index < len(data_list):

        # Iterate through dictionary at each element in list
        for k, v in data_list[index].items():
            try:
                # Attempt to instantiate an object of the BaseModel type above
                DataType_obj = DataTypes(**dict({k: v}))
            except ValidationError as e:
                # Errors are caught when a type from the data doesn't match the
                # type specified in the BaseModel
                ValidationLogger.warning(f"Error on row: {index}: {e} \n")  # noqa
                error_count += 1

        index += 1

    return error_count, DataType_obj


check = check_data_types()
print(check)
