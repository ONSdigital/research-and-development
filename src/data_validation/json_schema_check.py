import jsonschema

from jsonschema import validate
from src.utils.helpers import Config_settings
from src.utils.hdfs_mods import hdfs_load_json as read_data

conf_obj = Config_settings()
config = conf_obj.config_dict
config_paths = config["paths"]
snapshot_path = config_paths["snapshot_path"]  # Taken from config file

# Describe what kind of json you expect.
studentSchema = {
    "type": "object",
    "properties": {
        "reference": {"type": "int"},
        "rollnumber": {"type": "number"},
        "marks": {"type": "number"},
    },
}


def validateJson(jsonData):
    try:
        validate(instance=jsonData, schema=studentSchema)
    except jsonschema.exceptions.ValidationError as err:
        return err
    return True


# Convert json to python object.
# jsonData = json.loads('{"name": "jane doe", "rollnumber": "25", "marks": 72}')
snapdata = read_data(snapshot_path)
jsonData = snapdata["contributors"]

# validate it
isValid = validateJson(jsonData)
if isValid:
    print("Given JSON data is Valid")
else:
    print("Given JSON data is InValid")
