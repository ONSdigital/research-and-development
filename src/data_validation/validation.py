import os


def check_file_exists(filePath="./src/data_validation/validation.py") -> bool:
    """Checks if file exists and is non-empty

    Keyword Arguments:
        filePath -- Relative path to file
        (default: {"./src/data_validation/validation.py"})

    Returns:
        A bool: boolean value is True if file exists and is non-empty,
        False otherwise.
    """
    output = False

    fileExists = os.path.exists(filePath)
    if fileExists:
        fileSize = os.path.getsize(filePath)

    if fileExists and fileSize > 0:
        output = True
    return output
