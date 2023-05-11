import os


def check_file_exists(filename: str, filepath: str = "./data/raw/") -> bool:
    """Checks if file exists and is non-empty

    Keyword Arguments:
        filePath -- Relative path to file
        (default: {"./src/data_validation/validation.py"})

    Returns:
        A bool: boolean value is True if file exists and is non-empty,
        False otherwise.
    """
    output = False

    fileExists = os.path.exists(filepath + filename)
    if fileExists:
        fileSize = os.path.getsize(filepath + filename)

    if fileExists and fileSize > 0:
        output = True

    return output
