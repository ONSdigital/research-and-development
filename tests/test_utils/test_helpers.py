from src.utils.helpers import convert_formtype

def test_convert_formtype():
    """Test for convert_formtype()."""
    msg = "Converted formtype not as expected"
    assert convert_formtype("1") == "0001", msg
    assert convert_formtype("1.0") == "0001", msg
    assert convert_formtype("0001") == "0001", msg
    assert convert_formtype("6") == "0006", msg
    assert convert_formtype("6.0") == "0006", msg
    assert convert_formtype("0006") == "0006", msg
    assert convert_formtype(1) == "0001", msg
    assert convert_formtype("2") is None, msg
    assert convert_formtype("") is None, msg
    assert convert_formtype(None) is None, msg
