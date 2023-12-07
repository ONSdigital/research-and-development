def postcode_topup(mystr: str, target_len: int = 8) -> str:
    """Regulates the number of spaces between the first and the second part of
    a postcode, so that the total length is 8 characters.
    Brings all letters to upper case, in line with the mapper.
    Splits the postcode string into parts, separated by any number of spaces.
    If there are two or more parts, the first two parts are used.
    The third and following parts, if present, are ignored.
    Calculates how many spaces are needed so that the total length is 8.
    If the total length of part 1 and part 2 is already 8, no space will be
    inserted.
    If their total length is more than 8, joins part 1 and part 2 without
    spaces and cuts the tail on the right.
    If there is only one part, keeps the first 8 characters and tops it up with
    spaces on the right if needed.
    Empty input string would have zero parts and will return a string of
    eight spaces.

    Args:
        mystr (str): Input postcode.
        target_len (int): The desired length of the postcode after topping up.

    Returns:
        str: The postcode topped up to the desired number of characters.
    """

    mystr = mystr.upper()
    parts = mystr.split()
    if len(parts) >= 2:
        part1 = parts[0]
        part2 = parts[1]
        num_spaces = target_len - len(part1) - len(part2)
        if num_spaces >= 0:
            return part1 + " " * num_spaces + part2
        else:
            return (part1 + part2)[:target_len]
    else:
        return mystr[:target_len].ljust(target_len, " ")
