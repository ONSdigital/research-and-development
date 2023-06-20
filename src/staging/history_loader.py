"""Functions relating to loading historic data"""



def history_years(current, back_history):
    """Gets the year or years to load historic data for"""
    
    if back_history >0:
        # create a generator to yield the years to load
        for i in range(back_history):
            yield current -1
            current -= 1
    else:
        print("No historic data to load")
        return None

