def remove_empty(l):
    '''Remove items which evaluate to False (such as empty strings) from the input list.'''
    return [x for x in l if x]

def get_number_from_string(string, number_type=float):
    '''Remove commas from the input string and parse as a number'''
    return number_type(string.replace(',', ''))

def take_first_two(values):
    if len(values) >= 2:
        return [values[0], values[1]]
    else:
        return [values[0]]