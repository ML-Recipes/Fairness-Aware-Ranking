def to_float(data):
    
    result = None
    if data == "nan":
        result = 0.0
    elif data is None:
        result = 0.0
    else:
        result = data

    return result

def to_integer(data):
    
    result = None
    if data == "nan":
        result = 0
    elif data is None:
        result = 0
    else:
        result = data
    
    return result

def to_string(data):
    
    result = None
    if data == "nan":
        result = ""
    elif data is None:
        result = ""
    else:
        result = data
    
    return result