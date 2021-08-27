
def wrap_list(value):
    wrapped_list = value if isinstance(value, (list, tuple)) else [value]
    return list(filter(None, wrapped_list))