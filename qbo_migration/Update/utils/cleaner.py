# def remove_null_fields(obj):
#     if isinstance(obj, dict):
#         return {k: remove_null_fields(v) for k, v in obj.items() if v is not None}
#     elif isinstance(obj, list):
#         return [remove_null_fields(i) for i in obj if i is not None]
#     else:
#         return obj

def remove_null_fields(obj):
    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            if v is not None:
                cleaned[k] = remove_null_fields(v)
        return cleaned

    elif isinstance(obj, list):
        return [remove_null_fields(i) for i in obj if i is not None]

    elif isinstance(obj, str):
        # Auto-convert numeric strings like "6266.3" to float
        try:
            if '.' in obj:
                return float(obj)
            else:
                return int(obj)
        except ValueError:
            return obj

    else:
        return obj
