def clean_payload(data):
    """
    Recursively remove None, empty strings, and 'null' (case-insensitive)
    from dicts, lists, and nested structures.
    """
    if isinstance(data, dict):
        cleaned = {}
        for k, v in data.items():
            cv = clean_payload(v)
            if cv is None:
                continue
            if isinstance(cv, str) and cv.strip().lower() == "null":
                continue
            if isinstance(cv, str) and cv.strip() == "":
                continue
            cleaned[k] = cv
        return cleaned

    elif isinstance(data, list):
        return [clean_payload(v) for v in data if not (
            v is None or
            (isinstance(v, str) and (v.strip() == "" or v.strip().lower() == "null"))
        )]

    else:
        return data
