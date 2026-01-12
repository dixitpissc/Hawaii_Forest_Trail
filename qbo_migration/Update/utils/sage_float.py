def safe_float(val, default=0.0):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default
