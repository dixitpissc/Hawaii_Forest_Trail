
# utils/cleaning_utils.py
import pandas as pd
from typing import Any

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


__all__ = ["is_filled", "build_ref", "deep_clean"]

def is_filled(value: Any) -> bool:
    """
    Check whether a value is meaningfully filled (not None / NaN / empty string).

    Args:
        value: Any type of input value.

    Returns:
        bool: True if the value is non-null, non-NaN, and not an empty string.
    """
    if value is None:
        return False

    # Handle NaN / NaT safely
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass

    # Handle empty string
    if isinstance(value, str) and value.strip() == "":
        return False

    return True


def build_ref(value: Any) -> dict | None:
    """
    Build a QBO-style {"value": "..."} reference only if the input value is filled.

    Args:
        value: The input value to convert.

    Returns:
        dict | None: {"value": str(value)} if filled, else None.
    """
    return {"value": str(value)} if is_filled(value) else None


def deep_clean(obj: Any) -> Any:
    """
    Recursively remove None, empty strings, and empty dicts/lists from a structure.
    Useful for sanitizing QBO payloads or nested JSON structures before posting.

    Args:
        obj: A dict, list, or primitive value.

    Returns:
        Cleaned version of the same type, with empty/null fields stripped.
    """
    if isinstance(obj, dict):
        cleaned = {k: deep_clean(v) for k, v in obj.items()}
        return {
            k: v for k, v in cleaned.items()
            if not (
                v is None
                or v == ""
                or (isinstance(v, (dict, list)) and len(v) == 0)
            )
        }

    elif isinstance(obj, list):
        cleaned_list = [deep_clean(v) for v in obj]
        return [
            v for v in cleaned_list
            if not (
                v is None
                or v == ""
                or (isinstance(v, (dict, list)) and len(v) == 0)
            )
        ]

    else:
        return obj
