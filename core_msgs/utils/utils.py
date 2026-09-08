from typing import Any

import yaml


def load_config(path: str ) -> Any:
    """Load and parse a YAML configuration file."""
    try:
        with open(path, 'r') as file:
            return  yaml.safe_load(file)
    except:
        raise Exception("Config file not found")


def format_nested_strings(data, **kwargs):
    """
    Recursively searches a dictionary or list and formats any strings
    found using the provided keyword arguments.

    """
    if isinstance(data, dict):
        return {key: format_nested_strings(value, **kwargs) for key, value in data.items()}

    elif isinstance(data, list):
        return [format_nested_strings(item, **kwargs) for item in data]

    elif isinstance(data, str):
        return data.format_map(_LeaveUnmatched(kwargs))

    else:
        return data


class _LeaveUnmatched(dict):
    """dict subclass for str.format_map() that leaves an unknown
    placeholder as literal text (``{whatever}``) instead of raising
    KeyError, so format_nested_strings can be called more than once
    with different kwargs each time."""

    def __missing__(self, key):
        return "{" + key + "}"
