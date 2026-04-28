from __future__ import annotations

import re
from typing import Any

_PLAIN_SCALAR = re.compile(r"^[A-Za-z0-9_./$*?-][A-Za-z0-9_./$*?-]*$")
_NUMBER_LIKE = re.compile(r"^[+-]?(?:\d+|\d+\.\d+)(?:[eE][+-]?\d+)?$")


def scalar(value: Any) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        if not value:
            return "[]"
        return "[" + ", ".join(scalar(item) for item in value) + "]"
    if isinstance(value, dict):
        if not value:
            return "{}"
        return "{" + ", ".join(f"{key}: {scalar(item)}" for key, item in value.items()) + "}"
    text = str(value)
    if (
        _PLAIN_SCALAR.match(text)
        and text.lower() not in {"true", "false", "null"}
        and not _NUMBER_LIKE.match(text)
    ):
        return text
    escaped = text.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'
