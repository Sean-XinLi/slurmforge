from __future__ import annotations

from typing import Any


def read_schema_version(payload: dict[str, Any], *, default: int = 1) -> int:
    """Extract and coerce schema_version from a serialized payload.

    Handles ``None`` values and missing keys gracefully, always returning a
    valid integer.
    """
    return int(payload.get("schema_version", default) or default)
