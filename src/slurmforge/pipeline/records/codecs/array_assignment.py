from __future__ import annotations

import copy
from typing import Any

from ..models.array_assignment import ArrayAssignment


def serialize_array_assignment(value: ArrayAssignment) -> dict[str, Any]:
    return {
        "group_index": None if value.group_index is None else int(value.group_index),
        "group_signature": str(value.group_signature),
        "grouping_fields": copy.deepcopy(value.grouping_fields),
        "group_reason": str(value.group_reason),
    }


def ensure_array_assignment(value: Any, name: str = "array_assignment") -> ArrayAssignment:
    if isinstance(value, ArrayAssignment):
        return value
    if value is None:
        return ArrayAssignment()
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be a mapping")
    group_index_raw = value.get("group_index")
    return ArrayAssignment(
        group_index=None if group_index_raw is None else int(group_index_raw),
        group_signature=str(value.get("group_signature", "")),
        grouping_fields=dict(value.get("grouping_fields") or {}),
        group_reason=str(value.get("group_reason", "")),
    )
