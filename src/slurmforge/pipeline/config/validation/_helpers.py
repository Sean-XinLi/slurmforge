"""
Shared helpers for the validation layer.

All three check modules (completeness, correctness, advisory) import from here
so that time-parsing logic and nested-dict traversal are defined exactly once.
"""
from __future__ import annotations

import re
from typing import Any

__all__ = [
    "MISSING",
    "get_nested",
    "is_valid_time_limit",
    "parse_time_seconds",
]

MISSING: Any = object()
"""Sentinel returned by :func:`get_nested` when *default* is not overridden
and you need to distinguish 'key absent' from 'key present but None'."""

# Matches [D-]HH:MM:SS
# group(1) = day count (digits only, no dash), group(2) = HH, group(3) = MM, group(4) = SS
_TIME_LIMIT_RE = re.compile(r"^(?:(\d+)-)?(\d{1,2}):(\d{2}):(\d{2})$")


def get_nested(cfg: dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Traverse a nested dict by key path; return *default* when any key is absent.

    When *default* is :data:`MISSING`, the lookup uses strict containment
    (``key in cur``) so that a present-but-``None`` value is returned as-is
    rather than being confused with an absent key.
    """
    cur: Any = cfg
    for key in keys:
        if not isinstance(cur, dict):
            return default
        if default is MISSING:
            if key not in cur:
                return default
            cur = cur[key]
        else:
            cur = cur.get(key, default)
    return cur


def is_valid_time_limit(value: str) -> bool:
    """Return True if *value* matches HH:MM:SS or D-HH:MM:SS."""
    return bool(_TIME_LIMIT_RE.match(value.strip()))


def parse_time_seconds(value: str) -> int | None:
    """Convert a time_limit string to total seconds; return None if unparseable."""
    m = _TIME_LIMIT_RE.match(value.strip())
    if not m:
        return None
    days = int(m.group(1) or 0)
    return days * 86400 + int(m.group(2)) * 3600 + int(m.group(3)) * 60 + int(m.group(4))
