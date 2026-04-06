from __future__ import annotations

from typing import Any

from ...errors import ConfigContractError


def normalize_bool(value: Any, *, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "on"}:
            return True
        if text in {"false", "0", "no", "off"}:
            return False
    raise ConfigContractError(f"{name} must be a boolean")


def normalize_optional_bool(value: Any, *, name: str) -> bool | None:
    if value is None:
        return None
    return normalize_bool(value, name=name)
