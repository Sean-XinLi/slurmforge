from __future__ import annotations

from typing import Any

from ..errors import ConfigContractError


def require_mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigContractError(f"`{name}` must be a mapping")
    return value


def optional_mapping(value: Any, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    return require_mapping(value, name)
