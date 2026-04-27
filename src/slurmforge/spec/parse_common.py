from __future__ import annotations

from typing import Any

from ..errors import ConfigContractError


def reject_unknown_keys(data: dict[str, Any], *, allowed: set[str], name: str) -> None:
    unknown = sorted(set(data) - allowed)
    if unknown:
        joined = ", ".join(str(item) for item in unknown)
        raise ConfigContractError(f"Unsupported keys under `{name}`: {joined}")


def require_mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigContractError(f"`{name}` must be a mapping")
    return value


def optional_mapping(value: Any, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    return require_mapping(value, name)
