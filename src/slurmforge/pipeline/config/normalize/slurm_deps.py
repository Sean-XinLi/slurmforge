"""Validation and normalization helpers for Slurm dependency specifications."""
from __future__ import annotations

from typing import Any, Sequence

from ....errors import ConfigContractError


SLURM_DEPENDENCY_KINDS: tuple[str, ...] = (
    "after",
    "afterany",
    "afterok",
    "afternotok",
)


def _supported_kinds_text(allowed_kinds: Sequence[str]) -> str:
    return ", ".join(sorted(set(str(kind) for kind in allowed_kinds)))


def normalize_dependency_kind(
    value: Any,
    *,
    field_name: str,
    allowed_kinds: Sequence[str] = SLURM_DEPENDENCY_KINDS,
) -> str:
    if not isinstance(value, str):
        raise ConfigContractError(f"{field_name} must be a string when provided")
    normalized = value.strip().lower()
    if normalized not in allowed_kinds:
        raise ConfigContractError(f"{field_name} must be one of: {_supported_kinds_text(allowed_kinds)}")
    return normalized


def normalize_dependency_mapping(
    value: Any,
    *,
    field_name: str,
    allowed_kinds: Sequence[str] = SLURM_DEPENDENCY_KINDS,
) -> dict[str, list[str]]:
    if value is None or value == "":
        return {}
    if not isinstance(value, dict):
        raise ConfigContractError(f"{field_name} must be a mapping when provided")

    allowed = tuple(str(kind) for kind in allowed_kinds)
    unknown = sorted(str(key) for key in value if str(key) not in allowed)
    if unknown:
        raise ConfigContractError(
            f"{field_name} contains unsupported keys {unknown}; supported keys: {sorted(allowed)}"
        )

    normalized: dict[str, list[str]] = {}
    for dep_kind in allowed:
        raw_values = value.get(dep_kind)
        if raw_values is None or raw_values == "":
            continue
        if not isinstance(raw_values, list):
            raise ConfigContractError(f"{field_name}.{dep_kind} must be a list")
        normalized_values: list[str] = []
        seen_values: set[str] = set()
        for item in raw_values:
            normalized_item = str(item).strip()
            if not normalized_item:
                raise ConfigContractError(f"{field_name}.{dep_kind} must contain only non-empty values")
            if normalized_item in seen_values:
                continue
            seen_values.add(normalized_item)
            normalized_values.append(normalized_item)
        if normalized_values:
            normalized[dep_kind] = normalized_values
    return normalized
