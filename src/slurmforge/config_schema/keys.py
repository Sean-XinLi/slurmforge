from __future__ import annotations

import re
from functools import lru_cache
from typing import Any

from ..errors import ConfigContractError
from ..config_contract.workflows import SUPPORTED_STAGE_KEYS
from .fields import CONFIG_FIELDS

_STAGE_KEYS = SUPPORTED_STAGE_KEYS
_DYNAMIC_PARENT_KEYS = frozenset(
    {
        "environments",
        "hardware.gpu_types",
        "runtime.user",
        "runs.axes",
        "runs.cases[].set",
        "runs.cases[].axes",
        "stages.*.entry.args",
        "stages.*.inputs",
        "stages.*.outputs",
        "runtime.user.*.env",
        "environments.*.env",
    }
)


def allowed_top_level_keys() -> set[str]:
    return set(allowed_keys("<root>"))


def allowed_stage_keys() -> set[str]:
    return set(_STAGE_KEYS)


def allowed_keys(parent_path: str) -> set[str]:
    canonical = canonical_parent(parent_path)
    if canonical == "stages":
        return set(_STAGE_KEYS)
    children = set(_allowed_key_map().get(canonical, frozenset()))
    if not canonical.endswith("[]"):
        children = {child for child in children if not child.endswith("[]")}
    return children


def is_dynamic_parent(parent_path: str) -> bool:
    return canonical_parent(parent_path) in _DYNAMIC_PARENT_KEYS


def reject_unknown_config_keys(
    data: dict[str, Any], *, parent: str, name: str | None = None
) -> None:
    if is_dynamic_parent(parent):
        return
    allowed = allowed_keys(parent)
    unknown = sorted(set(data) - allowed)
    if unknown:
        joined = ", ".join(str(item) for item in unknown)
        raise ConfigContractError(
            f"Unsupported keys under `{name or parent}`: {joined}"
        )


def canonical_parent(parent_path: str) -> str:
    if parent_path in {"", "<root>"}:
        return "<root>"
    parent_path = re.sub(r"\[\d+\]", "[]", parent_path)
    parts = parent_path.split(".")
    if not parts:
        return parent_path
    if parts[0] == "stages":
        parts = _canonical_stage_parts(parts)
    elif parts[0] == "hardware" and len(parts) >= 3 and parts[1] == "gpu_types":
        parts[2] = "*"
    elif parts[0] == "environments" and len(parts) >= 2:
        parts[1] = "*"
    elif parts[0] == "runtime" and len(parts) >= 3 and parts[1] == "user":
        parts[2] = "*"
    elif parts[:2] == ["runs", "cases"] and len(parts) >= 2:
        parts[1] = "cases[]"
    return ".".join(parts)


def _canonical_stage_parts(parts: list[str]) -> list[str]:
    if len(parts) >= 2 and parts[1] in _STAGE_KEYS:
        parts[1] = "*"
    if len(parts) >= 4 and parts[2] in {"inputs", "outputs"}:
        parts[3] = "*"
    if len(parts) >= 4 and parts[2] == "before":
        parts[3] = "before[]"
    return parts


@lru_cache(maxsize=1)
def _allowed_key_map() -> dict[str, frozenset[str]]:
    keys: dict[str, set[str]] = {"<root>": set()}
    for field in CONFIG_FIELDS:
        _add_path(keys, field.path)
    for parent in _DYNAMIC_PARENT_KEYS:
        keys.setdefault(parent, set())
    return {parent: frozenset(children) for parent, children in keys.items()}


def _add_path(keys: dict[str, set[str]], path: str) -> None:
    parts = path.split(".")
    canonical_parts = _canonical_schema_parts(parts)
    for index, child in enumerate(canonical_parts):
        parent = ".".join(canonical_parts[:index]) if index else "<root>"
        keys.setdefault(parent, set()).add(child)
        keys.setdefault(".".join(canonical_parts[: index + 1]), set())


def _canonical_schema_parts(parts: list[str]) -> list[str]:
    if parts and parts[0] == "stages":
        parts = _canonical_stage_parts(list(parts))
    return parts
