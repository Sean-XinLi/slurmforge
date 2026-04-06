from __future__ import annotations

from typing import Any

from ....errors import ConfigContractError
from ...config.utils import ensure_dict
from ..models.metadata import GeneratedBy


def serialize_generated_by(value: GeneratedBy) -> dict[str, str]:
    return {
        "name": str(value.name),
        "version": str(value.version),
    }


def ensure_generated_by(value: Any, name: str = "generated_by") -> GeneratedBy:
    if isinstance(value, GeneratedBy):
        return value
    data = ensure_dict(value, name)
    record_name = str(data.get("name", "") or "").strip()
    record_version = str(data.get("version", "") or "").strip()
    if not record_name:
        raise ConfigContractError(f"{name}.name must be a non-empty string")
    if not record_version:
        raise ConfigContractError(f"{name}.version must be a non-empty string")
    return GeneratedBy(name=record_name, version=record_version)
