from __future__ import annotations

from typing import Any

from ....errors import ConfigContractError
from ...utils import deep_merge
from ..runtime import DEFAULT_VALIDATION, ValidationConfig
from ..utils import ensure_dict
from .shared import ensure_normalized_config


def normalize_validation(cfg: dict[str, Any]) -> ValidationConfig:
    raw_cfg = ensure_dict(cfg, "validation")
    merged = deep_merge(DEFAULT_VALIDATION, raw_cfg)
    aliases = {"strict": "error"}

    def _normalize_policy(field_name: str, default: str) -> str:
        raw = merged.get(field_name, default)
        if not isinstance(raw, str):
            raise ConfigContractError(f"validation.{field_name} must be one of: off, warn, error")
        normalized = aliases.get(raw.strip().lower() or default, raw.strip().lower() or default)
        if normalized not in {"off", "warn", "error"}:
            raise ConfigContractError(f"validation.{field_name} must be one of: off, warn, error")
        return normalized

    return ValidationConfig(
        cli_args=_normalize_policy("cli_args", "warn"),
        topology_errors=_normalize_policy("topology_errors", "error"),
        resource_warnings=_normalize_policy("resource_warnings", "warn"),
        runtime_preflight=_normalize_policy("runtime_preflight", "error"),
    )


def ensure_validation_config(value: Any, name: str = "validation") -> ValidationConfig:
    return ensure_normalized_config(
        value,
        name=name,
        config_type=ValidationConfig,
        normalizer=normalize_validation,
    )
