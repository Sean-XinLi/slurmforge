from __future__ import annotations

from pathlib import Path
from typing import Any

from ....errors import ConfigContractError
from ..utils import ensure_dict
from .definitions import AUTHORING_SCHEMA, REPLAY_SCHEMA
from .sections import SECTION_VALIDATORS
from .traversal import validate_mapping_schema


def validate_config_profile(
    cfg: dict[str, Any],
    *,
    config_path: str | Path,
    schema,
    required_keys: tuple[str, ...],
) -> None:
    validate_mapping_schema(cfg, name=str(config_path), schema=schema)
    missing = [key for key in required_keys if key not in cfg]
    if missing:
        raise ConfigContractError(f"{config_path}: missing required top-level keys {missing}")
    for key, validator in SECTION_VALIDATORS.items():
        if key not in schema.fields or key not in cfg:
            continue
        validator(ensure_dict(cfg.get(key), f"{config_path}: {key}"), name=f"{config_path}: {key}")


__all__ = [
    "AUTHORING_SCHEMA",
    "REPLAY_SCHEMA",
    "validate_config_profile",
]
