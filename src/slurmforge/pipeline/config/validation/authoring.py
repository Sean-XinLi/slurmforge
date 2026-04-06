from __future__ import annotations

from pathlib import Path

from ....errors import ConfigContractError
from .api import (
    AUTHORING_SCHEMA,
    validate_config_profile,
)
from .sweep_rules import validate_batch_scoped_sweep_paths, validate_declared_sweep_paths


def validate_authoring_source_cfg(cfg: dict, *, config_path: Path) -> None:
    if not isinstance(cfg, dict):
        raise ConfigContractError(f"{config_path}: top-level YAML must be a mapping")
    validate_config_profile(
        cfg,
        config_path=config_path,
        schema=AUTHORING_SCHEMA,
        required_keys=("project", "experiment_name", "run"),
    )
    sweep_spec = normalize_authoring_sweep_spec(cfg, config_path=config_path)
    validate_declared_sweep_paths(sweep_spec, config_path=config_path)
    validate_batch_scoped_sweep_paths(sweep_spec, config_path=config_path)


def normalize_authoring_sweep_spec(cfg: dict, *, config_path: Path):
    from ....sweep import normalize_sweep_config

    if not isinstance(cfg, dict):
        raise ConfigContractError(f"{config_path}: top-level YAML must be a mapping")
    validate_config_profile(
        cfg,
        config_path=config_path,
        schema=AUTHORING_SCHEMA,
        required_keys=("project", "experiment_name", "run"),
    )
    sweep_spec = normalize_sweep_config(cfg)
    validate_declared_sweep_paths(sweep_spec, config_path=config_path)
    validate_batch_scoped_sweep_paths(sweep_spec, config_path=config_path)
    return sweep_spec
