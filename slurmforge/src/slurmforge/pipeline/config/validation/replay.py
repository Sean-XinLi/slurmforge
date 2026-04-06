from __future__ import annotations

from pathlib import Path

from ....errors import ConfigContractError
from ..constants import REPLAY_MODEL_CATALOG_KEY
from ..utils import resolve_config_label, resolve_spec_project_root
from .api import REPLAY_SCHEMA, validate_config_profile


def validate_replay_config(
    replay_cfg: dict,
    *,
    config_path: Path | None = None,
    config_label: str | None = None,
    project_root: Path,
) -> tuple[str, Path]:
    config_ref = resolve_config_label(
        config_path=config_path,
        config_label=config_label,
        default="<replay config>",
    )
    resolved_project_root = resolve_spec_project_root(config_path, project_root)
    if not isinstance(replay_cfg, dict):
        raise ConfigContractError(f"{config_ref}: replay config must be a mapping")
    validate_config_profile(
        replay_cfg,
        config_path=config_ref,
        schema=REPLAY_SCHEMA,
        required_keys=("project", "experiment_name", "run", REPLAY_MODEL_CATALOG_KEY),
    )
    return config_ref, resolved_project_root
