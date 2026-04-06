from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Sequence

import yaml

from ....errors import ConfigContractError
from ....sweep import deep_set, parse_override


def load_authoring_source_cfg(
    config_path: Path,
    *,
    cli_overrides: Sequence[str],
) -> tuple[Path, dict[str, Any]]:
    resolved_config_path = config_path.expanduser().resolve()
    try:
        cfg = yaml.safe_load(resolved_config_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ConfigContractError(f"Failed to parse YAML config `{resolved_config_path}`: {exc}") from exc

    if cfg is None:
        cfg = {}
    if not isinstance(cfg, dict):
        raise ConfigContractError(f"Top-level YAML document must be a mapping in `{resolved_config_path}`")

    loaded_cfg = copy.deepcopy(cfg)
    for item in cli_overrides:
        try:
            key, value = parse_override(item)
        except Exception as exc:
            raise ConfigContractError(f"Failed to parse --set override `{item}`: {exc}") from exc
        deep_set(loaded_cfg, key, value)
    return resolved_config_path, loaded_cfg
