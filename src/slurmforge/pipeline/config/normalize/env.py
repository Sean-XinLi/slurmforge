from __future__ import annotations

import re
from typing import Any

from ....errors import ConfigContractError
from ...utils import deep_merge
from ..runtime import DEFAULT_ENV, EnvConfig
from ..utils import ensure_dict
from .shared import ensure_normalized_config


def normalize_env(cfg: dict[str, Any]) -> EnvConfig:
    merged = deep_merge(DEFAULT_ENV, ensure_dict(cfg, "env"))
    modules = [str(x) for x in list(merged.get("modules") or [])]
    extra_env = {str(key): str(value) for key, value in dict(merged.get("extra_env") or {}).items()}
    for key in extra_env.keys():
        if not isinstance(key, str) or not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
            raise ConfigContractError(f"env.extra_env key `{key}` is invalid; expected shell variable name")
    return EnvConfig(
        modules=modules,
        conda_activate=str(merged["conda_activate"]),
        venv_activate=str(merged["venv_activate"]),
        extra_env=extra_env,
    )


def ensure_env_config(value: Any, name: str = "env") -> EnvConfig:
    return ensure_normalized_config(
        value,
        name=name,
        config_type=EnvConfig,
        normalizer=normalize_env,
    )
