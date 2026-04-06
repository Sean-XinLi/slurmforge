from __future__ import annotations

from typing import Any

from ..models import EnvConfig


def serialize_env_config(config: EnvConfig) -> dict[str, Any]:
    return {
        "modules": [str(item) for item in config.modules],
        "conda_activate": str(config.conda_activate),
        "venv_activate": str(config.venv_activate),
        "extra_env": {str(key): str(value) for key, value in config.extra_env.items()},
    }
