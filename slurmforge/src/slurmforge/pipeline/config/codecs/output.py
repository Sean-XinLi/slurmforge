from __future__ import annotations

import copy
from typing import Any

from ..models.output import OutputConfigSpec


def serialize_output_config(config: OutputConfigSpec) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "base_output_dir": config.base_output_dir,
        "dependencies": copy.deepcopy(config.dependencies),
    }
    if config.batch_name is not None:
        payload["batch_name"] = config.batch_name
    return payload
