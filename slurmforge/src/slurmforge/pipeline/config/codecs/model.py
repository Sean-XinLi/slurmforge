from __future__ import annotations

from typing import Any

from ..models.model import ModelConfigSpec


def serialize_model_config(config: ModelConfigSpec) -> dict[str, Any]:
    payload: dict[str, Any] = {"name": config.name}
    if config.script is not None:
        payload["script"] = config.script
    if config.yaml is not None:
        payload["yaml"] = config.yaml
    if config.ddp_supported is not None:
        payload["ddp_supported"] = bool(config.ddp_supported)
    if config.ddp_required is not None:
        payload["ddp_required"] = bool(config.ddp_required)
    if config.estimator_profile is not None:
        payload["estimator_profile"] = config.estimator_profile
    return payload
