from __future__ import annotations

from pathlib import Path
from typing import Any

from .....errors import ConfigContractError
from ...models import ModelConfigSpec
from ...scalars import normalize_optional_bool
from ...utils import non_empty_text
from .shared import normalize_optional_text


def normalize_model_config(value: Any, *, required: bool, config_path: Path | str) -> ModelConfigSpec | None:
    if value is None:
        if required:
            raise ConfigContractError(
                f"{config_path}: model config is required when run.mode/model inference resolves to `model_cli`"
            )
        return None
    if not isinstance(value, dict):
        raise ConfigContractError(f"{config_path}: model must be a mapping")

    name = non_empty_text(value.get("name"))
    if not name:
        raise ConfigContractError(f"{config_path}: model.name must be a non-empty string")

    return ModelConfigSpec(
        name=name,
        script=normalize_optional_text(value.get("script"), name=f"{config_path}: model.script"),
        yaml=normalize_optional_text(value.get("yaml"), name=f"{config_path}: model.yaml"),
        ddp_supported=normalize_optional_bool(
            value.get("ddp_supported"),
            name=f"{config_path}: model.ddp_supported",
        )
        if "ddp_supported" in value
        else None,
        ddp_required=normalize_optional_bool(
            value.get("ddp_required"),
            name=f"{config_path}: model.ddp_required",
        )
        if "ddp_required" in value
        else None,
        estimator_profile=normalize_optional_text(
            value.get("estimator_profile"),
            name=f"{config_path}: model.estimator_profile",
        ),
    )
