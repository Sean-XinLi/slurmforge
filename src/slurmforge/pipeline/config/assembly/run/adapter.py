from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from .....errors import ConfigContractError
from ...constants import LAUNCH_MODES
from ...models import AdapterConfig
from ...normalize import normalize_launcher
from ...scalars import normalize_bool, normalize_optional_bool
from ...utils import ensure_dict, non_empty_text
from .shared import normalize_optional_text


def normalize_adapter_config(value: Any, *, config_path: Path | str) -> AdapterConfig:
    data = ensure_dict(value, "run.adapter")
    script = non_empty_text(data.get("script"))
    if not script:
        raise ConfigContractError(f"{config_path}: run.adapter.script must be a non-empty string in adapter mode")

    launch_mode = normalize_optional_text(data.get("launch_mode"), name=f"{config_path}: run.adapter.launch_mode")
    if launch_mode is not None:
        launch_mode = launch_mode.lower()
        if launch_mode not in LAUNCH_MODES:
            raise ConfigContractError(f"{config_path}: run.adapter.launch_mode must be one of: {sorted(LAUNCH_MODES)}")

    args = ensure_dict(data.get("args"), "run.adapter.args")
    return AdapterConfig(
        script=script,
        args=copy.deepcopy(args),
        launcher=normalize_launcher(ensure_dict(data.get("launcher"), "run.adapter.launcher")),
        workdir=normalize_optional_text(data.get("workdir"), name=f"{config_path}: run.adapter.workdir"),
        launch_mode=launch_mode,
        pass_run_args=normalize_bool(
            data.get("pass_run_args", True),
            name=f"{config_path}: run.adapter.pass_run_args",
        ),
        run_args_flag=str(data.get("run_args_flag", "run_args_json")).strip() or "run_args_json",
        pass_model_overrides=normalize_bool(
            data.get("pass_model_overrides", True),
            name=f"{config_path}: run.adapter.pass_model_overrides",
        ),
        model_overrides_flag=str(data.get("model_overrides_flag", "model_overrides_json")).strip()
        or "model_overrides_json",
        ddp_supported=normalize_optional_bool(
            data.get("ddp_supported"),
            name=f"{config_path}: run.adapter.ddp_supported",
        )
        if "ddp_supported" in data
        else None,
        ddp_required=normalize_bool(
            data.get("ddp_required", False),
            name=f"{config_path}: run.adapter.ddp_required",
        ),
    )
