from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..runtime import LauncherConfig
from .runtime import ExternalRuntimeConfig


@dataclass(frozen=True)
class EvalTrainOutputsConfig:
    required: bool = True
    checkpoint_policy: str = "latest"
    explicit_checkpoint: str | None = None


@dataclass(frozen=True)
class EvalConfigSpec:
    enabled: bool = False
    command: str | None = None
    command_mode: str | None = None
    script: str | None = None
    external_runtime: ExternalRuntimeConfig | None = None
    workdir: str | None = None
    args: dict[str, Any] = field(default_factory=dict)
    pass_run_args: bool = True
    run_args_flag: str = "run_args_json"
    pass_model_overrides: bool = False
    model_overrides_flag: str = "model_overrides_json"
    launch_mode: str | None = None
    launcher: LauncherConfig = field(default_factory=LauncherConfig)
    train_outputs: EvalTrainOutputsConfig = field(default_factory=EvalTrainOutputsConfig)
