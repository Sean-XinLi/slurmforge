from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..runtime import LauncherConfig
from .runtime import ExternalRuntimeConfig


@dataclass(frozen=True)
class AdapterConfig:
    script: str
    args: dict[str, Any] = field(default_factory=dict)
    launcher: LauncherConfig = field(default_factory=LauncherConfig)
    workdir: str | None = None
    launch_mode: str | None = None
    pass_run_args: bool = True
    run_args_flag: str = "run_args_json"
    pass_model_overrides: bool = True
    model_overrides_flag: str = "model_overrides_json"
    ddp_supported: bool | None = None
    ddp_required: bool = False


@dataclass(frozen=True)
class RunConfigSpec:
    mode: str
    args: dict[str, Any] = field(default_factory=dict)
    model_overrides: dict[str, Any] = field(default_factory=dict)
    command: str | None = None
    command_mode: str | None = None
    workdir: str | None = None
    resume_from_checkpoint: str | None = None
    adapter: AdapterConfig | None = None
    external_runtime: ExternalRuntimeConfig = field(default_factory=ExternalRuntimeConfig)
