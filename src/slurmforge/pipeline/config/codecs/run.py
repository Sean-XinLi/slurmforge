from __future__ import annotations

from typing import Any

from ..runtime import serialize_launcher_config
from .runtime import serialize_external_runtime_config
from ..models.run import AdapterConfig, RunConfigSpec


def serialize_adapter_config(config: AdapterConfig) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "script": config.script,
        "args": dict(config.args),
        "launcher": serialize_launcher_config(config.launcher),
        "pass_run_args": bool(config.pass_run_args),
        "run_args_flag": config.run_args_flag,
        "pass_model_overrides": bool(config.pass_model_overrides),
        "model_overrides_flag": config.model_overrides_flag,
        "ddp_required": bool(config.ddp_required),
    }
    if config.workdir is not None:
        payload["workdir"] = config.workdir
    if config.launch_mode is not None:
        payload["launch_mode"] = config.launch_mode
    if config.ddp_supported is not None:
        payload["ddp_supported"] = bool(config.ddp_supported)
    return payload


def serialize_run_config(config: RunConfigSpec) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "mode": config.mode,
        "args": dict(config.args),
        "model_overrides": dict(config.model_overrides),
    }
    if config.command is not None:
        payload["command"] = config.command
    if config.command_mode is not None:
        payload["command_mode"] = config.command_mode
    if config.workdir is not None:
        payload["workdir"] = config.workdir
    if config.resume_from_checkpoint is not None:
        payload["resume_from_checkpoint"] = config.resume_from_checkpoint
    if config.adapter is not None:
        payload["adapter"] = serialize_adapter_config(config.adapter)
    if config.mode == "command":
        payload["external_runtime"] = serialize_external_runtime_config(config.external_runtime)
    return payload
