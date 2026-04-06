from __future__ import annotations

import copy
from pathlib import Path

from .....errors import ConfigContractError
from ...models import ExternalRuntimeConfig, RunConfigSpec
from ...utils import ensure_dict, non_empty_text
from .adapter import normalize_adapter_config
from .external_runtime import normalize_external_runtime
from .shared import normalize_optional_text, warn_external_command_raw_shell_semantics


def build_run_spec(
    run_cfg: dict,
    *,
    config_path: Path | str,
    run_mode: str,
) -> RunConfigSpec:
    args = copy.deepcopy(ensure_dict(run_cfg.get("args"), "run.args"))
    model_overrides = copy.deepcopy(ensure_dict(run_cfg.get("model_overrides"), "run.model_overrides"))
    command_mode = normalize_optional_text(run_cfg.get("command_mode"), name=f"{config_path}: run.command_mode")
    if command_mode is not None:
        command_mode = command_mode.lower()
        if command_mode not in {"argv", "raw"}:
            raise ConfigContractError(f"{config_path}: run.command_mode must be one of: argv, raw")

    adapter = None
    external_runtime = ExternalRuntimeConfig()
    if run_mode == "adapter":
        if command_mode is not None:
            raise ConfigContractError(f"{config_path}: run.command_mode is only valid in command mode")
        adapter = normalize_adapter_config(run_cfg.get("adapter"), config_path=config_path)
    elif run_mode == "command":
        external_runtime = normalize_external_runtime(
            run_cfg.get("external_runtime"),
            config_path=config_path,
            field_name="run.external_runtime",
        )
    else:
        if command_mode is not None:
            raise ConfigContractError(f"{config_path}: run.command_mode is only valid in command mode")

    if run_mode != "command" and run_cfg.get("external_runtime") not in (None, ""):
        raise ConfigContractError(f"{config_path}: run.external_runtime is only valid in command mode")

    if run_mode == "command" and command_mode == "raw":
        warn_external_command_raw_shell_semantics(
            config_path=config_path,
            mode_field_name="run.command_mode",
            command_field_name="run.command",
        )

    return RunConfigSpec(
        mode=run_mode,
        args=args,
        model_overrides=model_overrides,
        command=non_empty_text(run_cfg.get("command")),
        command_mode=("argv" if run_mode == "command" and command_mode is None else command_mode),
        workdir=normalize_optional_text(run_cfg.get("workdir"), name=f"{config_path}: run.workdir"),
        resume_from_checkpoint=normalize_optional_text(
            run_cfg.get("resume_from_checkpoint"),
            name=f"{config_path}: run.resume_from_checkpoint",
        ),
        adapter=adapter,
        external_runtime=external_runtime,
    )
