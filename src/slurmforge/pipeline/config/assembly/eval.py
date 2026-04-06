from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from ....errors import ConfigContractError
from ..constants import EVAL_LAUNCH_MODES
from ..codecs import ensure_eval_train_outputs_config
from ..models import EvalConfigSpec, ExternalRuntimeConfig
from ..normalize import normalize_launcher
from ..scalars import normalize_bool
from ..utils import ensure_dict
from .run import (
    normalize_external_runtime,
    normalize_optional_text,
    validate_external_command_launcher,
    warn_external_command_raw_shell_semantics,
)


def normalize_eval_config(value: Any, *, config_path: Path | str) -> EvalConfigSpec:
    data = ensure_dict(value, "eval")
    enabled = normalize_bool(data.get("enabled", False), name=f"{config_path}: eval.enabled")
    train_outputs = ensure_eval_train_outputs_config(
        data.get("train_outputs"),
        name=f"{config_path}: eval.train_outputs",
    )
    command = normalize_optional_text(data.get("command"), name=f"{config_path}: eval.command")
    command_mode = normalize_optional_text(data.get("command_mode"), name=f"{config_path}: eval.command_mode")
    script = normalize_optional_text(data.get("script"), name=f"{config_path}: eval.script")
    if command and script:
        raise ConfigContractError(f"{config_path}: eval.command and eval.script cannot be used together")
    if command_mode is not None:
        command_mode = command_mode.lower()
        if command_mode not in {"argv", "raw"}:
            raise ConfigContractError(f"{config_path}: eval.command_mode must be one of: argv, raw")

    launch_mode = normalize_optional_text(data.get("launch_mode"), name=f"{config_path}: eval.launch_mode")
    if launch_mode is not None:
        launch_mode = launch_mode.lower()
        if launch_mode not in EVAL_LAUNCH_MODES:
            raise ConfigContractError(f"{config_path}: eval.launch_mode must be one of: {sorted(EVAL_LAUNCH_MODES)}")

    args = copy.deepcopy(ensure_dict(data.get("args"), "eval.args"))
    launcher = normalize_launcher(ensure_dict(data.get("launcher"), "eval.launcher"))
    external_runtime_value = data.get("external_runtime")
    external_runtime: ExternalRuntimeConfig | None = None

    if command is not None:
        if launch_mode is not None:
            raise ConfigContractError(f"{config_path}: eval.launch_mode is not valid with eval.command")
        if args:
            raise ConfigContractError(
                f"{config_path}: eval.command does not consume eval.args; inline those arguments into eval.command"
            )
        if normalize_bool(data.get("pass_run_args", False), name=f"{config_path}: eval.pass_run_args"):
            raise ConfigContractError(
                f"{config_path}: eval.command does not inject run args; inline them into eval.command"
            )
        if normalize_bool(data.get("pass_model_overrides", False), name=f"{config_path}: eval.pass_model_overrides"):
            raise ConfigContractError(
                f"{config_path}: eval.command does not inject model overrides; inline them into eval.command"
            )
        if external_runtime_value in (None, ""):
            raise ConfigContractError(f"{config_path}: eval.command requires eval.external_runtime")
        external_runtime = normalize_external_runtime(
            external_runtime_value,
            config_path=config_path,
            field_name="eval.external_runtime",
        )
        validate_external_command_launcher(
            launcher,
            ensure_dict(data.get("launcher"), "eval.launcher"),
            config_path=config_path,
            context_name="eval.command",
            runtime_field_name="eval.external_runtime",
            launcher_field_name="eval.launcher",
        )
        if command_mode == "raw":
            warn_external_command_raw_shell_semantics(
                config_path=config_path,
                mode_field_name="eval.command_mode",
                command_field_name="eval.command",
            )
    else:
        if command_mode is not None:
            raise ConfigContractError(f"{config_path}: eval.command_mode is only valid with eval.command")
        if external_runtime_value not in (None, ""):
            raise ConfigContractError(f"{config_path}: eval.external_runtime is only valid with eval.command")

    return EvalConfigSpec(
        enabled=enabled,
        command=command,
        command_mode=("argv" if command is not None and command_mode is None else command_mode),
        script=script,
        external_runtime=external_runtime,
        workdir=normalize_optional_text(data.get("workdir"), name=f"{config_path}: eval.workdir"),
        args=args,
        pass_run_args=normalize_bool(
            data.get("pass_run_args", False if command is not None else True),
            name=f"{config_path}: eval.pass_run_args",
        ),
        run_args_flag=str(data.get("run_args_flag", "run_args_json")).strip() or "run_args_json",
        pass_model_overrides=normalize_bool(
            data.get("pass_model_overrides", False),
            name=f"{config_path}: eval.pass_model_overrides",
        ),
        model_overrides_flag=str(data.get("model_overrides_flag", "model_overrides_json")).strip()
        or "model_overrides_json",
        launch_mode=launch_mode,
        launcher=launcher,
        train_outputs=train_outputs,
    )
