from __future__ import annotations

import copy
from typing import Any

from ....errors import ConfigContractError
from ..constants import EVAL_CHECKPOINT_POLICIES
from ..models.eval import EvalConfigSpec, EvalTrainOutputsConfig
from ..runtime import serialize_launcher_config
from ..scalars import normalize_bool
from ..utils import ensure_dict
from .runtime import serialize_external_runtime_config


def ensure_eval_train_outputs_config(
    value: Any,
    *,
    name: str = "eval.train_outputs",
) -> EvalTrainOutputsConfig:
    if isinstance(value, EvalTrainOutputsConfig):
        return value
    data = ensure_dict(value, name)
    required = normalize_bool(data.get("required", True), name=f"{name}.required")
    checkpoint_policy = str(data.get("checkpoint_policy", "latest") or "latest").strip().lower()
    if checkpoint_policy not in EVAL_CHECKPOINT_POLICIES:
        raise ConfigContractError(
            f"{name}.checkpoint_policy must be one of: {sorted(EVAL_CHECKPOINT_POLICIES)}"
        )
    explicit_checkpoint_raw = data.get("explicit_checkpoint")
    if explicit_checkpoint_raw is None:
        explicit_checkpoint = None
    elif not isinstance(explicit_checkpoint_raw, str):
        raise ConfigContractError(f"{name}.explicit_checkpoint must be a string when provided")
    else:
        explicit_checkpoint = explicit_checkpoint_raw.strip() or None
    if checkpoint_policy == "explicit" and explicit_checkpoint is None:
        raise ConfigContractError(f"{name}.explicit_checkpoint must be set when checkpoint_policy=explicit")
    if checkpoint_policy != "explicit" and explicit_checkpoint is not None:
        raise ConfigContractError(
            f"{name}.explicit_checkpoint is only valid when checkpoint_policy=explicit"
        )
    return EvalTrainOutputsConfig(
        required=required,
        checkpoint_policy=checkpoint_policy,
        explicit_checkpoint=explicit_checkpoint,
    )


def serialize_eval_train_outputs_config(config: EvalTrainOutputsConfig) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "required": bool(config.required),
        "checkpoint_policy": str(config.checkpoint_policy),
    }
    if config.explicit_checkpoint is not None:
        payload["explicit_checkpoint"] = str(config.explicit_checkpoint)
    return payload


def serialize_eval_config(config: EvalConfigSpec) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "enabled": bool(config.enabled),
        "args": copy.deepcopy(config.args),
        "pass_run_args": bool(config.pass_run_args),
        "run_args_flag": config.run_args_flag,
        "pass_model_overrides": bool(config.pass_model_overrides),
        "model_overrides_flag": config.model_overrides_flag,
        "launcher": serialize_launcher_config(config.launcher),
        "train_outputs": serialize_eval_train_outputs_config(config.train_outputs),
    }
    if config.command is not None:
        payload["command"] = config.command
    if config.command_mode is not None:
        payload["command_mode"] = config.command_mode
    if config.script is not None:
        payload["script"] = config.script
    if config.external_runtime is not None:
        payload["external_runtime"] = serialize_external_runtime_config(config.external_runtime)
    if config.workdir is not None:
        payload["workdir"] = config.workdir
    if config.launch_mode is not None:
        payload["launch_mode"] = config.launch_mode
    return payload
