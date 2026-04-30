from __future__ import annotations

import shlex

from ...config_contract.option_sets import ENTRY_PYTHON_SCRIPT
from ...config_contract.registry import default_for
from ...contracts import InputBinding
from ...plans.stage import StageInstancePlan
from ..bindings import binding_injected_value
from .args import args_to_argv, flag
from .command import command_entry_command, python_script_command

DEFAULT_PYTHON_BIN = default_for("runtime.executor.python.bin")
DEFAULT_STAGE_LAUNCHER_TYPE = default_for("stages.*.launcher.type")


def build_shell_script(
    instance: StageInstancePlan, bindings: tuple[InputBinding, ...]
) -> str:
    command, use_shell = _build_command(instance, bindings)
    if isinstance(command, list):
        command_text = shlex.join(command)
    else:
        command_text = command if use_shell else shlex.quote(command)
    return "\n".join(
        [*_env_setup_lines(instance), *_before_lines(instance), command_text]
    )


def _env_setup_lines(instance: StageInstancePlan) -> list[str]:
    return ["set -euo pipefail"]


def _before_lines(instance: StageInstancePlan) -> list[str]:
    lines: list[str] = []
    for index, step in enumerate(instance.before_steps):
        name = str(step.name or f"before_{index + 1}")
        lines.append(f'printf "%s\\n" {shlex.quote(f"[BEFORE] {name}")}')
        lines.append(str(step.run))
    return lines


def _build_command(
    instance: StageInstancePlan, bindings: tuple[InputBinding, ...]
) -> tuple[list[str] | str, bool]:
    entry = instance.entry
    extra_args = args_to_argv(entry.args)
    for binding in bindings:
        injected = binding_injected_value(binding)
        if binding.inject.get("required") and injected is None:
            raise FileNotFoundError(
                f"Required input `{binding.input_name}` is unresolved"
            )
        injected_flag = binding.inject.get("flag")
        if injected_flag and injected is not None:
            extra_args.extend([flag(str(injected_flag)), injected])
    launcher = instance.launcher_plan
    launcher_type = launcher.type or DEFAULT_STAGE_LAUNCHER_TYPE
    runtime_user = instance.runtime_plan.user
    python_bin = (
        runtime_user.python.bin if runtime_user is not None else DEFAULT_PYTHON_BIN
    )
    if entry.type == ENTRY_PYTHON_SCRIPT:
        return python_script_command(
            python_bin=python_bin,
            script=str(entry.script),
            script_args=extra_args,
            launcher=launcher,
            launcher_type=launcher_type,
        )
    return command_entry_command(
        entry.command, extra_args, launcher=launcher, launcher_type=launcher_type
    )
