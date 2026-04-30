from __future__ import annotations

import shlex

from ...config_contract.option_sets import (
    LAUNCHER_COMMAND,
    LAUNCHER_MPIRUN,
    LAUNCHER_PYTHON,
    LAUNCHER_SINGLE,
    LAUNCHER_SRUN,
    LAUNCHER_TORCHRUN,
)
from ...plans.launcher import LauncherPlan
from .mpirun import mpirun_list_command, mpirun_shell_command
from .srun import srun_list_command, srun_shell_command
from .torchrun import torchrun_python_script_command


def python_script_command(
    *,
    python_bin: str,
    script: str,
    script_args: list[str],
    launcher: LauncherPlan,
    launcher_type: str,
) -> tuple[list[str] | str, bool]:
    base = [python_bin, script, *script_args]
    if launcher_type in {LAUNCHER_SINGLE, LAUNCHER_PYTHON}:
        return base, False
    if launcher_type == LAUNCHER_TORCHRUN:
        return torchrun_python_script_command(
            python_bin=python_bin,
            script=script,
            script_args=script_args,
            launcher=launcher,
        )
    if launcher_type == LAUNCHER_SRUN:
        return srun_list_command(base, launcher)
    if launcher_type == LAUNCHER_MPIRUN:
        return mpirun_list_command(base, launcher)
    if launcher_type == LAUNCHER_COMMAND:
        return base, False
    raise ValueError(f"Unsupported launcher type: {launcher_type}")


def command_entry_command(
    command: str | list[str] | None,
    extra_args: list[str],
    *,
    launcher: LauncherPlan,
    launcher_type: str,
) -> tuple[list[str] | str, bool]:
    if isinstance(command, list):
        base = [str(item) for item in command] + extra_args
        if launcher_type == LAUNCHER_SRUN:
            return srun_list_command(base, launcher)
        if launcher_type == LAUNCHER_MPIRUN:
            return mpirun_list_command(base, launcher)
        return base, False
    suffix = "" if not extra_args else " " + shlex.join(extra_args)
    command_text = str(command) + suffix
    if launcher_type == LAUNCHER_SRUN:
        return srun_shell_command(command_text, launcher)
    if launcher_type == LAUNCHER_MPIRUN:
        return mpirun_shell_command(command_text, launcher)
    return command_text, True
