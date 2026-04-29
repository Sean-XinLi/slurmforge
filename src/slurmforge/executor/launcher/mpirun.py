from __future__ import annotations

import shlex

from ...plans.launcher import LauncherPlan


def mpirun_list_command(
    base: list[str], launcher: LauncherPlan
) -> tuple[list[str], bool]:
    return ["mpirun", *[str(item) for item in launcher.args], *base], False


def mpirun_shell_command(command_text: str, launcher: LauncherPlan) -> tuple[str, bool]:
    args = " ".join(shlex.quote(str(item)) for item in launcher.args)
    return f"mpirun {args} {command_text}".strip(), True
