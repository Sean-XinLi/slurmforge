from __future__ import annotations

import shlex

from ...plans import LauncherPlan


def srun_list_command(base: list[str], launcher: LauncherPlan) -> tuple[list[str], bool]:
    return ["srun", *[str(item) for item in launcher.args], *base], False


def srun_shell_command(command_text: str, launcher: LauncherPlan) -> tuple[str, bool]:
    args = " ".join(shlex.quote(str(item)) for item in launcher.args)
    return f"srun {args} {command_text}".strip(), True
