from __future__ import annotations

import shlex
from pathlib import Path
from typing import Any

from ...model_support.argparse_introspect import extract_cli_arg_actions
from ..config.normalize import ensure_resources_config
from ..config.runtime import DEFAULT_RESOURCES, LauncherConfig, ResourcesConfig
from .cli_args import to_cli_args
from .strategies import STRATEGIES
from .types import LaunchRuntime, ShellToken


def max_gpus_per_job(resources_cfg: ResourcesConfig | dict[str, Any]) -> int:
    resources = ensure_resources_config(resources_cfg)
    default_limit = int(DEFAULT_RESOURCES["max_gpus_per_job"])
    return max(1, int(resources.max_gpus_per_job or default_limit))


def build_stage_command(
    script_path: Path,
    args: dict[str, Any],
    launcher_cfg: LauncherConfig | dict[str, Any],
    launch_mode: str,
    run_idx: int,
) -> tuple[str, LaunchRuntime]:
    strategy = STRATEGIES[launch_mode]
    prefix, runtime = strategy.build_prefix(launcher_cfg, run_idx)
    arg_actions = extract_cli_arg_actions(str(script_path))
    tokens = prefix + [ShellToken(str(script_path))] + [ShellToken(value) for value in to_cli_args(args, arg_actions=arg_actions)]
    return _render_shell_tokens(tokens), runtime


def _render_shell_tokens(tokens: list[ShellToken]) -> str:
    rendered: list[str] = []
    for token in tokens:
        if token.raw:
            rendered.append(token.value)
        else:
            rendered.append(shlex.quote(token.value))
    return " ".join(rendered)
