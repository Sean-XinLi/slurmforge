from __future__ import annotations

from typing import Any

from ..config_comments import option_comment
from .scalar import scalar


def render_project(lines: list[str], config: dict[str, Any]) -> None:
    lines.extend(
        [
            "# Project identity. Used in output paths under storage.root.",
            f"project: {scalar(config['project'])}",
            f"experiment: {scalar(config['experiment'])}",
            "",
            "storage:",
            "  # Root directory for generated plans, logs, status records, and artifacts.",
            f"  root: {scalar(config['storage']['root'])}",
            "",
        ]
    )


def render_environments(lines: list[str], config: dict[str, Any]) -> None:
    env = config["environments"]["default"]
    lines.extend(
        [
            "environments:",
            "  default:",
            "    # Optional environment modules to load before executor/user scripts.",
            f"    modules: {scalar(env['modules'])}",
            "    # Optional shell files or commands to source before execution.",
            f"    source: {scalar(env['source'])}",
            "    # Environment variables added to the Slurm job environment.",
            f"    env: {scalar(env['env'])}",
            "",
        ]
    )


def render_runtime(lines: list[str], config: dict[str, Any]) -> None:
    executor = config["runtime"]["executor"]
    executor_python = executor["python"]
    user_default = config["runtime"]["user"]["default"]
    user_python = user_default["python"]
    lines.extend(
        [
            "runtime:",
            "  executor:",
            "    python:",
            "      # Python used by slurmforge's executor wrapper on compute nodes.",
            f"      bin: {scalar(executor_python['bin'])}",
            f"      min_version: {scalar(executor_python['min_version'])}",
            "    # Executor module; most users should keep this default.",
            f"    module: {scalar(executor['module'])}",
            "  user:",
            "    default:",
            "      python:",
            "        # Python used to run your stage scripts.",
            f"        bin: {scalar(user_python['bin'])}",
            f"        min_version: {scalar(user_python['min_version'])}",
            "      # Environment variables visible to user scripts.",
            f"      env: {scalar(user_default['env'])}",
            "",
        ]
    )


def render_artifact_store(lines: list[str], config: dict[str, Any]) -> None:
    store = config["artifact_store"]
    lines.extend(
        [
            "artifact_store:",
            option_comment("artifact_store.strategy", indent=2),
            "  # copy is safest because managed outputs remain available.",
            f"  strategy: {scalar(store['strategy'])}",
            option_comment("artifact_store.fallback_strategy", indent=2),
            f"  fallback_strategy: {scalar(store['fallback_strategy'])}",
            "  # Verify managed output digests after artifact storage.",
            f"  verify_digest: {scalar(store['verify_digest'])}",
            "  # Fail the run if artifact verification cannot prove integrity.",
            f"  fail_on_verify_error: {scalar(store['fail_on_verify_error'])}",
            "",
        ]
    )


def render_runs(lines: list[str], config: dict[str, Any]) -> None:
    runs = config["runs"]
    lines.extend(
        [
            "runs:",
            option_comment("runs.type", indent=2),
            "  # single is best for a starter; use grid/cases/matrix for sweeps.",
            f"  type: {scalar(runs['type'])}",
            "",
        ]
    )


def render_dispatch(lines: list[str], config: dict[str, Any]) -> None:
    dispatch = config["dispatch"]
    lines.extend(
        [
            "dispatch:",
            "  # Global GPU budget used to serialize Slurm array groups if needed.",
            f"  max_available_gpus: {scalar(dispatch['max_available_gpus'])}",
            option_comment("dispatch.overflow_policy", indent=2),
            f"  overflow_policy: {scalar(dispatch['overflow_policy'])}",
            "",
        ]
    )


def render_orchestration(lines: list[str], config: dict[str, Any]) -> None:
    controller = config["orchestration"]["controller"]
    lines.extend(
        [
            "orchestration:",
            "  controller:",
            "    # Slurm resources for the lightweight train/eval controller job.",
            f"    partition: {scalar(controller['partition'])}",
            f"    cpus: {scalar(controller['cpus'])}",
            f"    mem: {scalar(controller['mem'])}",
            f"    time_limit: {scalar(controller['time_limit'])}",
            "    # Must reference a key under environments.",
            f"    environment: {scalar(controller['environment'])}",
            "",
        ]
    )
