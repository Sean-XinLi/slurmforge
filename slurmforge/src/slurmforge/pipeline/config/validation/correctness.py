"""
Level 1 correctness validation: format and logical consistency checks.

Catches configuration values that are syntactically valid YAML but will cause
sbatch generation failures or produce nonsensical jobs — before the compiler
ever runs.

Returns a list of (field_label, message) tuples consumed by
messages.format_correctness_report.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from ._helpers import get_nested, is_valid_time_limit

__all__ = ["check_correctness"]

# Sanity upper bound for total GPUs in a single job.
# Larger allocations belong to multi-job pipelines, not a single sbatch array.
_MAX_TOTAL_GPUS = 512

_VALID_LAUNCHER_MODES = frozenset({"auto", "ddp", "single"})


def check_correctness(
    cfg: dict[str, Any],
    *,
    config_path: Path,
) -> list[tuple[str, str]]:
    """
    Run Level 1 correctness checks on a raw config dict.

    Returns a list of (field_label, message) pairs.  An empty list means the
    config passes all correctness checks.
    """
    errors: list[tuple[str, str]] = []
    loc = str(config_path)   # used as prefix in messages for traceability

    # ── project / experiment_name: must be non-empty strings ─────────────────
    for key in ("project", "experiment_name"):
        val = cfg.get(key)
        if val is not None and (not isinstance(val, str) or not val.strip()):
            errors.append((
                key,
                f"{loc}: must be a non-empty string; got {val!r}",
            ))

    # ── cluster.time_limit: must match HH:MM:SS or D-HH:MM:SS ───────────────
    time_limit = get_nested(cfg, "cluster", "time_limit")
    if time_limit is not None and isinstance(time_limit, str):
        if not is_valid_time_limit(time_limit):
            errors.append((
                "cluster.time_limit",
                (
                    f"invalid format {time_limit!r}. "
                    'Expected HH:MM:SS or D-HH:MM:SS — '
                    'e.g. "02:00:00" for 2 hours, "1-12:00:00" for 1 day 12 hours.'
                ),
            ))

    # ── cluster: total GPU count sanity ──────────────────────────────────────
    gpus_per_node = get_nested(cfg, "cluster", "gpus_per_node")
    nodes = get_nested(cfg, "cluster", "nodes", default=1)
    if isinstance(gpus_per_node, int) and isinstance(nodes, int) and gpus_per_node > 0:
        total = gpus_per_node * nodes
        if total > _MAX_TOTAL_GPUS:
            errors.append((
                "cluster.gpus_per_node",
                (
                    f"gpus_per_node ({gpus_per_node}) × nodes ({nodes}) = {total} total GPUs, "
                    f"which exceeds the sanity limit of {_MAX_TOTAL_GPUS}. "
                    "Check your values; use a multi-job pipeline for very large allocations."
                ),
            ))

    # ── env: venv_activate and conda_activate must not both be set ────────────
    venv = get_nested(cfg, "env", "venv_activate", default="")
    conda = get_nested(cfg, "env", "conda_activate", default="")
    if isinstance(venv, str) and isinstance(conda, str) and venv.strip() and conda.strip():
        errors.append((
            "env",
            (
                "venv_activate and conda_activate are both set. "
                "Use one or the other — activating both causes unpredictable PATH conflicts."
            ),
        ))

    # ── launcher.mode: must be a known value ─────────────────────────────────
    launcher_mode = get_nested(cfg, "launcher", "mode")
    if launcher_mode is not None and launcher_mode not in _VALID_LAUNCHER_MODES:
        errors.append((
            "launcher.mode",
            (
                f"unknown value {launcher_mode!r}. "
                'Valid options: "auto", "ddp", "single".'
            ),
        ))

    # ── eval: if enabled, must have script or command ─────────────────────────
    eval_enabled = get_nested(cfg, "eval", "enabled", default=False)
    eval_script = get_nested(cfg, "eval", "script")
    eval_command = get_nested(cfg, "eval", "command")
    if eval_enabled and not eval_script and not eval_command:
        errors.append((
            "eval",
            (
                "eval.enabled is true but neither eval.script nor eval.command is set. "
                "Provide one to run evaluation after training."
            ),
        ))

    return errors
