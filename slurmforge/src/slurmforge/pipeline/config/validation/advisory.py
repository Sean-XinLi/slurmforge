"""
Level 2 advisory validation: best-practice warnings.

These checks detect configurations that are valid and will run, but are likely
mistakes or suboptimal choices — e.g. a long job with no checkpointing.  They
never block generation; they only print warnings.

Returns a list of (field_label, message) tuples consumed by
messages.format_advisory_report.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from ._helpers import get_nested, parse_time_seconds

__all__ = ["check_advisory"]

_24H_SECONDS = 24 * 3600


def check_advisory(
    cfg: dict[str, Any],
    *,
    config_path: Path,  # noqa: ARG001 — available for future message enrichment
) -> list[tuple[str, str]]:
    """
    Run Level 2 advisory checks on a raw config dict.

    Returns a list of (field_label, message) pairs.  An empty list means no
    advisory warnings were found.
    """
    warnings: list[tuple[str, str]] = []

    # ── no environment activation configured ─────────────────────────────────
    # Jobs that skip venv/conda/modules are likely to fail with ImportError.
    venv = get_nested(cfg, "env", "venv_activate", default="")
    conda = get_nested(cfg, "env", "conda_activate", default="")
    modules = get_nested(cfg, "env", "modules", default=[])
    env_activated = (
        (isinstance(venv, str) and venv.strip())
        or (isinstance(conda, str) and conda.strip())
        or (isinstance(modules, list) and modules)
    )
    if not env_activated:
        warnings.append((
            "env.venv_activate",
            (
                "No environment activation configured "
                "(env.venv_activate, env.conda_activate, and env.modules are all empty). "
                "The job will run in the default shell environment, which may not have "
                "your packages installed. "
                'Set env.venv_activate, e.g. "source /path/to/venv/bin/activate".'
            ),
        ))

    # ── long job without checkpoint globs ────────────────────────────────────
    # A job longer than 24 h with no checkpointing risks losing all progress
    # if the node is preempted or times out.
    time_limit = get_nested(cfg, "cluster", "time_limit")
    checkpoint_globs = get_nested(cfg, "artifacts", "checkpoint_globs", default=[])
    if (
        isinstance(time_limit, str)
        and not (isinstance(checkpoint_globs, list) and checkpoint_globs)
    ):
        seconds = parse_time_seconds(time_limit)
        if seconds is not None and seconds > _24H_SECONDS:
            warnings.append((
                "artifacts.checkpoint_globs",
                (
                    f"cluster.time_limit is over 24 h ({time_limit!r}) but "
                    "artifacts.checkpoint_globs is empty. "
                    "If the job fails or is preempted near the end, all progress will be lost. "
                    'Add checkpoint glob patterns, e.g. ["checkpoints/**/*.pt"].'
                ),
            ))

    # ── empty run.args for non-command modes ─────────────────────────────────
    # Command mode embeds all parameters in run.command, so empty args is expected.
    # For script/adapter/registry modes, empty args is likely an oversight.
    run_args = get_nested(cfg, "run", "args", default=None)
    run_command = get_nested(cfg, "run", "command")
    if isinstance(run_args, dict) and not run_args and run_command is None:
        warnings.append((
            "run.args",
            (
                "run.args is empty. If your training script requires hyperparameters "
                "(learning rate, epochs, data path, etc.), add them here. "
                'Example: {lr: 1e-4, epochs: 10, data_dir: "/path/to/data"}'
            ),
        ))

    return warnings
