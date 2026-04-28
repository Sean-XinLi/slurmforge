from __future__ import annotations

import argparse

from ..orchestration.results import ExecutionMode


def add_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config", required=True, help="Path to stage-batch experiment YAML"
    )
    parser.add_argument(
        "--set",
        action="append",
        default=[],
        help="Override config by dot-path, e.g. --set stages.train.entry.args.lr=0.004",
    )
    parser.add_argument(
        "--project-root",
        dest="project_root",
        default=None,
        help="Override project root used to resolve relative paths (default: config file directory)",
    )


def add_execution_mode_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        nargs="?",
        const="summary",
        default=False,
        choices=("summary", "json", "full"),
        help="Compile without writing files. Use --dry-run=json or --dry-run=full for machine-readable audit output",
    )
    parser.add_argument(
        "--emit-only",
        dest="emit_only",
        action="store_true",
        help="Write plan and sbatch files without submitting",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Write machine-readable dry-run output to this path",
    )


def add_eval_source_args(parser: argparse.ArgumentParser, *, required: bool) -> None:
    group = parser.add_mutually_exclusive_group(required=required)
    group.add_argument(
        "--from-train-batch",
        dest="from_train_batch",
        help="Path to a train stage batch root",
    )
    group.add_argument(
        "--from-run",
        dest="from_run",
        help="Path to a run directory with stage_outputs.json",
    )
    group.add_argument(
        "--checkpoint", help="Explicit checkpoint path for all selected eval runs"
    )
    parser.add_argument(
        "--input-name", default=None, help="Eval input to bind from the selected source"
    )


def execution_mode_from_args(
    args: argparse.Namespace, *, default: ExecutionMode = "submit"
) -> ExecutionMode:
    if getattr(args, "dry_run", False):
        return "preview"
    if getattr(args, "emit_only", False):
        return "emit"
    return default


def dry_run_mode_from_args(args: argparse.Namespace) -> str:
    value = getattr(args, "dry_run", False)
    if value is True:
        return "summary"
    if value in (False, None, ""):
        return ""
    return str(value)
