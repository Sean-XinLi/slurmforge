"""``sforge rerun`` -- rebuild a retry batch from an existing batch's run records."""
from __future__ import annotations

import argparse
import datetime
from pathlib import Path

from ..pipeline.compiler import BatchCompileError, RetrySourceRequest, compile_source, iter_compile_report_lines
from ..pipeline.compiler.reports import report_total_runs, require_success
from .common import add_common_args, materialize_or_print_batch, print_batch_ready


def render_rerun(
    *,
    source_batch_root: Path,
    cli_overrides: list[str],
    dry_run: bool,
    project_root_override: str | None,
    status_query: str,
) -> None:
    default_batch_name = datetime.datetime.now().strftime("retry_%Y%m%d_%H%M%S_%f")
    report = compile_source(
        RetrySourceRequest(
            source_batch_root=source_batch_root,
            status_query=status_query,
            cli_overrides=tuple(cli_overrides),
            project_root=None if project_root_override is None else Path(project_root_override),
            default_batch_name=default_batch_name,
        )
    )
    source_summary = getattr(report, "source_summary", "") or str(source_batch_root)
    print(f"[RETRY] source={source_summary} selected_runs={report_total_runs(report)}")
    for line in iter_compile_report_lines(report):
        print(line)
    try:
        planned_batch = require_success(report)
    except BatchCompileError:
        raise

    dispatch = materialize_or_print_batch(
        planned_batch=planned_batch,
        dry_run=dry_run,
    )
    if dispatch is None:
        return
    print_batch_ready(dispatch=dispatch, sbatch_dir=planned_batch.sbatch_dir, retry=True)


def handle_rerun(args: argparse.Namespace) -> None:
    render_rerun(
        source_batch_root=Path(args.source_batch_root).resolve(),
        cli_overrides=args.set,
        dry_run=args.dry_run,
        project_root_override=args.project_root,
        status_query=args.status,
    )


def add_subparser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    rerun_parser = subparsers.add_parser("rerun", help="Rebuild a retry batch from an existing batch_root")
    rerun_parser.add_argument(
        "--from",
        dest="source_batch_root",
        required=True,
        help="Path to an existing batch_root; rebuild and resubmit a filtered retry batch from its run records",
    )
    rerun_parser.add_argument(
        "--status",
        default="failed",
        help=(
            "Retry filter for existing batch runs: failed(non-success), success, "
            "pending, running, oom, preempted, node_failure, script_error, eval_failed, all"
        ),
    )
    add_common_args(rerun_parser)
    rerun_parser.set_defaults(handler=handle_rerun)
