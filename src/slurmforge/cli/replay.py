"""``sforge replay`` -- regenerate a batch from persisted run snapshots."""
from __future__ import annotations

import argparse
import datetime
from pathlib import Path

from ..pipeline.compiler import BatchCompileError, ReplaySourceRequest, compile_source, iter_compile_report_lines
from ..pipeline.compiler.reports import report_total_runs, require_success
from .common import add_common_args, materialize_or_print_batch, print_batch_ready


def render_replay(
    *,
    source_run_dir: Path | None,
    source_batch_root: Path | None,
    run_ids: list[str],
    run_indices: list[int],
    cli_overrides: list[str],
    dry_run: bool,
    project_root_override: str | None,
) -> None:
    default_batch_name = datetime.datetime.now().strftime("replay_%Y%m%d_%H%M%S_%f")
    request = ReplaySourceRequest(
        source_run_dir=source_run_dir,
        source_batch_root=source_batch_root,
        run_ids=tuple(run_ids),
        run_indices=tuple(run_indices),
        cli_overrides=tuple(cli_overrides),
        project_root=None if project_root_override is None else Path(project_root_override),
        default_batch_name=default_batch_name,
    )
    report = compile_source(request)
    source_summary = getattr(report, "source_summary", "") or "<missing replay source>"
    print(f"[REPLAY] source={source_summary} selected_runs={report_total_runs(report)}")
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
    print_batch_ready(dispatch=dispatch, sbatch_dir=planned_batch.sbatch_dir)


def handle_replay(args: argparse.Namespace) -> None:
    render_replay(
        source_run_dir=None if args.source_run_dir is None else Path(args.source_run_dir),
        source_batch_root=None if args.source_batch_root is None else Path(args.source_batch_root),
        run_ids=args.run_id,
        run_indices=args.run_index,
        cli_overrides=args.set,
        dry_run=args.dry_run,
        project_root_override=args.project_root,
    )


def add_subparser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    replay_parser = subparsers.add_parser(
        "replay",
        help="Replay one or more persisted runs from a run dir or batch root",
    )
    source_group = replay_parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--from-run",
        dest="source_run_dir",
        help="Path to a persisted run directory under an existing batch_root",
    )
    source_group.add_argument(
        "--from-batch",
        dest="source_batch_root",
        help="Path to an existing batch_root; replays all runs unless selectors are provided",
    )
    replay_parser.add_argument(
        "--run_id",
        action="append",
        default=[],
        help="Select specific run_id values when replaying from --from-batch",
    )
    replay_parser.add_argument(
        "--run_index",
        action="append",
        type=int,
        default=[],
        help="Select specific run_index values when replaying from --from-batch",
    )
    add_common_args(replay_parser)
    replay_parser.set_defaults(handler=handle_replay)
