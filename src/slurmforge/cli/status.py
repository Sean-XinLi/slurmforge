"""``sforge status`` -- summarize execution status for runs in a batch."""
from __future__ import annotations

import argparse
from pathlib import Path

from ..pipeline.records import load_batch_run_plans
from ..pipeline.status import ExecutionStatus, load_or_infer_execution_status, status_matches_query


def _status_bucket(status: ExecutionStatus | None) -> str:
    if status is None:
        return "missing"
    if status.state == "success":
        return "success"
    if status.state == "pending":
        return "pending"
    if status.state == "running":
        return "running"
    return status.failure_class or status.state or "unknown"


def _trim_reason(reason: str, limit: int = 120) -> str:
    cleaned = " ".join((reason or "").split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3] + "..."


def render_status(*, batch_root: Path, status_query: str) -> None:
    if not batch_root.exists():
        raise FileNotFoundError(f"status batch_root does not exist: {batch_root}")

    plans = load_batch_run_plans(batch_root)
    counts: dict[str, int] = {}
    matched_rows: list[str] = []
    for plan in plans:
        run_dir = Path(plan.run_dir).resolve()
        status = load_or_infer_execution_status(run_dir)
        bucket = _status_bucket(status)
        counts[bucket] = counts.get(bucket, 0) + 1
        if not status_matches_query(status, status_query):
            continue
        state = status.state if status is not None else "missing"
        failure_class = status.failure_class if status is not None and status.failure_class else "-"
        failed_stage = status.failed_stage if status is not None and status.failed_stage else "-"
        job_key = status.job_key if status is not None and status.job_key else "-"
        reason = _trim_reason(status.reason if status is not None else "no execution result directory found")
        matched_rows.append(
            f"{run_dir.name}: state={state} class={failure_class} stage={failed_stage} job={job_key} reason={reason}"
        )

    print(f"[STATUS] batch={batch_root} total_runs={len(plans)} matched={len(matched_rows)} query={status_query}")
    if counts:
        summary = ", ".join(f"{key}={counts[key]}" for key in sorted(counts))
        print(f"[STATUS] counts: {summary}")
    for row in matched_rows:
        print(row)


def handle_status(args: argparse.Namespace) -> None:
    render_status(
        batch_root=Path(args.batch_root).resolve(),
        status_query=args.status,
    )


def add_subparser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    status_parser = subparsers.add_parser("status", help="Summarize inferred execution status for runs in an existing batch")
    status_parser.add_argument(
        "--from",
        "--batch_root",
        dest="batch_root",
        required=True,
        help="Path to an existing batch_root to summarize",
    )
    status_parser.add_argument(
        "--status",
        default="all",
        help=(
            "Filter results by status: all, failed(non-success), success, pending, running, "
            "oom, preempted, node_failure, script_error, eval_failed"
        ),
    )
    status_parser.set_defaults(handler=handle_status)
