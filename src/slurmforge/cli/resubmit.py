"""``sforge resubmit`` -- rebuild and optionally submit one stage batch."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..io import SchemaVersion, to_jsonable
from ..orchestration import (
    build_prior_source_stage_batch,
    emit_sourced_stage_batch,
    summarize_stage_batch,
)
from .stage_common import dry_run_mode_from_args, emit_machine_dry_run_if_requested, print_lines


def handle_resubmit(args: argparse.Namespace) -> None:
    source_root = Path(args.root).resolve()
    plan = build_prior_source_stage_batch(
        source_root=source_root,
        stage_name=args.stage,
        query=args.query,
        run_ids=args.run_id or (),
        overrides=args.set or (),
    )
    if plan is None:
        mode = dry_run_mode_from_args(args)
        if mode in {"json", "full"}:
            payload = json.dumps(
                to_jsonable(
                    {
                        "schema_version": SchemaVersion.SOURCE_PLAN,
                        "command": "resubmit",
                        "state": "valid",
                        "plan_kind": "empty_source_selection",
                        "plan": {},
                        "validation": {
                            "selected_runs": 0,
                            "stage": args.stage,
                            "query": args.query,
                            "source_root": str(source_root),
                        },
                    }
                ),
                indent=2,
                sort_keys=True,
            ) + "\n"
            if args.output:
                Path(args.output).write_text(payload, encoding="utf-8")
            else:
                print(payload, end="")
            return
        print(f"[RESUBMIT] selected_runs=0 stage={args.stage} query={args.query}")
        return
    if emit_machine_dry_run_if_requested(args, plan.spec, plan.batch, command="resubmit"):
        return
    print(f"[RESUBMIT] selected_runs={len(plan.selected_runs)} source={source_root}")
    if dry_run_mode_from_args(args):
        print_lines(summarize_stage_batch(plan.batch))
        return

    concrete, group_job_ids = emit_sourced_stage_batch(plan, submit=not args.emit_only)
    print_lines(summarize_stage_batch(concrete.batch))
    if group_job_ids is not None:
        print(f"[OK] submitted resubmit batch: {concrete.batch.submission_root}")
        print(f"[OK] scheduler_job_ids={','.join(group_job_ids.values())}")


def add_subparser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("resubmit", help="Resubmit one stage from a batch or train/eval pipeline root")
    parser.add_argument("--from", dest="root", required=True, help="Stage batch or train/eval pipeline root")
    parser.add_argument("--stage", required=True, help="Stage name to resubmit")
    parser.add_argument("--query", default="state=failed", help="Selection query, e.g. state=failed")
    parser.add_argument("--run-id", action="append", default=[], help="Select a specific run_id")
    parser.add_argument("--set", action="append", default=[], help="Override config by dot-path")
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        nargs="?",
        const="summary",
        default=False,
        choices=("summary", "json", "full"),
        help="Compile without writing files; json/full produce machine-readable audit output",
    )
    parser.add_argument(
        "--emit-only",
        dest="emit_only",
        action="store_true",
        help="Write plan and sbatch files without submitting",
    )
    parser.add_argument("--output", default=None, help="Write machine-readable dry-run output to this path")
    parser.set_defaults(handler=handle_resubmit)
