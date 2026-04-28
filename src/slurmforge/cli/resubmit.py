"""``sforge resubmit`` -- rebuild and optionally submit one stage batch."""

from __future__ import annotations

import argparse
from pathlib import Path

from ..orchestration.audit import build_empty_source_selection_audit
from ..orchestration.launch import emit_sourced_stage_batch
from ..orchestration.stage_build import (
    build_prior_source_stage_batch,
    summarize_stage_batch,
)
from ..spec import ExperimentSpec, parse_experiment_spec, validate_experiment_spec
from .args import dry_run_mode_from_args
from .dry_run import emit_machine_dry_run_if_requested, emit_machine_payload
from .render import print_lines, print_sourced_stage_batch_execution_result


def _spec_for_sourced_plan(plan) -> ExperimentSpec:
    project_root = Path(plan.lineage.source_root)
    if plan.batch.stage_instances:
        project_root = Path(
            str(
                plan.batch.stage_instances[0].lineage.get("project_root")
                or project_root
            )
        )
    spec = parse_experiment_spec(
        plan.spec_snapshot,
        config_path=(Path(plan.lineage.source_root) / "spec_snapshot.yaml").resolve(),
        project_root=project_root.resolve(),
    )
    validate_experiment_spec(spec)
    return spec


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
            emit_machine_payload(
                args,
                build_empty_source_selection_audit(
                    command="resubmit",
                    stage=args.stage,
                    query=args.query,
                    source_root=str(source_root),
                ),
            )
            return
        print(f"[RESUBMIT] selected_runs=0 stage={args.stage} query={args.query}")
        return
    if emit_machine_dry_run_if_requested(
        args, _spec_for_sourced_plan(plan), plan.batch, command="resubmit"
    ):
        return
    print(f"[RESUBMIT] selected_runs={len(plan.selected_runs)} source={source_root}")
    if dry_run_mode_from_args(args):
        print_lines(summarize_stage_batch(plan.batch))
        return

    result = emit_sourced_stage_batch(plan, submit=not args.emit_only)
    print_lines(summarize_stage_batch(result.plan.batch))
    print_sourced_stage_batch_execution_result(result, noun="resubmit batch")


def add_subparser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "resubmit", help="Resubmit one stage from a batch or train/eval pipeline root"
    )
    parser.add_argument(
        "--from",
        dest="root",
        required=True,
        help="Stage batch or train/eval pipeline root",
    )
    parser.add_argument("--stage", required=True, help="Stage name to resubmit")
    parser.add_argument(
        "--query", default="state=failed", help="Selection query, e.g. state=failed"
    )
    parser.add_argument(
        "--run-id", action="append", default=[], help="Select a specific run_id"
    )
    parser.add_argument(
        "--set", action="append", default=[], help="Override config by dot-path"
    )
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
    parser.add_argument(
        "--output",
        default=None,
        help="Write machine-readable dry-run output to this path",
    )
    parser.set_defaults(handler=handle_resubmit)
