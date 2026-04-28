"""``sforge plan`` -- compile a stage batch or pipeline without submitting."""

from __future__ import annotations

import argparse

from ..errors import ConfigContractError
from ..orchestration.launch import (
    execute_stage_batch_plan,
    execute_train_eval_pipeline_plan,
)
from ..orchestration.pipeline_build import build_train_eval_pipeline_plan
from ..orchestration.stage_build import build_eval_stage_batch, build_train_stage_batch
from .args import add_config_args, add_eval_source_args, execution_mode_from_args
from .builders import load_spec_from_args
from .dry_run import emit_machine_dry_run_if_requested
from .render import (
    print_stage_batch_execution_result,
    print_train_eval_pipeline_plan,
    print_train_eval_pipeline_execution_result,
    print_stage_batch_plan,
)
from .requests import eval_source_from_args


def handle_plan(args: argparse.Namespace) -> None:
    spec = load_spec_from_args(args)
    command = str(args.plan_command)
    if command == "run":
        pipeline_plan = build_train_eval_pipeline_plan(spec)
        if emit_machine_dry_run_if_requested(args, spec, pipeline_plan, command="run"):
            return
        print_train_eval_pipeline_plan(pipeline_plan)
        print_train_eval_pipeline_execution_result(
            execute_train_eval_pipeline_plan(
                spec, pipeline_plan, mode=execution_mode_from_args(args, default="emit")
            )
        )
        return
    if command == "train":
        batch = build_train_stage_batch(spec)
    else:
        source = eval_source_from_args(args)
        batch = build_eval_stage_batch(
            spec,
            from_train_batch=source.value
            if source is not None and source.kind == "from_train_batch"
            else None,
            from_run=source.value
            if source is not None and source.kind == "from_run"
            else None,
            checkpoint=source.value
            if source is not None and source.kind == "checkpoint"
            else None,
            input_name=None if source is None else source.input_name,
            allow_unresolved=source is None,
        )
    if emit_machine_dry_run_if_requested(args, spec, batch, command=command):
        return
    print_stage_batch_plan(batch)
    if command == "eval" and eval_source_from_args(args) is None:
        if execution_mode_from_args(args, default="emit") == "preview":
            return
        raise ConfigContractError(
            "eval plan has unresolved required inputs; provide --checkpoint, --from-run, "
            "or --from-train-batch, or use --dry-run for logical preview only"
        )
    print_stage_batch_execution_result(
        execute_stage_batch_plan(
            spec, batch, mode=execution_mode_from_args(args, default="emit")
        )
    )


def _add_plan_mode_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        nargs="?",
        const="summary",
        default=False,
        choices=("summary", "json", "full"),
        help="Print the plan without writing files; json/full produce machine-readable audit output",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Write machine-readable dry-run output to this path",
    )


def add_subparser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "plan", help="Compile plan and sbatch files without submitting"
    )
    plan_subparsers = parser.add_subparsers(dest="plan_command")
    plan_subparsers.required = True

    train_parser = plan_subparsers.add_parser(
        "train", help="Compile a train stage batch"
    )
    add_config_args(train_parser)
    _add_plan_mode_args(train_parser)
    train_parser.set_defaults(handler=handle_plan, plan_command="train")

    eval_parser = plan_subparsers.add_parser("eval", help="Compile an eval stage batch")
    add_config_args(eval_parser)
    add_eval_source_args(eval_parser, required=False)
    _add_plan_mode_args(eval_parser)
    eval_parser.set_defaults(handler=handle_plan, plan_command="eval")

    run_parser = plan_subparsers.add_parser(
        "run", help="Compile a train/eval pipeline controller plan"
    )
    add_config_args(run_parser)
    _add_plan_mode_args(run_parser)
    run_parser.set_defaults(handler=handle_plan, plan_command="run")
