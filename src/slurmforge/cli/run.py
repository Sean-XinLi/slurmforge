"""``sforge run`` -- submit a train/eval pipeline controller."""
from __future__ import annotations

import argparse

from ..orchestration import execute_train_eval_pipeline_plan
from .stage_common import (
    add_config_args,
    add_execution_mode_args,
    build_train_eval_pipeline_from_args,
    emit_machine_dry_run_if_requested,
    execution_mode_from_args,
    print_train_eval_pipeline_plan,
)


def handle_run(args: argparse.Namespace) -> None:
    spec, plan = build_train_eval_pipeline_from_args(args)
    if emit_machine_dry_run_if_requested(args, spec, plan, command="run"):
        return
    print_train_eval_pipeline_plan(plan)
    execute_train_eval_pipeline_plan(spec, plan, mode=execution_mode_from_args(args))


def add_subparser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("run", help="Submit a train/eval pipeline controller")
    add_config_args(parser)
    add_execution_mode_args(parser)
    parser.set_defaults(handler=handle_run)
