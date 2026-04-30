"""``sforge run`` -- submit a streaming train/eval pipeline."""

from __future__ import annotations

import argparse

from ..orchestration.launch import execute_train_eval_pipeline_plan
from .args import add_config_args, add_execution_mode_args, execution_mode_from_args
from .builders import build_train_eval_pipeline_from_args
from .dry_run import emit_machine_dry_run_if_requested
from .render import (
    print_train_eval_pipeline_execution_result,
    print_train_eval_pipeline_plan,
)


def handle_run(args: argparse.Namespace) -> None:
    spec, plan = build_train_eval_pipeline_from_args(args)
    if emit_machine_dry_run_if_requested(args, spec, plan, command="run"):
        return
    print_train_eval_pipeline_plan(plan)
    print_train_eval_pipeline_execution_result(
        execute_train_eval_pipeline_plan(
            spec, plan, mode=execution_mode_from_args(args)
        )
    )


def add_subparser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "run", help="Submit a streaming train/eval pipeline"
    )
    add_config_args(parser)
    add_execution_mode_args(parser)
    parser.set_defaults(handler=handle_run)
