"""``sforge eval`` -- submit only the eval stage batch."""
from __future__ import annotations

import argparse

from ..orchestration import execute_stage_batch_plan
from .args import add_config_args, add_eval_source_args, add_execution_mode_args, execution_mode_from_args
from .builders import build_eval_batch_from_args
from .dry_run import emit_machine_dry_run_if_requested
from .render import print_stage_batch_execution_result, print_stage_batch_plan


def handle_eval(args: argparse.Namespace) -> None:
    spec, batch = build_eval_batch_from_args(args)
    if emit_machine_dry_run_if_requested(args, spec, batch, command="eval"):
        return
    print_stage_batch_plan(batch)
    print_stage_batch_execution_result(execute_stage_batch_plan(spec, batch, mode=execution_mode_from_args(args)))


def add_subparser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("eval", help="Submit an eval-only stage batch")
    add_config_args(parser)
    add_eval_source_args(parser, required=True)
    add_execution_mode_args(parser)
    parser.set_defaults(handler=handle_eval)
