"""``sforge train`` -- submit only the train stage batch."""
from __future__ import annotations

import argparse

from ..orchestration import execute_stage_batch_plan
from .args import add_config_args, add_execution_mode_args, execution_mode_from_args
from .builders import build_train_batch_from_args
from .dry_run import emit_machine_dry_run_if_requested
from .render import print_stage_batch_execution_result, print_stage_batch_plan


def handle_train(args: argparse.Namespace) -> None:
    spec, batch = build_train_batch_from_args(args)
    if emit_machine_dry_run_if_requested(args, spec, batch, command="train"):
        return
    print_stage_batch_plan(batch)
    print_stage_batch_execution_result(execute_stage_batch_plan(spec, batch, mode=execution_mode_from_args(args)))


def add_subparser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("train", help="Submit a train-only stage batch")
    add_config_args(parser)
    add_execution_mode_args(parser)
    parser.set_defaults(handler=handle_train)
