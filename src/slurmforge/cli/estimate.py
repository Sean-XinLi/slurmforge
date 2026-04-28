"""``sforge estimate`` -- preview declared resource sizing and GPU budget."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..io import to_jsonable
from ..orchestration.estimate import (
    build_resource_estimate_for_plan,
    render_resource_estimate_for_plan,
)
from ..orchestration.pipeline_build import build_train_eval_pipeline_plan
from ..orchestration.stage_build import build_eval_stage_batch, build_train_stage_batch
from .args import add_config_args
from .builders import load_spec_from_args
from .render import print_lines


def _build_estimate_plan(spec):
    if spec.stage_order() == ("train", "eval"):
        return build_train_eval_pipeline_plan(spec)
    if "train" in spec.enabled_stages:
        return build_train_stage_batch(spec)
    return build_eval_stage_batch(spec, allow_unresolved=True)


def handle_estimate(args: argparse.Namespace) -> None:
    spec = load_spec_from_args(args)
    plan = _build_estimate_plan(spec)
    estimate = build_resource_estimate_for_plan(plan)
    if args.json:
        payload = json.dumps(to_jsonable(estimate), indent=2, sort_keys=True) + "\n"
        if args.output:
            Path(args.output).write_text(payload, encoding="utf-8")
        else:
            print(payload, end="")
        return
    print_lines(render_resource_estimate_for_plan(plan))


def add_subparser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "estimate", help="Preview runs, GPU sizing, and dispatch waves"
    )
    add_config_args(parser)
    parser.add_argument(
        "--json", action="store_true", help="Write machine-readable estimate JSON"
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Write machine-readable estimate JSON to this path",
    )
    parser.set_defaults(handler=handle_estimate)
