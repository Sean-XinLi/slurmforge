#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys

from .cli import estimate, eval as eval_cmd
from .cli import init, plan, resubmit, run, status, train, validate
from .errors import UserFacingError
from .identity import PACKAGE_NAME, __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Slurm-oriented stage-batch pipeline CLI for training and evaluation workflows."
        )
    )
    parser.add_argument(
        "--version",
        "-V",
        action="version",
        version=f"{PACKAGE_NAME} {__version__}",
    )
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True
    init.add_subparser(subparsers)
    validate.add_subparser(subparsers)
    estimate.add_subparser(subparsers)
    plan.add_subparser(subparsers)
    train.add_subparser(subparsers)
    eval_cmd.add_subparser(subparsers)
    run.add_subparser(subparsers)
    status.add_subparser(subparsers)
    resubmit.add_subparser(subparsers)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.handler(args)
    except UserFacingError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
