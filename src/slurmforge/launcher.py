#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys

from .cli.estimate import add_subparser as add_estimate_subparser
from .cli.eval import add_subparser as add_eval_subparser
from .cli.init import add_subparser as add_init_subparser
from .cli.plan import add_subparser as add_plan_subparser
from .cli.resubmit import add_subparser as add_resubmit_subparser
from .cli.run import add_subparser as add_run_subparser
from .cli.status import add_subparser as add_status_subparser
from .cli.train import add_subparser as add_train_subparser
from .cli.validate import add_subparser as add_validate_subparser
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
    add_init_subparser(subparsers)
    add_validate_subparser(subparsers)
    add_estimate_subparser(subparsers)
    add_plan_subparser(subparsers)
    add_train_subparser(subparsers)
    add_eval_subparser(subparsers)
    add_run_subparser(subparsers)
    add_status_subparser(subparsers)
    add_resubmit_subparser(subparsers)
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
