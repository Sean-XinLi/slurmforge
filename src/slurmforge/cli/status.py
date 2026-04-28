"""``sforge status`` -- summarize stage-level status records."""

from __future__ import annotations

import argparse
from pathlib import Path

from ..orchestration.status_view import render_status_lines


def handle_status(args: argparse.Namespace) -> None:
    for line in render_status_lines(
        root=Path(args.root).resolve(),
        status_query=args.query,
        stage=args.stage,
        reconcile=args.reconcile,
        missing_output_grace_seconds=args.missing_output_grace_seconds,
    ):
        print(line)


def add_subparser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "status",
        help="Summarize stage-level status for a batch or train/eval pipeline root",
    )
    parser.add_argument(
        "--from",
        dest="root",
        required=True,
        help="Stage batch or train/eval pipeline root",
    )
    parser.add_argument("--stage", default=None, help="Filter by stage name")
    parser.add_argument(
        "--query",
        "--status",
        dest="query",
        default="all",
        help="Filter, e.g. state=failed or failed",
    )
    parser.add_argument(
        "--reconcile",
        action="store_true",
        help="Query Slurm through submission ledgers before printing",
    )
    parser.add_argument(
        "--missing-output-grace-seconds",
        type=int,
        default=300,
        help="Grace period used by --reconcile before classifying missing stage_outputs.json",
    )
    parser.set_defaults(handler=handle_status)
