#!/usr/bin/env python3
from __future__ import annotations

import argparse

from .cli import examples, generate, init, replay, rerun, status, validate


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Slurm-oriented experiment orchestration CLI. "
            "Start with `sforge init` for a starter project scaffold or "
            "`sforge examples` for raw YAML references."
        )
    )
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True
    generate.add_subparser(subparsers)
    examples.add_subparser(subparsers)
    init.add_subparser(subparsers)
    replay.add_subparser(subparsers)
    rerun.add_subparser(subparsers)
    status.add_subparser(subparsers)
    validate.add_subparser(subparsers)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.handler(args)


if __name__ == "__main__":
    main()
