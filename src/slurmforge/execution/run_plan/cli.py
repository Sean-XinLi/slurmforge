#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from .api import execute_plan
from .loader import load_plan


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Low-level runtime helper that executes one slurmforge run record. "
            "This command is usually invoked by generated batch scripts, not by end users directly."
        )
    )
    parser.add_argument("--record", required=True, help="Path to run record JSON")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    record_path = Path(args.record).resolve()
    plan = load_plan(record_path)
    sys.exit(execute_plan(plan))


if __name__ == "__main__":
    main()
