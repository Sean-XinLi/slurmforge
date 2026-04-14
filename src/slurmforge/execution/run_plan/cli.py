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
    parser.add_argument(
        "--batch-root",
        required=True,
        help="Absolute path to the batch root directory",
    )
    parser.add_argument(
        "--group-index",
        required=True,
        type=int,
        help="Array group index (1-based, matches the sbatch array group number)",
    )
    parser.add_argument(
        "--task-index",
        required=True,
        type=int,
        help="Array task index within the group (0-based, matches SLURM_ARRAY_TASK_ID)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    batch_root = Path(args.batch_root).resolve()
    plan = load_plan(batch_root, group_index=args.group_index, task_index=args.task_index)
    sys.exit(execute_plan(plan, batch_root=batch_root))


if __name__ == "__main__":
    main()
