from __future__ import annotations

import sys
from pathlib import Path

from ..plans.train_eval import EVAL_SHARD_GATE, FINAL_GATE, TRAIN_GROUP_GATE
from .workflow import advance_pipeline_once


def run_gate(pipeline_root: Path, *, gate: str, group_id: str | None = None) -> int:
    advance_pipeline_once(pipeline_root, gate=gate, group_id=group_id)
    return 0


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Advance one short-lived slurmforge train/eval pipeline gate"
    )
    parser.add_argument("--pipeline-root", required=True)
    parser.add_argument(
        "--gate",
        required=True,
        choices=(TRAIN_GROUP_GATE, EVAL_SHARD_GATE, FINAL_GATE),
    )
    parser.add_argument("--group-id", default=None)
    args = parser.parse_args(argv)
    raise SystemExit(
        run_gate(
            Path(args.pipeline_root).resolve(),
            gate=args.gate,
            group_id=args.group_id,
        )
    )


if __name__ == "__main__":
    main(sys.argv[1:])
