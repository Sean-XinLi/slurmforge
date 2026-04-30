from __future__ import annotations

import sys
from pathlib import Path

from ..io import read_json
from ..workflow_contract import (
    DISPATCH_CATCHUP_GATE,
    FINAL_GATE,
    STAGE_INSTANCE_GATE,
)
from .workflow import advance_pipeline_once


def run_gate(
    pipeline_root: Path,
    *,
    gate: str | None = None,
    event: str | None = None,
    stage_instance_id: str | None = None,
    task_map: Path | None = None,
) -> int:
    if stage_instance_id is None and task_map is not None:
        payload = read_json(task_map)
        stage_instance_id = str(dict(payload["tasks"])[_slurm_array_task_id()])
    advance_pipeline_once(
        pipeline_root,
        gate=gate,
        event=event,
        stage_instance_id=stage_instance_id,
    )
    return 0


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Advance one short-lived slurmforge train/eval pipeline gate"
    )
    parser.add_argument("--pipeline-root", required=True)
    parser.add_argument(
        "--gate",
        default=None,
        choices=(STAGE_INSTANCE_GATE, DISPATCH_CATCHUP_GATE, FINAL_GATE),
    )
    parser.add_argument(
        "--event",
        default=None,
        choices=("stage-instance-finished",),
    )
    parser.add_argument("--stage-instance-id", default=None)
    parser.add_argument("--task-map", default=None)
    args = parser.parse_args(argv)
    raise SystemExit(
        run_gate(
            Path(args.pipeline_root).resolve(),
            gate=args.gate,
            event=args.event,
            stage_instance_id=args.stage_instance_id,
            task_map=None if args.task_map is None else Path(args.task_map).resolve(),
        )
    )


def _slurm_array_task_id() -> str:
    import os

    task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
    if not task_id:
        raise RuntimeError("SLURM_ARRAY_TASK_ID is required when --task-map is used")
    return task_id


if __name__ == "__main__":
    main(sys.argv[1:])
