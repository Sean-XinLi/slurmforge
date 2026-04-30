from __future__ import annotations

from pathlib import Path

from ..storage.plan_reader import run_definitions_from_stage_batch
from ..workflow_contract import EVAL_STAGE, TRAIN_STAGE
from .train_group import group_plan


def eval_shard_root(plan, group_id: str) -> Path:
    return Path(plan.root_dir) / "stage_batches" / EVAL_STAGE / "shards" / group_id


def group_run_definitions(plan, group_id: str):
    train_group = group_plan(plan.stage_batches[TRAIN_STAGE], group_id)
    run_ids = set(train_group.run_ids)
    return tuple(
        run
        for run in run_definitions_from_stage_batch(plan.stage_batches[EVAL_STAGE])
        if run.run_id in run_ids
    )
