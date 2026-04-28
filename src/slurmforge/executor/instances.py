from __future__ import annotations

from pathlib import Path

from ..io import read_json
from ..plans.stage import StageInstancePlan
from ..plans.serde import stage_instance_plan_from_dict
from ..storage.loader import load_execution_stage_batch_plan


def _stage_plan_path(run_dir: Path) -> Path:
    return run_dir / "stage_plan.json"


def find_stage_instance(batch_root: Path, group_index: int, task_index: int) -> StageInstancePlan:
    batch = load_execution_stage_batch_plan(batch_root)
    group = next((item for item in batch.group_plans if item.group_index == group_index), None)
    if group is None:
        raise ValueError(f"No group_index={group_index} in {batch_root}")
    if task_index < 0 or task_index >= len(group.stage_instance_ids):
        raise ValueError(f"task_index={task_index} outside group array size {len(group.stage_instance_ids)}")
    target = group.stage_instance_ids[task_index]
    return next(item for item in batch.stage_instances if item.stage_instance_id == target)


def load_stage_instance_from_run_dir(run_dir: Path) -> StageInstancePlan:
    return stage_instance_plan_from_dict(read_json(_stage_plan_path(run_dir)))
