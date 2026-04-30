from __future__ import annotations

from pathlib import Path
from typing import Any

from ..errors import ConfigContractError
from ..root_model.snapshots import (
    refresh_stage_batch_status,
    refresh_train_eval_pipeline_status,
)
from ..slurm import SlurmClientProtocol
from ..status.models import TERMINAL_STATES
from ..status.reader import read_stage_status
from ..status.reconcile import reconcile_stage_batch_with_slurm
from ..submission.ledger import submitted_group_job_ids
from ..workflow_contract import TRAIN_GROUP_TRAIN_SUBMITTED
from .state_model import submitted_gate_job_ids
from .state_records import TrainGroupState, WorkflowState, set_train_group


def group_plan(batch, group_id: str):
    for group in batch.group_plans:
        if group.group_id == group_id:
            return group
    raise ConfigContractError(
        f"Stage `{batch.stage_name}` does not have group `{group_id}`"
    )


def group_run_dirs(batch, group_id: str) -> list[Path]:
    group = group_plan(batch, group_id)
    instances_by_id = {
        instance.stage_instance_id: instance for instance in batch.stage_instances
    }
    batch_root = Path(batch.submission_root)
    return [
        batch_root / instances_by_id[stage_instance_id].run_dir_rel
        for stage_instance_id in group.stage_instance_ids
    ]


def group_terminal(batch, group_id: str) -> bool:
    statuses = [read_stage_status(run_dir) for run_dir in group_run_dirs(batch, group_id)]
    return bool(statuses) and all(
        status is not None and status.state in TERMINAL_STATES for status in statuses
    )


def initialize_train_groups(state: WorkflowState, train_batch) -> None:
    for group in train_batch.group_plans:
        existing = state.train_groups.get(group.group_id)
        set_train_group(
            state,
            TrainGroupState(
                group_id=group.group_id,
                state=existing.state
                if existing is not None
                else TRAIN_GROUP_TRAIN_SUBMITTED,
                run_ids=tuple(group.run_ids),
                stage_instance_ids=tuple(group.stage_instance_ids),
                train_group_gate_key=""
                if existing is None
                else existing.train_group_gate_key,
                eval_shard_root="" if existing is None else existing.eval_shard_root,
                eval_shard_gate_key=""
                if existing is None
                else existing.eval_shard_gate_key,
                terminal_dependency_gate_key=""
                if existing is None
                else existing.terminal_dependency_gate_key,
                eval_shard_group_count=0
                if existing is None
                else existing.eval_shard_group_count,
            ),
        )


def submitted_train_group_job_id(train_batch, group_id: str) -> str:
    job_ids = submitted_group_job_ids(Path(train_batch.submission_root))
    if group_id not in job_ids:
        raise ConfigContractError(
            f"Train group `{group_id}` has no submitted scheduler job id"
        )
    return job_ids[group_id]


def reconcile_train_group(
    pipeline_root: Path,
    train_batch,
    group_id: str,
    *,
    client: SlurmClientProtocol,
    missing_output_grace_seconds: int,
) -> None:
    reconcile_stage_batch_with_slurm(
        train_batch,
        group_job_ids={group_id: submitted_train_group_job_id(train_batch, group_id)},
        client=client,
        missing_output_grace_seconds=missing_output_grace_seconds,
    )
    refresh_stage_batch_status(Path(train_batch.submission_root))
    refresh_train_eval_pipeline_status(pipeline_root)


def terminal_gate_job_ids(
    pipeline_root: Path, state: WorkflowState
) -> tuple[str, ...] | None:
    groups = state.train_groups
    if not groups:
        return None
    gate_jobs = submitted_gate_job_ids(pipeline_root)
    job_ids: list[str] = []
    for _, record in sorted(groups.items()):
        gate_key = record.terminal_dependency_gate_key
        if not gate_key or gate_key not in gate_jobs:
            return None
        job_ids.append(gate_jobs[gate_key])
    return tuple(job_ids)


def all_groups_have_terminal_dependencies(
    pipeline_root: Path, state: WorkflowState
) -> bool:
    return terminal_gate_job_ids(pipeline_root, state) is not None
