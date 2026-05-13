from __future__ import annotations

from pathlib import Path

from ..plans.stage import StageBatchPlan
from ..slurm import SlurmClientProtocol
from ..status.models import TERMINAL_STATES
from ..status.reader import read_stage_status
from ..status.reconcile import reconcile_stage_batch_with_slurm
from ..storage.plan_reader import load_execution_stage_batch_plan
from ..storage.paths import stage_outputs_path
from ..storage.workflow_state_constants import (
    DISPATCH_ACTIVE_STATES,
    DISPATCH_FAILED,
    DISPATCH_SUBMITTED,
    DISPATCH_TERMINAL,
    INSTANCE_BLOCKED,
    INSTANCE_FAILED,
    INSTANCE_PLANNED,
    INSTANCE_RUNNING,
    INSTANCE_SKIPPED,
    INSTANCE_SUBMITTED,
    INSTANCE_SUCCESS,
    INSTANCE_TERMINAL_STATES,
)
from ..storage.workflow_state_models import (
    DispatchSubmissionState,
    WorkflowState,
)


def reconcile_instances(
    pipeline_root: Path,
    state: WorkflowState,
    *,
    client: SlurmClientProtocol,
    missing_output_grace_seconds: int = 300,
) -> WorkflowState:
    for submission in list(state.submissions.values()):
        if not submission.root:
            continue
        batch_root = Path(submission.root)
        batch = load_execution_stage_batch_plan(batch_root)
        group_job_ids = _group_job_ids(batch, submission)
        if group_job_ids and submission.state in DISPATCH_ACTIVE_STATES:
            reconcile_stage_batch_with_slurm(
                batch,
                group_job_ids=group_job_ids,
                client=client,
                missing_output_grace_seconds=missing_output_grace_seconds,
            )
        _sync_batch_statuses(state, batch, submission)
        _sync_group_states(state, submission)
        _sync_submission_state(submission)
    return state


def sync_materialized_statuses(state: WorkflowState, batch: StageBatchPlan) -> None:
    for instance in batch.stage_instances:
        record = state.instances.get(instance.stage_instance_id)
        if record is None:
            continue
        status = read_stage_status(Path(batch.submission_root) / instance.run_dir_rel)
        if status is None:
            continue
        _apply_status(record, status.state, Path(batch.submission_root) / instance.run_dir_rel, status.reason)


def _group_job_ids(
    batch: StageBatchPlan, submission: DispatchSubmissionState
) -> dict[str, str]:
    return {
        group.group_id: submission.groups[group.group_id].scheduler_job_id
        for group in batch.group_plans
        if group.group_id in submission.groups
        and submission.groups[group.group_id].scheduler_job_id
    }


def _sync_batch_statuses(
    state: WorkflowState,
    batch: StageBatchPlan,
    submission: DispatchSubmissionState,
) -> None:
    instance_to_job: dict[str, tuple[str, str]] = {}
    for group in batch.group_plans:
        submitted_group = submission.groups.get(group.group_id)
        job_id = "" if submitted_group is None else submitted_group.scheduler_job_id
        for task_index, stage_instance_id in enumerate(group.stage_instance_ids):
            if submitted_group is not None:
                array_task_id = submitted_group.task_ids_by_instance.get(
                    stage_instance_id, ""
                )
            else:
                array_task_id = str(task_index) if group.array_size > 1 else ""
            instance_to_job[stage_instance_id] = (job_id, array_task_id)

    for instance in batch.stage_instances:
        record = state.instances.get(instance.stage_instance_id)
        if record is None:
            continue
        job_id, task_id = instance_to_job.get(instance.stage_instance_id, ("", ""))
        if submission.submission_id and not record.submission_id:
            record.submission_id = submission.submission_id
        if job_id:
            record.scheduler_job_id = job_id
        if task_id:
            record.scheduler_array_task_id = task_id
        run_dir = Path(batch.submission_root) / instance.run_dir_rel
        status = read_stage_status(run_dir)
        if status is None:
            if record.state == INSTANCE_PLANNED:
                record.state = INSTANCE_SUBMITTED
            continue
        _apply_status(record, status.state, run_dir, status.reason)


def _apply_status(record, status_state: str, run_dir: Path, reason: str) -> None:
    if status_state == "success":
        record.state = INSTANCE_SUCCESS
        record.output_ready = stage_outputs_path(run_dir).exists()
        record.reason = "" if record.output_ready else "success without stage_outputs.json"
        return
    if status_state in {"failed", "cancelled"}:
        record.state = INSTANCE_FAILED
        record.output_ready = False
        record.reason = reason
        return
    if status_state == "blocked":
        record.state = INSTANCE_BLOCKED
        record.output_ready = False
        record.reason = reason
        return
    if status_state == "skipped":
        record.state = INSTANCE_SKIPPED
        record.output_ready = False
        record.reason = reason
        return
    if status_state == "running":
        record.state = INSTANCE_RUNNING
        record.reason = reason
        return
    if status_state == "queued":
        if record.state not in INSTANCE_TERMINAL_STATES:
            record.state = INSTANCE_SUBMITTED
            record.reason = reason
        return
    if status_state in TERMINAL_STATES:
        record.state = INSTANCE_FAILED
        record.output_ready = False
        record.reason = reason or f"terminal status `{status_state}`"


def _sync_group_states(state: WorkflowState, submission: DispatchSubmissionState) -> None:
    for group in submission.groups.values():
        records = [
            state.instances[instance_id]
            for instance_id in group.stage_instance_ids
            if instance_id in state.instances
        ]
        if not records:
            continue
        if any(record.state == INSTANCE_FAILED for record in records):
            group.state = DISPATCH_FAILED
            continue
        if all(record.state in INSTANCE_TERMINAL_STATES for record in records):
            group.state = DISPATCH_TERMINAL
            continue
        group.state = DISPATCH_SUBMITTED


def _sync_submission_state(submission: DispatchSubmissionState) -> None:
    if not submission.groups:
        return
    group_states = [group.state for group in submission.groups.values()]
    if any(state == DISPATCH_FAILED for state in group_states):
        submission.state = DISPATCH_FAILED
        return
    if all(state == DISPATCH_TERMINAL for state in group_states):
        submission.state = DISPATCH_TERMINAL
        return
    submission.state = DISPATCH_SUBMITTED
