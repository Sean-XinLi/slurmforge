from __future__ import annotations

from pathlib import Path

from ..emit.pipeline_gate import write_stage_instance_gate_array_submit_file
from ..slurm import SlurmClientProtocol, SlurmSubmitOptions
from ..submission.dependency_tree import MAX_DEPENDENCY_LENGTH
from ..workflow_contract import DISPATCH_CATCHUP_GATE
from .gates import submit_control_gate
from .stage_submit import ensure_stage_submitted
from .state import record_workflow_event
from .state_records import (
    DISPATCH_SUBMITTED,
    INSTANCE_SUBMITTED,
    DispatchSubmissionState,
    WorkflowState,
    set_submission,
)


def submit_dispatch(
    pipeline_root: Path,
    plan,
    state: WorkflowState,
    batch,
    *,
    submission_id: str,
    client: SlurmClientProtocol,
    max_dependency_length: int = MAX_DEPENDENCY_LENGTH,
) -> dict[str, str]:
    group_job_ids = ensure_stage_submitted(
        pipeline_root,
        batch,
        client=client,
        state_dispatch_id=submission_id,
    )
    record_dispatch_submission(
        state,
        batch,
        submission_id=submission_id,
        group_job_ids=group_job_ids,
        budgeted_gpus=_batch_budgeted_gpus(batch),
    )
    submit_dispatch_gates(
        pipeline_root,
        plan,
        state,
        batch,
        submission_id=submission_id,
        group_job_ids=group_job_ids,
        client=client,
        max_dependency_length=max_dependency_length,
    )
    return group_job_ids


def record_dispatch_submission(
    state: WorkflowState,
    batch,
    *,
    submission_id: str,
    group_job_ids: dict[str, str],
    budgeted_gpus: int,
) -> None:
    ordered_job_ids = tuple(
        group_job_ids.get(group.group_id, "") for group in batch.group_plans
    )
    submission = DispatchSubmissionState(
        submission_id=submission_id,
        stage_name=batch.stage_name,
        instance_ids=tuple(
            instance.stage_instance_id for instance in batch.stage_instances
        ),
        root=str(Path(batch.submission_root).resolve()),
        scheduler_job_ids=ordered_job_ids,
        budgeted_gpus=budgeted_gpus,
        state=DISPATCH_SUBMITTED,
    )
    set_submission(state, submission)
    jobs_by_instance: dict[str, tuple[str, str]] = {}
    for group_index, group in enumerate(batch.group_plans):
        job_id = ordered_job_ids[group_index]
        for task_index, instance_id in enumerate(group.stage_instance_ids):
            jobs_by_instance[instance_id] = (
                job_id,
                str(task_index) if group.array_size > 1 else "",
            )
    for instance_id in submission.instance_ids:
        instance = state.instances[instance_id]
        instance.submission_id = submission_id
        instance.state = INSTANCE_SUBMITTED
        job_id, task_id = jobs_by_instance.get(instance_id, ("", ""))
        instance.scheduler_job_id = job_id
        instance.scheduler_array_task_id = task_id


def submit_dispatch_gates(
    pipeline_root: Path,
    plan,
    state: WorkflowState,
    batch,
    *,
    submission_id: str,
    group_job_ids: dict[str, str],
    client: SlurmClientProtocol,
    max_dependency_length: int,
) -> None:
    for group in batch.group_plans:
        scheduler_job_id = group_job_ids.get(group.group_id)
        if not scheduler_job_id:
            continue
        gate_path = write_stage_instance_gate_array_submit_file(
            plan,
            submission_id=submission_id,
            group_id=group.group_id,
            stage_instance_ids=tuple(group.stage_instance_ids),
        )
        gate_job_id = client.submit(
            gate_path,
            options=SlurmSubmitOptions(dependency=f"aftercorr:{scheduler_job_id}"),
        )
        record_workflow_event(
            pipeline_root,
            "stage_instance_gate_submitted",
            submission_id=submission_id,
            stage=batch.stage_name,
            group_id=group.group_id,
            scheduler_job_id=gate_job_id,
            dependency_job_id=scheduler_job_id,
        )
    if group_job_ids:
        submit_control_gate(
            pipeline_root,
            state,
            plan,
            DISPATCH_CATCHUP_GATE,
            target_kind="dispatch",
            target_id=submission_id,
            dependency_job_ids=tuple(group_job_ids.values()),
            client=client,
            max_dependency_length=max_dependency_length,
        )


def _batch_budgeted_gpus(batch) -> int:
    if batch.budget_plan.max_available_gpus <= 0:
        return 0
    total = 0
    for group in batch.group_plans:
        throttle = group.array_throttle or group.array_size
        total += group.gpus_per_task * throttle
    return total
