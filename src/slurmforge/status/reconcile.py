from __future__ import annotations

from pathlib import Path

from ..io import SchemaVersion, utc_now
from ..plans import StageBatchPlan
from ..slurm import SlurmClient, failure_class_for_slurm_state, stage_state_for_slurm_state
from .machine import commit_stage_status
from .models import StageStatusRecord, TERMINAL_STATES
from .reader import read_stage_status
from .reconcile_state import (
    append_scheduler_observation,
    master_fallback,
    missing_output_expired,
    scheduler_attempt,
    stage_outputs_path,
)


def reconcile_stage_batch_with_slurm(
    batch: StageBatchPlan,
    *,
    group_job_ids: dict[str, str],
    client: SlurmClient,
    missing_output_grace_seconds: int = 300,
) -> None:
    batch_root = Path(batch.submission_root)
    instances_by_id = {instance.stage_instance_id: instance for instance in batch.stage_instances}
    for group in batch.group_plans:
        job_id = group_job_ids.get(group.group_id)
        if not job_id:
            continue
        job_states = client.query_observed_jobs([job_id])
        task_states: dict[int, object] = {}
        prefix = f"{job_id}_"
        for observed_job_id, observed_state in job_states.items():
            if observed_state.array_task_id is None:
                continue
            if observed_state.array_job_id == job_id or observed_job_id.startswith(prefix):
                task_states[observed_state.array_task_id] = observed_state
        fallback_state = master_fallback(job_states, job_id, group_size=group.array_size)
        for task_index, stage_instance_id in enumerate(group.stage_instance_ids):
            instance = instances_by_id[stage_instance_id]
            run_dir = batch_root / instance.run_dir_rel
            current = read_stage_status(run_dir)
            if current is not None and current.state in TERMINAL_STATES:
                continue
            slurm_state = task_states.get(task_index) or fallback_state
            if slurm_state is None:
                continue
            append_scheduler_observation(
                batch_root,
                {
                    "schema_version": SchemaVersion.SCHEDULER_OBSERVATION,
                    "group_id": group.group_id,
                    "stage_instance_id": instance.stage_instance_id,
                    "run_id": instance.run_id,
                    "scheduler_job_id": slurm_state.job_id,
                    "scheduler_array_job_id": slurm_state.array_job_id,
                    "scheduler_array_task_id": slurm_state.array_task_id,
                    "scheduler_state": slurm_state.state,
                    "scheduler_exit_code": slurm_state.exit_code,
                    "reason": slurm_state.reason,
                    "observed_at": utc_now(),
                },
            )
            stage_state = stage_state_for_slurm_state(slurm_state.state)
            if stage_state is None:
                continue
            latest_attempt_id = None
            if stage_state in TERMINAL_STATES or stage_state == "running":
                latest_attempt_id = scheduler_attempt(
                    run_dir,
                    stage_instance_id=instance.stage_instance_id,
                    slurm_state=slurm_state,
                    terminal=stage_state in TERMINAL_STATES,
                )
            if stage_state == "success" and not stage_outputs_path(run_dir).exists():
                if missing_output_expired(
                    run_dir,
                    slurm_state=slurm_state.state,
                    grace_seconds=missing_output_grace_seconds,
                ):
                    stage_state = "failed"
                    failure_class = "missing_attempt_result"
                    reason = f"Slurm job {slurm_state.job_id} completed but no stage_outputs.json was written"
                else:
                    stage_state = "running"
                    failure_class = None
                    reason = (
                        f"Slurm job {slurm_state.job_id} completed; waiting for stage_outputs.json "
                        f"for up to {missing_output_grace_seconds}s"
                    )
            else:
                failure_class = failure_class_for_slurm_state(slurm_state.state)
                reason = f"Slurm job {slurm_state.job_id} state={slurm_state.state}"
                if slurm_state.reason:
                    reason = f"{reason} reason={slurm_state.reason}"
            commit_stage_status(
                run_dir,
                StageStatusRecord(
                    schema_version=SchemaVersion.STATUS,
                    stage_instance_id=instance.stage_instance_id,
                    run_id=instance.run_id,
                    stage_name=instance.stage_name,
                    state=stage_state,
                    latest_attempt_id=latest_attempt_id,
                    failure_class=failure_class,
                    reason=reason,
                ),
                source="slurm_reconcile",
            )
