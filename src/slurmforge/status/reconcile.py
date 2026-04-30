from __future__ import annotations

from pathlib import Path

from ..io import SchemaVersion, utc_now
from ..plans.stage import StageBatchPlan
from ..slurm import SlurmClientProtocol
from .machine import commit_stage_status
from .models import StageStatusRecord, TERMINAL_STATES
from .reader import read_stage_status
from .reconcile_attempts import scheduler_attempt
from .reconcile_decision import decide_stage_status, scheduler_stage_state
from .reconcile_observations import append_scheduler_observation
from .slurm_observations import observe_group_slurm_state, observed_task_state


def reconcile_stage_batch_with_slurm(
    batch: StageBatchPlan,
    *,
    group_job_ids: dict[str, str],
    client: SlurmClientProtocol,
    missing_output_grace_seconds: int = 300,
) -> None:
    batch_root = Path(batch.submission_root)
    instances_by_id = {
        instance.stage_instance_id: instance for instance in batch.stage_instances
    }
    for group in batch.group_plans:
        job_id = group_job_ids.get(group.group_id)
        if not job_id:
            continue
        observation = observe_group_slurm_state(
            client=client,
            job_id=job_id,
            group_size=group.array_size,
        )
        for task_index, stage_instance_id in enumerate(group.stage_instance_ids):
            instance = instances_by_id[stage_instance_id]
            run_dir = batch_root / instance.run_dir_rel
            current = read_stage_status(run_dir)
            if current is not None and current.state in TERMINAL_STATES:
                continue
            slurm_state = observed_task_state(observation, task_index)
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
            stage_state = scheduler_stage_state(slurm_state)
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
            decision = decide_stage_status(
                run_dir=run_dir,
                slurm_state=slurm_state,
                initial_stage_state=stage_state,
                missing_output_grace_seconds=missing_output_grace_seconds,
            )
            commit_stage_status(
                run_dir,
                StageStatusRecord(
                    schema_version=SchemaVersion.STATUS,
                    stage_instance_id=instance.stage_instance_id,
                    run_id=instance.run_id,
                    stage_name=instance.stage_name,
                    state=decision.stage_state,
                    latest_attempt_id=latest_attempt_id,
                    failure_class=decision.failure_class,
                    reason=decision.reason,
                ),
                source="slurm_reconcile",
            )
