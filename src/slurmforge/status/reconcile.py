from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any

from ..io import SchemaVersion, read_json, utc_now, write_json
from ..plans import StageBatchPlan
from ..slurm import SlurmClient, failure_class_for_slurm_state, stage_state_for_slurm_state
from .machine import (
    commit_attempt,
    commit_stage_status,
    list_attempts,
    read_stage_status,
)
from .models import StageAttemptRecord, StageStatusRecord, TERMINAL_STATES


def _attempts_dir(run_dir: Path) -> Path:
    return Path(run_dir) / "attempts"


def _next_attempt_id(run_dir: Path) -> str:
    root = _attempts_dir(run_dir)
    if not root.exists():
        return "0001"
    existing = [int(path.name) for path in root.iterdir() if path.is_dir() and path.name.isdigit()]
    return f"{(max(existing) + 1) if existing else 1:04d}"


def _stage_outputs_path(run_dir: Path) -> Path:
    return Path(run_dir) / "stage_outputs.json"


def _reconcile_path(run_dir: Path) -> Path:
    return run_dir / "reconcile.json"


def _scheduler_observations_path(batch_root: Path) -> Path:
    return batch_root / "scheduler_observations.jsonl"


def _append_scheduler_observation(batch_root: Path, payload: dict[str, Any]) -> None:
    path = _scheduler_observations_path(batch_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _age_seconds(timestamp: str) -> float:
    started = datetime.datetime.fromisoformat(timestamp)
    now = datetime.datetime.now(datetime.timezone.utc)
    return (now - started).total_seconds()


def _missing_output_expired(run_dir: Path, *, slurm_state: str, grace_seconds: int) -> bool:
    path = _reconcile_path(run_dir)
    if path.exists():
        payload = read_json(path)
        first_missing = str(payload.get("first_missing_output_at") or utc_now())
    else:
        first_missing = utc_now()
        write_json(
            path,
            {
                "schema_version": SchemaVersion.STATUS,
                "first_missing_output_at": first_missing,
                "grace_seconds": grace_seconds,
                "slurm_state": slurm_state,
            },
        )
    return _age_seconds(first_missing) >= grace_seconds


def _exit_code_from_slurm(value: str) -> int | None:
    if not value:
        return None
    raw = value.split(":", 1)[0]
    try:
        return int(raw)
    except ValueError:
        return None


def _reconciled_attempt(
    *,
    attempt_id: str,
    stage_instance_id: str,
    slurm_state,
    previous: StageAttemptRecord | None = None,
) -> StageAttemptRecord:
    now = utc_now()
    return StageAttemptRecord(
        attempt_id=attempt_id,
        stage_instance_id=stage_instance_id,
        attempt_source="scheduler_reconcile" if previous is None else previous.attempt_source,
        attempt_state="reconciled",
        scheduler_job_id=slurm_state.job_id,
        scheduler_array_job_id=slurm_state.array_job_id or "",
        scheduler_array_task_id="" if slurm_state.array_task_id is None else str(slurm_state.array_task_id),
        scheduler_state=slurm_state.state,
        scheduler_exit_code=slurm_state.exit_code,
        node_list="" if previous is None else previous.node_list,
        started_by_executor=False if previous is None else previous.started_by_executor,
        executor_started_at="" if previous is None else previous.executor_started_at,
        executor_finished_at="" if previous is None else previous.executor_finished_at,
        started_at=now if previous is None or not previous.started_at else previous.started_at,
        finished_at=now,
        exit_code=_exit_code_from_slurm(slurm_state.exit_code),
        failure_class=failure_class_for_slurm_state(slurm_state.state),
        reason=f"reconciled from Slurm job {slurm_state.job_id} state={slurm_state.state}",
        log_paths=() if previous is None else previous.log_paths,
        artifact_paths=() if previous is None else previous.artifact_paths,
        artifact_manifest_path="" if previous is None else previous.artifact_manifest_path,
    )


def _scheduler_attempt(run_dir: Path, *, stage_instance_id: str, slurm_state, terminal: bool) -> str | None:
    for attempt in list_attempts(run_dir):
        if (
            attempt.scheduler_job_id == slurm_state.job_id
            or (
                attempt.scheduler_array_job_id
                and attempt.scheduler_array_job_id == (slurm_state.array_job_id or "")
                and attempt.scheduler_array_task_id == str(slurm_state.array_task_id)
            )
        ):
            if terminal and attempt.attempt_state != "final":
                commit_attempt(
                    run_dir,
                    _reconciled_attempt(
                        attempt_id=attempt.attempt_id,
                        stage_instance_id=stage_instance_id,
                        slurm_state=slurm_state,
                        previous=attempt,
                    ),
                )
            return attempt.attempt_id
    attempt_id = _next_attempt_id(run_dir)
    attempt = _reconciled_attempt(attempt_id=attempt_id, stage_instance_id=stage_instance_id, slurm_state=slurm_state)
    if not terminal:
        attempt = StageAttemptRecord(
            attempt_id=attempt.attempt_id,
            stage_instance_id=attempt.stage_instance_id,
            attempt_source=attempt.attempt_source,
            attempt_state="running",
            scheduler_job_id=attempt.scheduler_job_id,
            scheduler_array_job_id=attempt.scheduler_array_job_id,
            scheduler_array_task_id=attempt.scheduler_array_task_id,
            scheduler_state=attempt.scheduler_state,
            scheduler_exit_code=attempt.scheduler_exit_code,
            node_list=attempt.node_list,
            started_by_executor=attempt.started_by_executor,
            executor_started_at=attempt.executor_started_at,
            executor_finished_at=attempt.executor_finished_at,
            started_at=attempt.started_at,
            finished_at="",
            exit_code=None,
            failure_class=None,
            reason=f"reconstructed from Slurm job {slurm_state.job_id} state={slurm_state.state}",
        )
    commit_attempt(run_dir, attempt)
    return attempt_id


def _master_fallback(job_states: dict[str, object], job_id: str, *, group_size: int):
    master = job_states.get(job_id)
    if master is None or group_size <= 1:
        return master
    if master.is_terminal and not master.is_success:
        return master
    return None


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
        fallback_state = _master_fallback(job_states, job_id, group_size=group.array_size)
        for task_index, stage_instance_id in enumerate(group.stage_instance_ids):
            instance = instances_by_id[stage_instance_id]
            run_dir = batch_root / instance.run_dir_rel
            current = read_stage_status(run_dir)
            if current is not None and current.state in TERMINAL_STATES:
                continue
            slurm_state = task_states.get(task_index) or fallback_state
            if slurm_state is None:
                continue
            _append_scheduler_observation(
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
                latest_attempt_id = _scheduler_attempt(
                    run_dir,
                    stage_instance_id=instance.stage_instance_id,
                    slurm_state=slurm_state,
                    terminal=stage_state in TERMINAL_STATES,
                )
            if stage_state == "success" and not _stage_outputs_path(run_dir).exists():
                if _missing_output_expired(
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
