from __future__ import annotations

from pathlib import Path

from ..emit import write_controller_submit_file
from ..slurm import SlurmClient, failure_class_for_slurm_state, stage_state_for_slurm_state
from ..io import SchemaVersion, diagnostic_path, utc_now, write_exception_diagnostic
from ..storage.controller import (
    ControllerJobRecord,
    append_controller_event,
    read_controller_job,
    write_controller_job,
    write_controller_status,
)


def submit_controller_job(
    plan,
    *,
    client: SlurmClient | None = None,
) -> ControllerJobRecord:
    pipeline_root = Path(plan.root_dir)
    existing = read_controller_job(pipeline_root)
    if existing is not None:
        raise RuntimeError(f"controller job record already exists for train/eval pipeline root: {pipeline_root}")
    sbatch_path = write_controller_submit_file(plan).resolve()
    submitted_at = utc_now()
    slurm = client or SlurmClient()
    try:
        job_id = slurm.submit(sbatch_path)
    except Exception as exc:
        diagnostic = write_exception_diagnostic(
            diagnostic_path(pipeline_root, "controller", "controller_submit_traceback.log"),
            exc,
        )
        write_controller_status(
            pipeline_root,
            "failed",
            reason=str(exc),
            sbatch_path=str(sbatch_path),
            submitted_at=submitted_at,
        )
        append_controller_event(
            pipeline_root,
            "controller_submit_failed",
            reason=str(exc),
            diagnostic_path=str(diagnostic),
        )
        raise
    record = ControllerJobRecord(
        schema_version=SchemaVersion.CONTROLLER_JOB,
        pipeline_id=plan.pipeline_id,
        scheduler_job_id=job_id,
        submitted_at=submitted_at,
        sbatch_path=str(sbatch_path),
    )
    write_controller_job(pipeline_root, record)
    write_controller_status(
        pipeline_root,
        "queued",
        scheduler_job_id=job_id,
        sbatch_path=str(sbatch_path),
        reason="controller submitted",
    )
    append_controller_event(
        pipeline_root,
        "controller_submitted",
        scheduler_job_id=job_id,
        sbatch_path=str(sbatch_path),
    )
    return record


def reconcile_controller_job(
    pipeline_root: Path,
    *,
    client: SlurmClient | None = None,
) -> ControllerJobRecord | None:
    record = read_controller_job(pipeline_root)
    if record is None or not record.scheduler_job_id:
        return record
    slurm = client or SlurmClient()
    observed = slurm.query_observed_jobs([record.scheduler_job_id])
    slurm_state = observed.get(record.scheduler_job_id)
    if slurm_state is None:
        write_controller_status(
            pipeline_root,
            "queued",
            scheduler_job_id=record.scheduler_job_id,
            reason="controller job was not observed by Slurm",
        )
        return record
    status_state = stage_state_for_slurm_state(slurm_state.state) or "queued"
    reason = slurm_state.reason or f"Slurm job {slurm_state.job_id} state={slurm_state.state}"
    write_controller_status(
        pipeline_root,
        status_state,
        scheduler_job_id=record.scheduler_job_id,
        scheduler_state=slurm_state.state,
        scheduler_exit_code=slurm_state.exit_code,
        failure_class=failure_class_for_slurm_state(slurm_state.state),
        reason=reason,
        observed_at=utc_now(),
    )
    append_controller_event(
        pipeline_root,
        "controller_reconciled",
        scheduler_job_id=record.scheduler_job_id,
        scheduler_state=slurm_state.state,
        state=status_state,
    )
    return record
