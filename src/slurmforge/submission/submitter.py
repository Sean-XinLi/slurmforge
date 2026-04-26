from __future__ import annotations

from pathlib import Path
from typing import Literal

from ..errors import ConfigContractError
from ..inputs import input_verification_path
from ..io import SchemaVersion, read_json, utc_now
from ..plans import StageBatchPlan
from ..slurm import SlurmClient
from ..status import StageStatusRecord, commit_stage_status, read_stage_status
from ..storage import load_execution_stage_batch_plan, read_materialization_status
from .ledger import (
    append_submission_event,
    read_submission_ledger,
    submitted_group_job_ids,
    write_submission_ledger,
)
from .models import SubmissionLedger
from .models import PreparedSubmission


def _dependency_for(group_id: str, batch: StageBatchPlan, group_job_ids: dict[str, str]) -> str | None:
    deps = {item["to_group"]: item for item in batch.budget_plan.get("dependencies", ())}
    dep = deps.get(group_id)
    if dep is None:
        return None
    from_groups = dep.get("from_groups") or [dep.get("from_group")]
    missing = [str(item) for item in from_groups if item and str(item) not in group_job_ids]
    if missing:
        raise ConfigContractError(
            f"Cannot submit `{group_id}` before dependency groups have scheduler ids: {', '.join(missing)}"
        )
    from_job_ids = [group_job_ids[str(item)] for item in from_groups if item]
    return f"{dep['type']}:{':'.join(from_job_ids)}"


def mark_stage_batch_queued(batch: StageBatchPlan, group_job_ids: dict[str, str]) -> None:
    reason = f"submitted array jobs={','.join(group_job_ids.values())}"
    root = Path(batch.submission_root)
    for instance in batch.stage_instances:
        run_dir = root / instance.run_dir_rel
        status = read_stage_status(run_dir)
        commit_stage_status(
            run_dir,
            StageStatusRecord(
                schema_version=SchemaVersion.STATUS,
                stage_instance_id=instance.stage_instance_id,
                run_id=instance.run_id,
                stage_name=instance.stage_name,
                state="queued",
                latest_attempt_id=None if status is None else status.latest_attempt_id,
                latest_output_digest=None if status is None else status.latest_output_digest,
                failure_class=None,
                reason=reason,
            ),
            source="submission",
        )


def _load_ready_prepared_submission(prepared: PreparedSubmission) -> tuple[StageBatchPlan, SubmissionLedger]:
    batch_root = Path(prepared.batch_root)
    if prepared.materialization_state != "ready":
        raise ConfigContractError(
            f"Prepared submission `{batch_root}` was not produced as ready: "
            f"{prepared.materialization_state}"
        )
    materialization = read_materialization_status(batch_root)
    if materialization is None:
        raise ConfigContractError(f"Materialization status is missing for prepared submission `{batch_root}`")
    if materialization.state != "ready":
        raise ConfigContractError(
            f"Prepared submission `{batch_root}` is not ready: materialization state is `{materialization.state}`"
        )
    if materialization.batch_id != prepared.batch_id or materialization.stage_name != prepared.stage_name:
        raise ConfigContractError(
            f"Prepared submission `{batch_root}` materialization belongs to "
            f"{materialization.stage_name}/{materialization.batch_id}, not {prepared.stage_name}/{prepared.batch_id}"
        )
    if materialization.submit_manifest_path and Path(materialization.submit_manifest_path) != prepared.manifest_path:
        raise ConfigContractError(
            f"Prepared submission `{batch_root}` points at `{prepared.manifest_path}`, "
            f"but materialization records `{materialization.submit_manifest_path}`"
        )
    if not prepared.manifest_path.exists():
        raise ConfigContractError(f"Prepared submission manifest does not exist: {prepared.manifest_path}")
    ledger = read_submission_ledger(batch_root)
    if ledger is None:
        raise ConfigContractError(f"Submission ledger is missing for prepared submission `{batch_root}`")
    if ledger.batch_id != prepared.batch_id or ledger.stage_name != prepared.stage_name:
        raise ConfigContractError(
            f"Prepared submission `{batch_root}` ledger belongs to "
            f"{ledger.stage_name}/{ledger.batch_id}, not {prepared.stage_name}/{prepared.batch_id}"
        )
    if ledger.generation_id != prepared.generation_id:
        raise ConfigContractError(
            f"Prepared submission `{batch_root}` has generation `{prepared.generation_id}`, "
            f"but ledger records `{ledger.generation_id}`"
        )
    for record in ledger.groups.values():
        if not Path(record.sbatch_path).exists():
            raise ConfigContractError(f"Prepared sbatch file does not exist: {record.sbatch_path}")
    batch = load_execution_stage_batch_plan(batch_root)
    for instance in batch.stage_instances:
        verification_path = input_verification_path(batch_root / instance.run_dir_rel)
        if not verification_path.exists():
            raise ConfigContractError(f"Input verification report is missing: {verification_path}")
        verification = read_json(verification_path)
        if verification.get("state") == "failed":
            raise ConfigContractError(f"Input verification failed for prepared submission: {verification_path}")
        if verification.get("phase") != "submit":
            raise ConfigContractError(f"Input verification report is not a submit-phase report: {verification_path}")
    return batch, ledger


def submit_prepared_stage_batch(
    prepared: PreparedSubmission,
    *,
    client: SlurmClient | None = None,
    mark_queued: bool = True,
    policy: Literal["new_only", "recover_partial"] = "new_only",
) -> dict[str, str]:
    if policy not in {"new_only", "recover_partial"}:
        raise ConfigContractError(f"Unsupported submission policy: {policy}")
    batch, ledger = _load_ready_prepared_submission(prepared)
    batch_root = Path(prepared.batch_root)
    slurm = client or SlurmClient()
    group_job_ids = submitted_group_job_ids(batch_root)

    for group in batch.group_plans:
        record = ledger.groups[group.group_id]
        if record.scheduler_job_id and record.state in {"submitted", "adopted"}:
            if policy == "new_only":
                raise ConfigContractError(
                    f"Stage batch `{batch.stage_name}` already has submitted group `{group.group_id}` "
                    f"with scheduler job `{record.scheduler_job_id}`; submit a derived batch for a new execution"
                )
            group_job_ids[group.group_id] = record.scheduler_job_id
            continue
        if record.state == "submitting" and not record.scheduler_job_id:
            ledger.state = "uncertain"
            record.reason = "group may have reached sbatch without a recorded scheduler job id"
            write_submission_ledger(batch_root, ledger)
            raise ConfigContractError(
                f"Submission ledger for `{batch.stage_name}` is uncertain at group `{group.group_id}`; "
                "manual reconcile is required before retrying"
            )
        dependency = _dependency_for(group.group_id, batch, group_job_ids)
        record.state = "submitting"
        record.dependency = dependency
        record.reason = ""
        ledger.state = "partial" if group_job_ids else "submitting"
        write_submission_ledger(batch_root, ledger)
        append_submission_event(
            batch_root,
            "group_submit_started",
            stage=batch.stage_name,
            group_id=group.group_id,
            sbatch_path=record.sbatch_path,
            dependency=dependency,
        )
        try:
            job_id = slurm.submit(Path(record.sbatch_path), dependency=dependency)
        except Exception as exc:
            record.state = "failed"
            record.reason = str(exc)
            ledger.state = "failed"
            write_submission_ledger(batch_root, ledger)
            append_submission_event(
                batch_root,
                "group_submit_failed",
                stage=batch.stage_name,
                group_id=group.group_id,
                reason=str(exc),
            )
            raise
        record.scheduler_job_id = job_id
        record.submitted_at = utc_now()
        record.state = "submitted"
        record.reason = ""
        group_job_ids[group.group_id] = job_id
        ledger.state = "partial"
        write_submission_ledger(batch_root, ledger)
        append_submission_event(
            batch_root,
            "group_submitted",
            stage=batch.stage_name,
            group_id=group.group_id,
            scheduler_job_id=job_id,
            sbatch_path=record.sbatch_path,
            dependency=dependency,
        )

    ledger.state = "submitted"
    write_submission_ledger(batch_root, ledger)
    append_submission_event(
        batch_root,
        "batch_submitted",
        stage=batch.stage_name,
        scheduler_job_ids=list(group_job_ids.values()),
    )
    if mark_queued:
        mark_stage_batch_queued(batch, group_job_ids)
    return group_job_ids
