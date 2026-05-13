from __future__ import annotations

from pathlib import Path

from ..emit.stage import write_stage_submit_files
from ..errors import ConfigContractError, InputContractError
from ..inputs.models import StageInputVerificationReport
from ..inputs.verifier import verification_failure_reasons, verify_stage_batch_inputs
from ..io import SchemaVersion
from ..plans.stage import StageBatchPlan
from ..status.machine import commit_stage_status
from ..status.models import StageStatusRecord
from ..storage.batch_materialization_records import write_materialization_status
from .ledger import initialize_submission_ledger, ledger_path
from .models import SubmitGeneration
from .models import PreparedSubmission
from .submit_manifest import (
    load_submit_manifest,
    submit_manifest_dependency_records,
    submit_manifest_group_paths,
    submit_manifest_path,
)


def _report_failure_reason(report: StageInputVerificationReport) -> str:
    failures = [record for record in report.records if record.state == "failed"]
    return "; ".join(f"{record.input_name}: {record.reason}" for record in failures)


def _mark_blocked_stage_inputs(
    batch: StageBatchPlan, reports: tuple[StageInputVerificationReport, ...]
) -> None:
    batch_root = Path(batch.submission_root)
    instances_by_id = {
        instance.stage_instance_id: instance for instance in batch.stage_instances
    }
    for report in reports:
        if report.state != "failed":
            continue
        instance = instances_by_id[report.stage_instance_id]
        commit_stage_status(
            batch_root / instance.run_dir_rel,
            StageStatusRecord(
                schema_version=SchemaVersion.STATUS,
                stage_instance_id=instance.stage_instance_id,
                run_id=instance.run_id,
                stage_name=instance.stage_name,
                state="blocked",
                failure_class="input_contract_error",
                reason=_report_failure_reason(report),
            ),
            source="input_verification",
        )


def create_submit_generation(batch: StageBatchPlan) -> SubmitGeneration:
    batch_root = Path(batch.submission_root)
    write_stage_submit_files(batch)
    manifest = load_submit_manifest(batch_root)
    return SubmitGeneration(
        generation_id=manifest.generation_id,
        batch_id=manifest.batch_id,
        stage_name=manifest.stage_name,
        manifest_path=str(submit_manifest_path(batch_root)),
        submit_script=manifest.submit_script,
        sbatch_paths_by_group=submit_manifest_group_paths(manifest),
        dependency_plan=submit_manifest_dependency_records(manifest),
    )


def prepare_stage_submission(batch: StageBatchPlan) -> PreparedSubmission:
    batch_root = Path(batch.submission_root)
    write_materialization_status(
        batch_root,
        batch_id=batch.batch_id,
        stage_name=batch.stage_name,
        state="verifying_inputs",
    )
    reports = verify_stage_batch_inputs(batch, phase="submit", raise_on_failure=False)
    failures = verification_failure_reasons(reports)
    if failures:
        reason = "; ".join(failures)
        _mark_blocked_stage_inputs(batch, reports)
        write_materialization_status(
            batch_root,
            batch_id=batch.batch_id,
            stage_name=batch.stage_name,
            state="blocked",
            failure_class="input_contract_error",
            reason=reason,
        )
        raise InputContractError(
            f"stage batch input verification failed for `{batch.batch_id}`: {reason}"
        )
    generation = create_submit_generation(batch)
    try:
        initialize_submission_ledger(batch_root, generation)
    except ConfigContractError as exc:
        write_materialization_status(
            batch_root,
            batch_id=batch.batch_id,
            stage_name=batch.stage_name,
            state="blocked",
            failure_class="submission_contract_error",
            reason=str(exc),
        )
        raise
    write_materialization_status(
        batch_root,
        batch_id=batch.batch_id,
        stage_name=batch.stage_name,
        state="ready",
        submit_manifest_path=generation.manifest_path,
    )
    return PreparedSubmission(
        batch_root=batch_root,
        batch_id=generation.batch_id,
        stage_name=generation.stage_name,
        generation_id=generation.generation_id,
        manifest_path=Path(generation.manifest_path),
        ledger_path=ledger_path(batch_root),
    )
