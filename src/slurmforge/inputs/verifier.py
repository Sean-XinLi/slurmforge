from __future__ import annotations

from pathlib import Path

from ..errors import InputContractError
from ..io import SchemaVersion, utc_now, write_json
from ..contracts import InputBinding
from ..plans.stage import StageBatchPlan, StageInstancePlan
from .models import StageInputVerificationReport
from .verification import record_for_binding


def input_verification_path(run_dir: Path) -> Path:
    return run_dir / "input_verification.json"


def verify_stage_instance_inputs(
    instance: StageInstancePlan,
    bindings: tuple[InputBinding, ...],
    *,
    phase: str,
) -> StageInputVerificationReport:
    now = utc_now()
    records = tuple(record_for_binding(binding, phase=phase, now=now) for binding in bindings)
    state = "failed" if any(record.state == "failed" for record in records) else "verified"
    return StageInputVerificationReport(
        schema_version=SchemaVersion.INPUT_VERIFICATION,
        stage_instance_id=instance.stage_instance_id,
        run_id=instance.run_id,
        stage_name=instance.stage_name,
        phase=phase,
        state=state,
        records=records,
    )


def raise_for_failed_report(report: StageInputVerificationReport) -> None:
    failures = [record for record in report.records if record.state == "failed"]
    if not failures:
        return
    details = "; ".join(f"{record.input_name}: {record.reason}" for record in failures)
    raise InputContractError(f"stage input verification failed for `{report.stage_instance_id}`: {details}")


def verify_and_write_stage_instance_inputs(
    instance: StageInstancePlan,
    bindings: tuple[InputBinding, ...],
    *,
    phase: str,
    run_dir: Path,
) -> StageInputVerificationReport:
    report = verify_stage_instance_inputs(instance, bindings, phase=phase)
    write_json(input_verification_path(run_dir), report)
    raise_for_failed_report(report)
    return report


def verification_failure_reasons(reports: tuple[StageInputVerificationReport, ...]) -> list[str]:
    failures: list[str] = []
    for report in reports:
        for record in report.records:
            if record.state == "failed":
                failures.append(f"{report.stage_instance_id}.{record.input_name}: {record.reason}")
    return failures


def verify_stage_batch_inputs(
    batch: StageBatchPlan,
    *,
    phase: str,
    raise_on_failure: bool = True,
) -> tuple[StageInputVerificationReport, ...]:
    batch_root = Path(batch.submission_root)
    reports: list[StageInputVerificationReport] = []
    for instance in batch.stage_instances:
        run_dir = batch_root / instance.run_dir_rel
        report = verify_stage_instance_inputs(instance, instance.input_bindings, phase=phase)
        write_json(input_verification_path(run_dir), report)
        reports.append(report)
    failures = verification_failure_reasons(tuple(reports))
    if failures:
        joined = "; ".join(failures)
        if raise_on_failure:
            raise InputContractError(f"stage batch input verification failed for `{batch.batch_id}`: {joined}")
    return tuple(reports)
