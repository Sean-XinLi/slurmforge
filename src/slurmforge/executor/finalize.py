from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..errors import InputContractError, RuntimeContractError
from ..outputs import ArtifactIntegrityError, discover_stage_outputs, write_stage_outputs_record
from .attempt import ExecutionAttempt


@dataclass(frozen=True)
class StageOutputFinalizationResult:
    artifact_paths: tuple[str, ...]
    failure_class: str | None = None
    reason: str = ""


def finalize_successful_stage_outputs(attempt: ExecutionAttempt, workdir: Path) -> StageOutputFinalizationResult:
    output_result = discover_stage_outputs(
        attempt.instance,
        workdir,
        attempt_id=attempt.attempt_id,
        attempt_dir=attempt.attempt_dir,
    )
    artifact_paths = tuple(output_result.artifact_paths)
    if output_result.failure_reason is not None:
        return StageOutputFinalizationResult(
            artifact_paths=artifact_paths,
            failure_class="missing_output",
            reason=output_result.failure_reason,
        )
    write_stage_outputs_record(output_result.stage_outputs, run_dir=attempt.run_dir, attempt_dir=attempt.attempt_dir)
    return StageOutputFinalizationResult(artifact_paths=artifact_paths)


def failure_class_for_exception(exc: BaseException, current_failure_class: str | None) -> str:
    if isinstance(exc, ArtifactIntegrityError):
        return "artifact_integrity_error"
    if isinstance(exc, RuntimeContractError):
        return "runtime_contract_error"
    if isinstance(exc, InputContractError):
        return "input_contract_error"
    return current_failure_class or "executor_error"
