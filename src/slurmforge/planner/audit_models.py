from __future__ import annotations

from dataclasses import dataclass

from ..contracts import InputResolution, InputSource
from ..inputs.models import StageInputVerificationReport
from ..io import SchemaVersion, to_jsonable
from ..plans.budget import BudgetPlan
from ..plans.stage import GroupPlan, StageBatchPlan
from ..plans.train_eval import TrainEvalPipelinePlan
from ..runtime.probe import RuntimeContractReport
from ..resource_estimates.models import ExperimentResourceEstimate


@dataclass(frozen=True)
class InputResolutionAudit:
    kind: str = ""
    state: str = ""
    reason: str = ""
    producer_stage_instance_id: str = ""
    output_name: str = ""
    source_root: str = ""


@dataclass(frozen=True)
class UnresolvedInputAuditRecord:
    stage_instance_id: str
    run_id: str
    stage_name: str
    input_name: str
    source: InputSource
    expects: str
    resolution: InputResolutionAudit

    @property
    def deferred(self) -> bool:
        return (
            self.source.kind == "upstream_output"
            and self.resolution.state in {"awaiting_upstream_output", "unresolved"}
        )


@dataclass(frozen=True)
class StageBatchDryRunValidation:
    state: str
    batch_id: str
    stage_name: str
    root: str
    selected_runs: tuple[str, ...]
    resource_groups: tuple[GroupPlan, ...]
    budget_plan: BudgetPlan
    unresolved_inputs: tuple[UnresolvedInputAuditRecord, ...]
    input_verification: tuple[StageInputVerificationReport, ...] = ()
    runtime_contracts: tuple[RuntimeContractReport, ...] = ()


@dataclass(frozen=True)
class TrainEvalDryRunValidation:
    state: str
    project: str
    experiment: str
    stage_batches: dict[str, StageBatchDryRunValidation]


@dataclass(frozen=True)
class EmptySourceSelectionValidation:
    selected_runs: int
    stage: str
    query: str
    source_root: str


DryRunValidation = (
    StageBatchDryRunValidation
    | TrainEvalDryRunValidation
    | EmptySourceSelectionValidation
)


@dataclass(frozen=True)
class DryRunAudit:
    command: str
    state: str
    plan_kind: str
    plan: StageBatchPlan | TrainEvalPipelinePlan | None
    validation: DryRunValidation
    resource_estimate: ExperimentResourceEstimate | None = None
    schema_version: int = SchemaVersion.DRY_RUN_AUDIT


def input_resolution_audit_from_resolution(
    resolution: InputResolution,
) -> InputResolutionAudit:
    return InputResolutionAudit(
        kind=resolution.kind,
        state=resolution.state,
        reason=resolution.reason,
        producer_stage_instance_id=resolution.producer_stage_instance_id,
        output_name=resolution.output_name,
        source_root=resolution.source_root,
    )


def dry_run_audit_to_dict(audit: DryRunAudit) -> dict[str, object]:
    return {
        "schema_version": audit.schema_version,
        "command": audit.command,
        "state": audit.state,
        "plan_kind": audit.plan_kind,
        "plan": {} if audit.plan is None else to_jsonable(audit.plan),
        "validation": _validation_to_dict(audit.validation),
        "resource_estimate": {}
        if audit.resource_estimate is None
        else to_jsonable(audit.resource_estimate),
    }


def _validation_to_dict(validation: DryRunValidation) -> dict[str, object]:
    if isinstance(validation, StageBatchDryRunValidation):
        return _stage_batch_validation_to_dict(validation)
    if isinstance(validation, TrainEvalDryRunValidation):
        return {
            "state": validation.state,
            "project": validation.project,
            "experiment": validation.experiment,
            "stage_batches": {
                stage_name: _stage_batch_validation_to_dict(stage_validation)
                for stage_name, stage_validation in validation.stage_batches.items()
            },
        }
    return {
        "selected_runs": validation.selected_runs,
        "stage": validation.stage,
        "query": validation.query,
        "source_root": validation.source_root,
    }


def _stage_batch_validation_to_dict(
    validation: StageBatchDryRunValidation,
) -> dict[str, object]:
    return {
        "state": validation.state,
        "batch_id": validation.batch_id,
        "stage_name": validation.stage_name,
        "root": validation.root,
        "selected_runs": list(validation.selected_runs),
        "resource_groups": to_jsonable(validation.resource_groups),
        "budget_plan": to_jsonable(validation.budget_plan),
        "unresolved_inputs": [
            _unresolved_input_to_dict(record)
            for record in validation.unresolved_inputs
        ],
        "input_verification": to_jsonable(validation.input_verification),
        "runtime_contracts": to_jsonable(validation.runtime_contracts),
    }


def _unresolved_input_to_dict(
    record: UnresolvedInputAuditRecord,
) -> dict[str, object]:
    return {
        "stage_instance_id": record.stage_instance_id,
        "run_id": record.run_id,
        "stage_name": record.stage_name,
        "input_name": record.input_name,
        "source": to_jsonable(record.source),
        "expects": record.expects,
        "resolution": _input_resolution_to_dict(record.resolution),
        "deferred": record.deferred,
    }


def _input_resolution_to_dict(record: InputResolutionAudit) -> dict[str, object]:
    payload: dict[str, object] = {}
    for key, value in (
        ("kind", record.kind),
        ("state", record.state),
        ("reason", record.reason),
        ("producer_stage_instance_id", record.producer_stage_instance_id),
        ("output_name", record.output_name),
        ("source_root", record.source_root),
    ):
        if value:
            payload[key] = value
    return payload

