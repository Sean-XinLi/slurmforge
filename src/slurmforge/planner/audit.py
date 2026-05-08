from __future__ import annotations

from ..contracts import InputBinding, binding_is_ready_for_injection
from ..inputs.models import StageInputVerificationReport
from ..inputs.verifier import verify_stage_instance_inputs
from ..io import stable_json
from ..plans.stage import StageBatchPlan
from ..plans.train_eval import TrainEvalPipelinePlan
from ..runtime.probe import RuntimeContractReport, check_runtime_contract
from ..spec import ExperimentSpec
from .audit_models import (
    DryRunAudit,
    EmptySourceSelectionValidation,
    StageBatchDryRunValidation,
    TrainEvalDryRunValidation,
    UnresolvedInputAuditRecord,
    input_resolution_audit_from_mapping,
)
from .resource_estimate import build_resource_estimate


def build_empty_source_selection_audit(
    *,
    command: str,
    stage: str,
    query: str,
    source_root: str,
) -> DryRunAudit:
    validation = EmptySourceSelectionValidation(
        selected_runs=0,
        stage=stage,
        query=query,
        source_root=source_root,
    )
    return DryRunAudit(
        command=command,
        state="valid",
        plan_kind="empty_source_selection",
        plan=None,
        validation=validation,
    )


def _stage_batch_unresolved_inputs(
    batch: StageBatchPlan,
) -> tuple[UnresolvedInputAuditRecord, ...]:
    unresolved: list[UnresolvedInputAuditRecord] = []
    for instance in batch.stage_instances:
        for binding in instance.input_bindings:
            if not _binding_required(binding) or binding_is_ready_for_injection(binding):
                continue
            unresolved.append(
                UnresolvedInputAuditRecord(
                    stage_instance_id=instance.stage_instance_id,
                    run_id=instance.run_id,
                    stage_name=instance.stage_name,
                    input_name=binding.input_name,
                    source=binding.source,
                    expects=binding.expects,
                    resolution=input_resolution_audit_from_mapping(
                        binding.resolution
                    ),
                )
            )
    return tuple(unresolved)


def _stage_batch_verification(
    batch: StageBatchPlan, *, full: bool
) -> tuple[StageInputVerificationReport, ...]:
    if not full:
        return ()
    return tuple(
        verify_stage_instance_inputs(
            instance,
            instance.input_bindings,
            phase="dry_run",
        )
        for instance in batch.stage_instances
    )


def _runtime_contracts_for_stage_batch(
    batch: StageBatchPlan, *, full: bool
) -> tuple[RuntimeContractReport, ...]:
    if not full:
        return ()
    seen: set[str] = set()
    reports: list[RuntimeContractReport] = []
    for instance in batch.stage_instances:
        key = stable_json(instance.runtime_plan)
        if key in seen:
            continue
        seen.add(key)
        reports.append(check_runtime_contract(instance.runtime_plan))
    return tuple(reports)


def _stage_batch_validation(
    batch: StageBatchPlan, *, full: bool
) -> StageBatchDryRunValidation:
    unresolved = _stage_batch_unresolved_inputs(batch)
    verification = _stage_batch_verification(batch, full=full)
    runtime_contracts = _runtime_contracts_for_stage_batch(batch, full=full)
    deferred_keys = {
        (record.stage_instance_id, record.input_name)
        for record in unresolved
        if record.deferred
    }
    input_failures = tuple(
        input_record
        for report in verification
        for input_record in report.records
        if input_record.state == "failed"
        and (report.stage_instance_id, input_record.input_name) not in deferred_keys
    )
    runtime_failures = tuple(
        report for report in runtime_contracts if report.state == "failed"
    )
    blocking_unresolved = tuple(
        item
        for item in unresolved
        if not item.deferred
    )
    state = (
        "invalid"
        if input_failures or runtime_failures or blocking_unresolved
        else "valid"
    )
    return StageBatchDryRunValidation(
        state=state,
        batch_id=batch.batch_id,
        stage_name=batch.stage_name,
        root=batch.submission_root,
        selected_runs=tuple(batch.selected_runs),
        resource_groups=tuple(batch.group_plans),
        budget_plan=batch.budget_plan,
        unresolved_inputs=unresolved,
        input_verification=verification,
        runtime_contracts=runtime_contracts,
    )


def build_dry_run_audit(
    spec: ExperimentSpec,
    plan: StageBatchPlan | TrainEvalPipelinePlan,
    *,
    command: str,
    full: bool = False,
) -> DryRunAudit:
    if isinstance(plan, StageBatchPlan):
        validation = _stage_batch_validation(plan, full=full)
        return DryRunAudit(
            command=command,
            state=validation.state,
            plan_kind="stage_batch",
            plan=plan,
            validation=validation,
            resource_estimate=build_resource_estimate(plan) if full else None,
        )
    stages = {
        stage_name: _stage_batch_validation(batch, full=full)
        for stage_name, batch in plan.stage_batches.items()
    }
    state = (
        "invalid"
        if any(stage.state == "invalid" for stage in stages.values())
        else "valid"
    )
    return DryRunAudit(
        command=command,
        state=state,
        plan_kind="train_eval_pipeline",
        plan=plan,
        validation=TrainEvalDryRunValidation(
            state=state,
            project=spec.project,
            experiment=spec.experiment,
            stage_batches=stages,
        ),
        resource_estimate=build_resource_estimate(plan) if full else None,
    )


def _binding_required(binding: InputBinding) -> bool:
    if "required" not in binding.inject:
        return False
    return binding.inject["required"] is True
