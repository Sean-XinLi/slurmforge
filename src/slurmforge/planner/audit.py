from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..contracts import binding_is_ready_for_injection
from ..inputs.verifier import verify_stage_instance_inputs
from ..io import SchemaVersion, stable_json, to_jsonable
from ..plans.stage import StageBatchPlan
from ..plans.train_eval import TrainEvalPipelinePlan
from ..runtime.probe import check_runtime_contract
from ..spec import ExperimentSpec
from .resource_estimate import build_resource_estimate


@dataclass(frozen=True)
class DryRunAudit:
    command: str
    state: str
    plan_kind: str
    plan: dict[str, Any]
    validation: dict[str, Any]
    resource_estimate: dict[str, Any] = field(default_factory=dict)
    schema_version: int = SchemaVersion.DRY_RUN_AUDIT


def build_empty_source_selection_audit(
    *,
    command: str,
    stage: str,
    query: str,
    source_root: str,
) -> DryRunAudit:
    validation = {
        "selected_runs": 0,
        "stage": stage,
        "query": query,
        "source_root": source_root,
    }
    return DryRunAudit(
        command=command,
        state="valid",
        plan_kind="empty_source_selection",
        plan={},
        validation=validation,
    )


def _stage_batch_unresolved_inputs(batch: StageBatchPlan) -> list[dict[str, Any]]:
    unresolved: list[dict[str, Any]] = []
    for instance in batch.stage_instances:
        for binding in instance.input_bindings:
            if not binding.inject.get("required") or binding_is_ready_for_injection(binding):
                continue
            resolution = dict(binding.resolution or {})
            unresolved.append(
                {
                    "stage_instance_id": instance.stage_instance_id,
                    "run_id": instance.run_id,
                    "stage_name": instance.stage_name,
                    "input_name": binding.input_name,
                    "source": to_jsonable(binding.source),
                    "expects": binding.expects,
                    "resolution": resolution,
                    "deferred": resolution.get("state") in {"awaiting_upstream_output", "unresolved"}
                    and binding.source.kind == "upstream_output",
                }
            )
    return unresolved


def _stage_batch_verification(batch: StageBatchPlan, *, full: bool) -> list[dict[str, Any]]:
    if not full:
        return []
    return [
        to_jsonable(
            verify_stage_instance_inputs(
                instance,
                instance.input_bindings,
                phase="dry_run",
            )
        )
        for instance in batch.stage_instances
    ]


def _runtime_contracts_for_stage_batch(batch: StageBatchPlan, *, full: bool) -> list[dict[str, Any]]:
    if not full:
        return []
    seen: set[str] = set()
    reports: list[dict[str, Any]] = []
    for instance in batch.stage_instances:
        key = stable_json(instance.runtime_plan)
        if key in seen:
            continue
        seen.add(key)
        reports.append(to_jsonable(check_runtime_contract(instance.runtime_plan)))
    return reports


def _stage_batch_payload(batch: StageBatchPlan, *, full: bool) -> dict[str, Any]:
    unresolved = _stage_batch_unresolved_inputs(batch)
    verification = _stage_batch_verification(batch, full=full)
    runtime_contracts = _runtime_contracts_for_stage_batch(batch, full=full)
    deferred_keys = {
        (item["stage_instance_id"], item["input_name"])
        for item in unresolved
        if item["deferred"]
    }
    failures = []
    for report in verification:
        stage_instance_id = str(report.get("stage_instance_id") or "")
        for item in report.get("records", ()):
            if item.get("state") != "failed":
                continue
            if (stage_instance_id, item.get("input_name")) in deferred_keys:
                continue
            failures.append(item)
    runtime_failures = [item for item in runtime_contracts if item.get("state") == "failed"]
    blocking_unresolved = [item for item in unresolved if not item["deferred"]]
    state = "invalid" if failures or runtime_failures or blocking_unresolved else "valid"
    return {
        "state": state,
        "batch_id": batch.batch_id,
        "stage_name": batch.stage_name,
        "root": batch.submission_root,
        "selected_runs": list(batch.selected_runs),
        "resource_groups": to_jsonable(batch.group_plans),
        "budget_plan": to_jsonable(batch.budget_plan),
        "unresolved_inputs": unresolved,
        "input_verification": verification,
        "runtime_contracts": runtime_contracts,
    }


def build_dry_run_audit(
    spec: ExperimentSpec,
    plan: StageBatchPlan | TrainEvalPipelinePlan,
    *,
    command: str,
    full: bool = False,
) -> DryRunAudit:
    if isinstance(plan, StageBatchPlan):
        validation = _stage_batch_payload(plan, full=full)
        return DryRunAudit(
            command=command,
            state=str(validation["state"]),
            plan_kind="stage_batch",
            plan=to_jsonable(plan),
            validation=validation,
            resource_estimate=to_jsonable(build_resource_estimate(plan)) if full else {},
        )
    stages = {
        stage_name: _stage_batch_payload(batch, full=full)
        for stage_name, batch in plan.stage_batches.items()
    }
    state = "invalid" if any(stage["state"] == "invalid" for stage in stages.values()) else "valid"
    return DryRunAudit(
        command=command,
        state=state,
        plan_kind="train_eval_pipeline",
        plan=to_jsonable(plan),
        validation={
            "state": state,
            "project": spec.project,
            "experiment": spec.experiment,
            "stage_batches": stages,
        },
        resource_estimate=to_jsonable(build_resource_estimate(plan)) if full else {},
    )
