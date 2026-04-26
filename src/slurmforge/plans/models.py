from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..schema import (
    InputBinding,
    input_binding_from_dict,
)
from ..io import SchemaVersion, require_schema
from ..spec.output_contract import StageOutputContract, stage_output_contract_from_dict


JsonDict = dict[str, Any]


def _require_plan_schema(payload: JsonDict, *, name: str) -> None:
    require_schema(payload, name=name, version=SchemaVersion.PLAN)


@dataclass(frozen=True)
class OutputRef:
    output_name: str
    kind: str
    path: str
    producer_stage_instance_id: str
    cardinality: str = "one"
    producer_attempt_id: str = ""
    digest: str = ""
    source_path: str = ""
    managed: bool = False
    strategy: str = ""
    source_digest: str = ""
    managed_digest: str = ""
    verified: bool | None = None
    size_bytes: int | None = None
    selection_reason: str = ""
    value: Any = None
    schema_version: int = SchemaVersion.PLAN


@dataclass(frozen=True)
class StageOutputsRecord:
    stage_instance_id: str
    producer_attempt_id: str
    outputs: dict[str, OutputRef]
    artifacts: tuple[str, ...]
    artifact_manifest: str
    schema_version: int = SchemaVersion.OUTPUT_RECORD


@dataclass(frozen=True)
class StageInstancePlan:
    stage_instance_id: str
    run_id: str
    run_index: int
    stage_name: str
    stage_kind: str
    entry: JsonDict
    resources: JsonDict
    runtime_plan: JsonDict
    launcher_plan: JsonDict
    artifact_store_plan: JsonDict
    input_bindings: tuple[InputBinding, ...]
    output_contract: StageOutputContract
    lineage: JsonDict
    matrix_assignments: JsonDict
    spec_snapshot_digest: str
    run_dir_rel: str
    schema_version: int = SchemaVersion.PLAN

    @property
    def binding_map(self) -> dict[str, InputBinding]:
        return {binding.input_name: binding for binding in self.input_bindings}


@dataclass(frozen=True)
class GroupPlan:
    group_id: str
    group_index: int
    resource_key: str
    resources: JsonDict
    stage_instance_ids: tuple[str, ...]
    run_ids: tuple[str, ...]
    array_size: int
    array_throttle: int | None = None
    gpus_per_task: int = 0
    schema_version: int = SchemaVersion.PLAN


@dataclass(frozen=True)
class StageBatchPlan:
    batch_id: str
    stage_name: str
    project: str
    experiment: str
    selected_runs: tuple[str, ...]
    stage_instances: tuple[StageInstancePlan, ...]
    group_plans: tuple[GroupPlan, ...]
    submission_root: str
    source_ref: str
    spec_snapshot_digest: str
    budget_plan: JsonDict = field(default_factory=dict)
    schema_version: int = SchemaVersion.PLAN


@dataclass(frozen=True)
class ControllerPlan:
    pipeline_id: str
    stage_order: tuple[str, ...]
    config_path: str
    root_dir: str
    resources: JsonDict = field(default_factory=dict)
    runtime_plan: JsonDict = field(default_factory=dict)
    schema_version: int = SchemaVersion.PLAN


@dataclass(frozen=True)
class PipelinePlan:
    pipeline_id: str
    stage_order: tuple[str, ...]
    run_set: tuple[str, ...]
    root_dir: str
    controller_plan: ControllerPlan
    stage_batches: dict[str, StageBatchPlan]
    spec_snapshot_digest: str
    schema_version: int = SchemaVersion.PLAN


def stage_instance_plan_from_dict(payload: JsonDict) -> StageInstancePlan:
    _require_plan_schema(payload, name="stage_instance_plan")
    return StageInstancePlan(
        stage_instance_id=str(payload["stage_instance_id"]),
        run_id=str(payload["run_id"]),
        run_index=int(payload["run_index"]),
        stage_name=str(payload["stage_name"]),
        stage_kind=str(payload["stage_kind"]),
        entry=dict(payload["entry"]),
        resources=dict(payload["resources"]),
        runtime_plan=dict(payload.get("runtime_plan") or {}),
        launcher_plan=dict(payload.get("launcher_plan") or {"type": "single"}),
        artifact_store_plan=dict(payload.get("artifact_store_plan") or {"strategy": "copy", "verify_digest": True}),
        input_bindings=tuple(input_binding_from_dict(item) for item in payload.get("input_bindings", ())),
        output_contract=stage_output_contract_from_dict(payload.get("output_contract")),
        lineage=dict(payload.get("lineage") or {}),
        matrix_assignments=dict(payload.get("matrix_assignments") or {}),
        spec_snapshot_digest=str(payload["spec_snapshot_digest"]),
        run_dir_rel=str(payload["run_dir_rel"]),
    )


def output_ref_from_dict(payload: JsonDict) -> OutputRef:
    require_schema(payload, name="output_ref", version=SchemaVersion.OUTPUT_RECORD)
    return OutputRef(
        output_name=str(payload["output_name"]),
        kind=str(payload["kind"]),
        path=str(payload["path"]),
        producer_stage_instance_id=str(payload["producer_stage_instance_id"]),
        cardinality=str(payload.get("cardinality") or "one"),
        producer_attempt_id=str(payload.get("producer_attempt_id") or ""),
        digest=str(payload.get("digest") or ""),
        source_path=str(payload.get("source_path") or ""),
        managed=bool(payload.get("managed", False)),
        strategy=str(payload.get("strategy") or ""),
        source_digest=str(payload.get("source_digest") or ""),
        managed_digest=str(payload.get("managed_digest") or ""),
        verified=None if payload.get("verified") is None else bool(payload.get("verified")),
        size_bytes=None if payload.get("size_bytes") is None else int(payload.get("size_bytes")),
        selection_reason=str(payload.get("selection_reason") or ""),
        value=payload.get("value"),
    )


def stage_outputs_record_from_dict(payload: JsonDict) -> StageOutputsRecord:
    require_schema(payload, name="stage_outputs", version=SchemaVersion.OUTPUT_RECORD)
    return StageOutputsRecord(
        stage_instance_id=str(payload["stage_instance_id"]),
        producer_attempt_id=str(payload["producer_attempt_id"]),
        outputs={str(name): output_ref_from_dict(dict(item)) for name, item in dict(payload.get("outputs") or {}).items()},
        artifacts=tuple(str(item) for item in payload.get("artifacts", ())),
        artifact_manifest=str(payload["artifact_manifest"]),
    )


def group_plan_from_dict(payload: JsonDict) -> GroupPlan:
    _require_plan_schema(payload, name="group_plan")
    return GroupPlan(
        group_id=str(payload["group_id"]),
        group_index=int(payload["group_index"]),
        resource_key=str(payload["resource_key"]),
        resources=dict(payload["resources"]),
        stage_instance_ids=tuple(str(item) for item in payload.get("stage_instance_ids", ())),
        run_ids=tuple(str(item) for item in payload.get("run_ids", ())),
        array_size=int(payload["array_size"]),
        array_throttle=None if payload.get("array_throttle") in (None, "") else int(payload.get("array_throttle")),
        gpus_per_task=int(payload.get("gpus_per_task", 0) or 0),
    )


def stage_batch_plan_from_dict(payload: JsonDict) -> StageBatchPlan:
    _require_plan_schema(payload, name="stage_batch_plan")
    return StageBatchPlan(
        batch_id=str(payload["batch_id"]),
        stage_name=str(payload["stage_name"]),
        project=str(payload["project"]),
        experiment=str(payload["experiment"]),
        selected_runs=tuple(str(item) for item in payload.get("selected_runs", ())),
        stage_instances=tuple(stage_instance_plan_from_dict(item) for item in payload.get("stage_instances", ())),
        group_plans=tuple(group_plan_from_dict(item) for item in payload.get("group_plans", ())),
        submission_root=str(payload["submission_root"]),
        source_ref=str(payload.get("source_ref") or ""),
        spec_snapshot_digest=str(payload["spec_snapshot_digest"]),
        budget_plan=dict(payload.get("budget_plan") or {}),
    )


def pipeline_plan_from_dict(payload: JsonDict) -> PipelinePlan:
    _require_plan_schema(payload, name="pipeline_plan")
    controller = dict(payload["controller_plan"])
    _require_plan_schema(controller, name="controller_plan")
    return PipelinePlan(
        pipeline_id=str(payload["pipeline_id"]),
        stage_order=tuple(str(item) for item in payload.get("stage_order", ())),
        run_set=tuple(str(item) for item in payload.get("run_set", ())),
        root_dir=str(payload["root_dir"]),
        controller_plan=ControllerPlan(
            pipeline_id=str(controller["pipeline_id"]),
            stage_order=tuple(str(item) for item in controller.get("stage_order", ())),
            config_path=str(controller["config_path"]),
            root_dir=str(controller["root_dir"]),
            resources=dict(controller.get("resources") or {}),
            runtime_plan=dict(controller.get("runtime_plan") or {}),
        ),
        stage_batches={
            str(name): stage_batch_plan_from_dict(batch)
            for name, batch in dict(payload.get("stage_batches") or {}).items()
        },
        spec_snapshot_digest=str(payload["spec_snapshot_digest"]),
    )
