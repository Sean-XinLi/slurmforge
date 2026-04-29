from __future__ import annotations

from typing import Any

from ...contracts import input_binding_from_dict
from ...contracts.outputs import stage_output_contract_from_dict
from ..stage import EntryPlan, GroupPlan, StageBatchPlan, StageInstancePlan
from .budget import budget_plan_from_dict
from .common import require_plan_schema
from .launcher import before_step_plan_from_dict, launcher_plan_from_dict
from .notifications import notification_plan_from_dict
from .outputs import artifact_store_plan_from_dict
from .resources import resource_plan_from_dict, resource_sizing_from_dict
from .runtime import environment_plan_from_dict, runtime_plan_from_dict


def stage_instance_plan_from_dict(payload: dict[str, Any]) -> StageInstancePlan:
    require_plan_schema(payload, name="stage_instance_plan")
    return StageInstancePlan(
        stage_instance_id=str(payload["stage_instance_id"]),
        run_id=str(payload["run_id"]),
        run_index=int(payload["run_index"]),
        stage_name=str(payload["stage_name"]),
        stage_kind=str(payload["stage_kind"]),
        entry=entry_plan_from_dict(payload["entry"]),
        resources=resource_plan_from_dict(payload["resources"]),
        runtime_plan=runtime_plan_from_dict(payload["runtime_plan"]),
        environment_name=str(payload["environment_name"]),
        environment_plan=environment_plan_from_dict(payload["environment_plan"]),
        before_steps=tuple(
            before_step_plan_from_dict(item) for item in payload["before_steps"]
        ),
        launcher_plan=launcher_plan_from_dict(payload["launcher_plan"]),
        artifact_store_plan=artifact_store_plan_from_dict(
            payload["artifact_store_plan"]
        ),
        input_bindings=tuple(
            input_binding_from_dict(item) for item in payload["input_bindings"]
        ),
        output_contract=stage_output_contract_from_dict(payload["output_contract"]),
        lineage=dict(payload["lineage"]),
        run_overrides=dict(payload["run_overrides"]),
        resource_sizing=resource_sizing_from_dict(payload["resource_sizing"]),
        spec_snapshot_digest=str(payload["spec_snapshot_digest"]),
        run_dir_rel=str(payload["run_dir_rel"]),
    )


def entry_plan_from_dict(payload: dict[str, Any]) -> EntryPlan:
    command = payload["command"]
    if isinstance(command, list):
        command = [str(item) for item in command]
    elif command is not None:
        command = str(command)
    return EntryPlan(
        type=str(payload["type"]),
        script=None if payload["script"] in (None, "") else str(payload["script"]),
        command=command,
        workdir=str(payload["workdir"]),
        args=dict(payload["args"]),
    )


def group_plan_from_dict(payload: dict[str, Any]) -> GroupPlan:
    require_plan_schema(payload, name="group_plan")
    array_throttle = payload["array_throttle"]
    return GroupPlan(
        group_id=str(payload["group_id"]),
        group_index=int(payload["group_index"]),
        resource_key=str(payload["resource_key"]),
        resources=resource_plan_from_dict(payload["resources"]),
        stage_instance_ids=tuple(str(item) for item in payload["stage_instance_ids"]),
        run_ids=tuple(str(item) for item in payload["run_ids"]),
        array_size=int(payload["array_size"]),
        array_throttle=None if array_throttle in (None, "") else int(array_throttle),
        gpus_per_task=int(payload["gpus_per_task"]),
    )


def stage_batch_plan_from_dict(payload: dict[str, Any]) -> StageBatchPlan:
    require_plan_schema(payload, name="stage_batch_plan")
    return StageBatchPlan(
        batch_id=str(payload["batch_id"]),
        stage_name=str(payload["stage_name"]),
        project=str(payload["project"]),
        experiment=str(payload["experiment"]),
        selected_runs=tuple(str(item) for item in payload["selected_runs"]),
        stage_instances=tuple(
            stage_instance_plan_from_dict(item) for item in payload["stage_instances"]
        ),
        group_plans=tuple(
            group_plan_from_dict(item) for item in payload["group_plans"]
        ),
        submission_root=str(payload["submission_root"]),
        source_ref=str(payload["source_ref"]),
        spec_snapshot_digest=str(payload["spec_snapshot_digest"]),
        budget_plan=budget_plan_from_dict(payload["budget_plan"]),
        notification_plan=notification_plan_from_dict(payload["notification_plan"]),
    )
