from __future__ import annotations

from typing import Any

from ...contracts import input_binding_from_dict
from ...contracts.outputs import stage_output_contract_from_dict
from ...errors import RecordContractError
from ...record_fields import (
    required_int,
    required_nullable_int,
    required_nullable_string,
    required_object,
    required_object_array,
    required_string,
    required_string_array,
)
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
        stage_instance_id=required_string(
            payload, "stage_instance_id", label="stage_instance_plan", non_empty=True
        ),
        run_id=required_string(
            payload, "run_id", label="stage_instance_plan", non_empty=True
        ),
        run_index=required_int(payload, "run_index", label="stage_instance_plan"),
        stage_name=required_string(
            payload, "stage_name", label="stage_instance_plan", non_empty=True
        ),
        stage_kind=required_string(
            payload, "stage_kind", label="stage_instance_plan", non_empty=True
        ),
        entry=entry_plan_from_dict(
            required_object(payload, "entry", label="stage_instance_plan")
        ),
        resources=resource_plan_from_dict(
            required_object(payload, "resources", label="stage_instance_plan")
        ),
        runtime_plan=runtime_plan_from_dict(
            required_object(payload, "runtime_plan", label="stage_instance_plan")
        ),
        environment_name=required_string(
            payload, "environment_name", label="stage_instance_plan"
        ),
        environment_plan=environment_plan_from_dict(
            required_object(payload, "environment_plan", label="stage_instance_plan")
        ),
        before_steps=tuple(
            before_step_plan_from_dict(item)
            for item in required_object_array(
                payload, "before_steps", label="stage_instance_plan"
            )
        ),
        launcher_plan=launcher_plan_from_dict(
            required_object(payload, "launcher_plan", label="stage_instance_plan")
        ),
        artifact_store_plan=artifact_store_plan_from_dict(
            required_object(payload, "artifact_store_plan", label="stage_instance_plan")
        ),
        input_bindings=tuple(
            input_binding_from_dict(item)
            for item in required_object_array(
                payload, "input_bindings", label="stage_instance_plan"
            )
        ),
        output_contract=stage_output_contract_from_dict(
            required_object(payload, "output_contract", label="stage_instance_plan")
        ),
        lineage=required_object(payload, "lineage", label="stage_instance_plan"),
        run_overrides=required_object(
            payload, "run_overrides", label="stage_instance_plan"
        ),
        resource_sizing=resource_sizing_from_dict(
            required_object(payload, "resource_sizing", label="stage_instance_plan")
        ),
        spec_snapshot_digest=required_string(
            payload, "spec_snapshot_digest", label="stage_instance_plan"
        ),
        run_dir_rel=required_string(
            payload, "run_dir_rel", label="stage_instance_plan", non_empty=True
        ),
    )


def entry_plan_from_dict(payload: dict[str, Any]) -> EntryPlan:
    return EntryPlan(
        type=required_string(payload, "type", label="entry_plan", non_empty=True),
        script=required_nullable_string(payload, "script", label="entry_plan"),
        command=_required_command(payload, "command", label="entry_plan"),
        workdir=required_string(payload, "workdir", label="entry_plan"),
        args=required_object(payload, "args", label="entry_plan"),
    )


def group_plan_from_dict(payload: dict[str, Any]) -> GroupPlan:
    require_plan_schema(payload, name="group_plan")
    return GroupPlan(
        group_id=required_string(payload, "group_id", label="group_plan", non_empty=True),
        group_index=required_int(payload, "group_index", label="group_plan"),
        resource_key=required_string(
            payload, "resource_key", label="group_plan", non_empty=True
        ),
        resources=resource_plan_from_dict(
            required_object(payload, "resources", label="group_plan")
        ),
        stage_instance_ids=required_string_array(
            payload, "stage_instance_ids", label="group_plan"
        ),
        run_ids=required_string_array(payload, "run_ids", label="group_plan"),
        array_size=required_int(payload, "array_size", label="group_plan"),
        array_throttle=required_nullable_int(
            payload, "array_throttle", label="group_plan"
        ),
        gpus_per_task=required_int(payload, "gpus_per_task", label="group_plan"),
    )


def stage_batch_plan_from_dict(payload: dict[str, Any]) -> StageBatchPlan:
    require_plan_schema(payload, name="stage_batch_plan")
    return StageBatchPlan(
        batch_id=required_string(
            payload, "batch_id", label="stage_batch_plan", non_empty=True
        ),
        stage_name=required_string(
            payload, "stage_name", label="stage_batch_plan", non_empty=True
        ),
        project=required_string(
            payload, "project", label="stage_batch_plan", non_empty=True
        ),
        experiment=required_string(
            payload, "experiment", label="stage_batch_plan", non_empty=True
        ),
        selected_runs=required_string_array(
            payload, "selected_runs", label="stage_batch_plan"
        ),
        stage_instances=tuple(
            stage_instance_plan_from_dict(item)
            for item in required_object_array(
                payload, "stage_instances", label="stage_batch_plan"
            )
        ),
        group_plans=tuple(
            group_plan_from_dict(item)
            for item in required_object_array(
                payload, "group_plans", label="stage_batch_plan"
            )
        ),
        submission_root=required_string(
            payload, "submission_root", label="stage_batch_plan", non_empty=True
        ),
        source_ref=required_string(
            payload, "source_ref", label="stage_batch_plan", non_empty=True
        ),
        spec_snapshot_digest=required_string(
            payload, "spec_snapshot_digest", label="stage_batch_plan"
        ),
        budget_plan=budget_plan_from_dict(
            required_object(payload, "budget_plan", label="stage_batch_plan")
        ),
        notification_plan=notification_plan_from_dict(
            required_object(payload, "notification_plan", label="stage_batch_plan")
        ),
    )


def _required_command(
    payload: dict[str, Any], field_name: str, *, label: str
) -> str | list[str] | None:
    if field_name not in payload:
        raise RecordContractError(f"{label}.{field_name} is required")
    value = payload[field_name]
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, list):
        result: list[str] = []
        for item in value:
            if not isinstance(item, str):
                raise RecordContractError(f"{label}.{field_name} items must be strings")
            result.append(item)
        return result
    raise RecordContractError(f"{label}.{field_name} must be a string, array, or null")
