from __future__ import annotations

from typing import Any

from ...io import SchemaVersion, require_schema
from ...record_fields import (
    required_int,
    required_nullable_int,
    required_object_array,
    required_string,
    required_string_array,
)
from ..budget import (
    BudgetDependencyPlan,
    BudgetGroupPlan,
    BudgetPlan,
    BudgetWaveGroupPlan,
    BudgetWavePlan,
)


def budget_plan_from_dict(payload: dict[str, Any]) -> BudgetPlan:
    require_schema(payload, name="budget_plan", version=SchemaVersion.BUDGET_PLAN)
    return BudgetPlan(
        max_available_gpus=required_int(
            payload, "max_available_gpus", label="budget_plan"
        ),
        overflow_policy=required_string(
            payload, "overflow_policy", label="budget_plan", non_empty=True
        ),
        policy_applied=required_string(
            payload, "policy_applied", label="budget_plan", non_empty=True
        ),
        waves=tuple(
            _budget_wave_from_dict(wave)
            for wave in required_object_array(payload, "waves", label="budget_plan")
        ),
        groups=tuple(
            _budget_group_from_dict(group)
            for group in required_object_array(payload, "groups", label="budget_plan")
        ),
        cpu_groups=required_string_array(payload, "cpu_groups", label="budget_plan"),
        dependencies=tuple(
            _budget_dependency_from_dict(dep)
            for dep in required_object_array(
                payload, "dependencies", label="budget_plan"
            )
        ),
        warnings=required_string_array(payload, "warnings", label="budget_plan"),
    )


def _budget_wave_from_dict(payload: dict[str, Any]) -> BudgetWavePlan:
    label = "budget_plan.waves"
    return BudgetWavePlan(
        wave_id=required_string(payload, "wave_id", label=label, non_empty=True),
        groups=tuple(
            _budget_wave_group_from_dict(group)
            for group in required_object_array(payload, "groups", label=label)
        ),
        total_wave_gpus=required_int(payload, "total_wave_gpus", label=label),
    )


def _budget_wave_group_from_dict(payload: dict[str, Any]) -> BudgetWaveGroupPlan:
    label = "budget_plan.waves.groups"
    return BudgetWaveGroupPlan(
        group_id=required_string(payload, "group_id", label=label, non_empty=True),
        gpus_per_task=required_int(payload, "gpus_per_task", label=label),
        array_size=required_int(payload, "array_size", label=label),
        array_throttle=required_int(payload, "array_throttle", label=label),
    )


def _budget_group_from_dict(payload: dict[str, Any]) -> BudgetGroupPlan:
    label = "budget_plan.groups"
    return BudgetGroupPlan(
        group_id=required_string(payload, "group_id", label=label, non_empty=True),
        gpus_per_task=required_int(payload, "gpus_per_task", label=label),
        array_size=required_int(payload, "array_size", label=label),
        array_throttle=required_nullable_int(payload, "array_throttle", label=label),
        budgeted_gpus=required_nullable_int(payload, "budgeted_gpus", label=label),
    )


def _budget_dependency_from_dict(payload: dict[str, Any]) -> BudgetDependencyPlan:
    label = "budget_plan.dependencies"
    return BudgetDependencyPlan(
        from_groups=required_string_array(payload, "from_groups", label=label),
        to_group=required_string(payload, "to_group", label=label, non_empty=True),
        type=required_string(payload, "type", label=label, non_empty=True),
        from_wave=required_string(payload, "from_wave", label=label),
        to_wave=required_string(payload, "to_wave", label=label),
    )
