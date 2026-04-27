from __future__ import annotations

from typing import Any

from ...io import SchemaVersion, require_schema
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
        max_available_gpus=int(payload["max_available_gpus"]),
        overflow_policy=str(payload["overflow_policy"]),
        policy_applied=str(payload["policy_applied"]),
        waves=tuple(
            BudgetWavePlan(
                wave_id=str(wave["wave_id"]),
                groups=tuple(
                    BudgetWaveGroupPlan(
                        group_id=str(group["group_id"]),
                        gpus_per_task=int(group["gpus_per_task"]),
                        array_size=int(group["array_size"]),
                        array_throttle=int(group["array_throttle"]),
                    )
                    for group in wave["groups"]
                ),
                total_wave_gpus=int(wave["total_wave_gpus"]),
            )
            for wave in payload["waves"]
        ),
        groups=tuple(
            BudgetGroupPlan(
                group_id=str(group["group_id"]),
                gpus_per_task=int(group["gpus_per_task"]),
                array_size=int(group["array_size"]),
                array_throttle=None if group["array_throttle"] in (None, "") else int(group["array_throttle"]),
                budgeted_gpus=None if group["budgeted_gpus"] in (None, "") else int(group["budgeted_gpus"]),
            )
            for group in payload["groups"]
        ),
        cpu_groups=tuple(str(item) for item in payload["cpu_groups"]),
        dependencies=tuple(
            BudgetDependencyPlan(
                from_groups=tuple(str(item) for item in dep["from_groups"]),
                to_group=str(dep["to_group"]),
                type=str(dep["type"]),
                from_wave=str(dep["from_wave"]),
                to_wave=str(dep["to_wave"]),
            )
            for dep in payload["dependencies"]
        ),
        warnings=tuple(str(item) for item in payload["warnings"]),
    )
