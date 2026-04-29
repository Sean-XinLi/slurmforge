from __future__ import annotations

import copy
from dataclasses import replace

from ..errors import ConfigContractError
from ..io import content_digest
from ..plans.budget import (
    BudgetDependencyPlan,
    BudgetGroupPlan,
    BudgetPlan,
    BudgetWaveGroupPlan,
    BudgetWavePlan,
)
from ..plans.resources import ResourcePlan
from ..plans.stage import GroupPlan, StageInstancePlan


def _group_key(instance: StageInstancePlan) -> str:
    payload = {
        "resources": instance.resources,
        "runtime_plan": instance.runtime_plan,
        "environment_plan": instance.environment_plan,
    }
    return content_digest(payload, prefix=16)


def group_stage_instances(
    instances: tuple[StageInstancePlan, ...],
) -> tuple[GroupPlan, ...]:
    by_key: dict[str, list[StageInstancePlan]] = {}
    resources_by_key: dict[str, ResourcePlan] = {}
    for instance in instances:
        key = _group_key(instance)
        by_key.setdefault(key, []).append(instance)
        resources_by_key[key] = instance.resources
    groups: list[GroupPlan] = []
    for group_index, key in enumerate(sorted(by_key), start=1):
        items = tuple(sorted(by_key[key], key=lambda item: item.run_index))
        resources = resources_by_key[key]
        gpus_per_task = resources.total_gpus
        groups.append(
            GroupPlan(
                group_id=f"group_{group_index:03d}",
                group_index=group_index,
                resource_key=key,
                resources=copy.deepcopy(resources),
                stage_instance_ids=tuple(item.stage_instance_id for item in items),
                run_ids=tuple(item.run_id for item in items),
                array_size=len(items),
                array_throttle=None,
                gpus_per_task=gpus_per_task,
            )
        )
    return tuple(groups)


def _allocate_wave_throttles(
    wave_groups: list[GroupPlan],
    *,
    max_available_gpus: int,
) -> dict[str, int]:
    throttles = {group.group_id: 1 for group in wave_groups}
    used = sum(group.gpus_per_task for group in wave_groups)
    remaining = max_available_gpus - used
    for group in wave_groups:
        if remaining <= 0:
            break
        extra = min(group.array_size - 1, remaining // group.gpus_per_task)
        if extra > 0:
            throttles[group.group_id] += extra
            remaining -= extra * group.gpus_per_task
    return throttles


def apply_budget_plan(
    groups: tuple[GroupPlan, ...],
    *,
    max_available_gpus: int,
    overflow_policy: str,
) -> tuple[tuple[GroupPlan, ...], BudgetPlan]:
    gpu_groups = [group for group in groups if group.gpus_per_task > 0]
    cpu_groups = [group for group in groups if group.gpus_per_task <= 0]
    warnings: list[str] = []
    dependencies: list[BudgetDependencyPlan] = []
    policy_applied = "none"
    waves: list[BudgetWavePlan] = []
    throttles: dict[str, int | None] = {group.group_id: None for group in groups}

    if max_available_gpus <= 0 or not gpu_groups:
        policy_applied = "unlimited" if gpu_groups else "none"
        for group in groups:
            throttles[group.group_id] = None
    else:
        for group in gpu_groups:
            if group.gpus_per_task > max_available_gpus:
                raise ConfigContractError(
                    f"{group.stage_instance_ids[0]} needs {group.gpus_per_task} GPUs per task, "
                    f"above dispatch.max_available_gpus={max_available_gpus}"
                )
        current: list[GroupPlan] = []
        current_used = 0
        for group in gpu_groups:
            if current and current_used + group.gpus_per_task > max_available_gpus:
                wave_throttles = _allocate_wave_throttles(
                    current, max_available_gpus=max_available_gpus
                )
                waves.append(_wave_payload(waves, current, wave_throttles))
                throttles.update(wave_throttles)
                current = []
                current_used = 0
            current.append(group)
            current_used += group.gpus_per_task
        if current:
            wave_throttles = _allocate_wave_throttles(
                current, max_available_gpus=max_available_gpus
            )
            waves.append(_wave_payload(waves, current, wave_throttles))
            throttles.update(wave_throttles)
        policy_applied = "global_waves" if len(waves) > 1 else "global_shared_budget"
        for previous, current_wave in zip(waves, waves[1:]):
            from_groups = tuple(item.group_id for item in previous.groups)
            for item in current_wave.groups:
                dependencies.append(
                    BudgetDependencyPlan(
                        from_groups=from_groups,
                        to_group=item.group_id,
                        type="afterany",
                        from_wave=previous.wave_id,
                        to_wave=current_wave.wave_id,
                    )
                )
        if overflow_policy == "best_effort":
            warnings.append(
                "best_effort accepted; global wave planning still enforces the GPU ceiling"
            )
        if overflow_policy == "error" and len(waves) > 1:
            raise ConfigContractError(
                "stage batch requires multiple GPU waves to satisfy dispatch.max_available_gpus; "
                "use overflow_policy=serialize_groups"
            )

    updated_groups = tuple(
        replace(group, array_throttle=throttles[group.group_id]) for group in groups
    )
    budget_plan = BudgetPlan(
        max_available_gpus=max_available_gpus,
        overflow_policy=overflow_policy,
        policy_applied=policy_applied,
        waves=tuple(waves),
        groups=tuple(
            BudgetGroupPlan(
                group_id=group.group_id,
                gpus_per_task=group.gpus_per_task,
                array_size=group.array_size,
                array_throttle=throttles[group.group_id],
                budgeted_gpus=None
                if throttles[group.group_id] is None
                else group.gpus_per_task * int(throttles[group.group_id] or 0),
            )
            for group in groups
        ),
        cpu_groups=tuple(group.group_id for group in cpu_groups),
        dependencies=tuple(dependencies),
        warnings=tuple(warnings),
    )
    return updated_groups, budget_plan


def _wave_payload(
    existing_waves: list[BudgetWavePlan],
    groups: list[GroupPlan],
    throttles: dict[str, int],
) -> BudgetWavePlan:
    return BudgetWavePlan(
        wave_id=f"wave_{len(existing_waves) + 1:03d}",
        groups=tuple(
            BudgetWaveGroupPlan(
                group_id=item.group_id,
                gpus_per_task=item.gpus_per_task,
                array_size=item.array_size,
                array_throttle=throttles[item.group_id],
            )
            for item in groups
        ),
        total_wave_gpus=sum(
            item.gpus_per_task * throttles[item.group_id] for item in groups
        ),
    )
