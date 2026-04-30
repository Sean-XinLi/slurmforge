from __future__ import annotations

from ..workflow_contract import EVAL_STAGE
from .state_records import (
    DISPATCH_ACTIVE_STATES,
    INSTANCE_RUNNING,
    INSTANCE_SUBMITTED,
    WorkflowState,
)


def active_budgeted_gpus(state: WorkflowState) -> int:
    return sum(_submission_active_gpus(state, submission) for submission in state.submissions.values())


def select_instances_with_budget(
    plan,
    state: WorkflowState,
    ready_ids: tuple[str, ...],
) -> tuple[str, ...]:
    max_gpus = plan.stage_batches[EVAL_STAGE].budget_plan.max_available_gpus
    if max_gpus <= 0:
        return ready_ids
    active = active_budgeted_gpus(state)
    selected: list[str] = []
    selected_gpus = 0
    instances_by_id = {
        instance.stage_instance_id: instance
        for instance in plan.stage_batches[EVAL_STAGE].stage_instances
    }
    for instance_id in ready_ids:
        plan_instance = instances_by_id[instance_id]
        required = plan_instance.resources.total_gpus
        if required <= 0:
            selected.append(instance_id)
            continue
        if active + selected_gpus + required > max_gpus:
            continue
        selected.append(instance_id)
        selected_gpus += required
    return tuple(selected)


def _submission_active_gpus(state: WorkflowState, submission) -> int:
    if submission.state not in DISPATCH_ACTIVE_STATES:
        return 0
    active_instance_ids = {
        instance_id
        for instance_id in submission.instance_ids
        if instance_id in state.instances
        and state.instances[instance_id].state in {INSTANCE_SUBMITTED, INSTANCE_RUNNING}
    }
    total = 0
    for group in submission.groups.values():
        active_tasks = sum(
            1
            for instance_id in group.stage_instance_ids
            if instance_id in active_instance_ids
        )
        if active_tasks <= 0:
            continue
        throttle = group.array_throttle or group.array_size
        total += group.gpus_per_task * min(active_tasks, throttle)
    return total
