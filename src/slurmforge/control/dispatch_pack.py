from __future__ import annotations

from datetime import datetime, timezone

from ..io import content_digest
from ..workflow_contract import EVAL_STAGE, TRAIN_STAGE
from ..storage.workflow_state_records import INSTANCE_READY, RELEASE_WINDOWED, WorkflowState


def ready_instance_ids(
    state: WorkflowState, *, stage_name: str
) -> tuple[str, ...]:
    return tuple(
        instance_id
        for instance_id in state.dispatch_queue
        if state.instances[instance_id].stage_name == stage_name
        and state.instances[instance_id].state == INSTANCE_READY
    )


def ready_train_ids(state: WorkflowState) -> tuple[str, ...]:
    return ready_instance_ids(state, stage_name=TRAIN_STAGE)


def ready_eval_ids(state: WorkflowState) -> tuple[str, ...]:
    return ready_instance_ids(state, stage_name=EVAL_STAGE)


def window_allows_dispatch(plan, state: WorkflowState, ready_ids: tuple[str, ...]) -> bool:
    if state.release_policy != RELEASE_WINDOWED:
        return True
    window_size = int(getattr(plan, "dispatch_window_size", 1) or 1)
    window_seconds = int(getattr(plan, "dispatch_window_seconds", 0) or 0)
    if len(ready_ids) >= window_size or window_seconds <= 0:
        return True
    now = datetime.now(timezone.utc)
    for instance_id in ready_ids:
        ready_at = state.instances[instance_id].ready_at
        if not ready_at:
            continue
        if (now - datetime.fromisoformat(ready_at)).total_seconds() >= window_seconds:
            return True
    return False


def dispatch_submission_id(stage_name: str, instance_ids: tuple[str, ...]) -> str:
    digest = content_digest(
        {"stage": stage_name, "instances": sorted(instance_ids)}, prefix=12
    )
    return f"{stage_name}_{digest}"
