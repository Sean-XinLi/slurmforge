from __future__ import annotations

from pathlib import Path

from ..slurm import SlurmClientProtocol
from ..submission.dependency_tree import MAX_DEPENDENCY_LENGTH
from ..workflow_contract import EVAL_STAGE, TRAIN_STAGE, WORKFLOW_STREAMING
from .dispatch_budget import select_instances_with_budget
from .dispatch_materialization import materialize_eval_dispatch
from .dispatch_pack import (
    dispatch_submission_id,
    ready_eval_ids,
    ready_train_ids,
    window_allows_dispatch,
)
from .dispatch_submit import submit_dispatch
from .instance_reconcile import sync_materialized_statuses
from .state import record_workflow_event
from ..storage.workflow_state_records import (
    DISPATCH_ROLE_DISPATCH,
    DISPATCH_ROLE_INITIAL,
    WorkflowState,
    dequeue_instances,
)


def dispatch_ready_instances(
    pipeline_root: Path,
    plan,
    state: WorkflowState,
    *,
    client: SlurmClientProtocol,
    max_dependency_length: int = MAX_DEPENDENCY_LENGTH,
) -> WorkflowState:
    _dispatch_initial_train(
        pipeline_root,
        plan,
        state,
        client=client,
        max_dependency_length=max_dependency_length,
    )
    _dispatch_ready_eval(
        pipeline_root,
        plan,
        state,
        client=client,
        max_dependency_length=max_dependency_length,
    )
    return state


def _dispatch_initial_train(
    pipeline_root: Path,
    plan,
    state: WorkflowState,
    *,
    client: SlurmClientProtocol,
    max_dependency_length: int,
) -> None:
    if _has_stage_submission(state, TRAIN_STAGE):
        return
    train_ids = ready_train_ids(state)
    if not train_ids:
        return
    batch = plan.stage_batches[TRAIN_STAGE]
    submission_id = f"{TRAIN_STAGE}_initial"
    submit_dispatch(
        pipeline_root,
        plan,
        state,
        batch,
        submission_id=submission_id,
        role=DISPATCH_ROLE_INITIAL,
        display_key=TRAIN_STAGE,
        client=client,
        max_dependency_length=max_dependency_length,
    )
    dequeue_instances(state, set(train_ids))
    state.state = WORKFLOW_STREAMING
    state.current_stage = TRAIN_STAGE
    record_workflow_event(
        pipeline_root,
        "dispatch_submitted",
        submission_id=submission_id,
        stage=TRAIN_STAGE,
        instance_ids=list(train_ids),
    )


def _dispatch_ready_eval(
    pipeline_root: Path,
    plan,
    state: WorkflowState,
    *,
    client: SlurmClientProtocol,
    max_dependency_length: int,
) -> None:
    ready_ids = ready_eval_ids(state)
    if not ready_ids or not window_allows_dispatch(plan, state, ready_ids):
        return
    selected_ids = select_instances_with_budget(plan, state, ready_ids)
    if not selected_ids:
        return
    submission_id = dispatch_submission_id(EVAL_STAGE, selected_ids)
    materialized = materialize_eval_dispatch(
        pipeline_root,
        plan,
        state,
        submission_id=submission_id,
        selected_ids=selected_ids,
    )
    if materialized is None:
        return
    submit_dispatch(
        pipeline_root,
        plan,
        state,
        materialized.batch,
        submission_id=submission_id,
        role=DISPATCH_ROLE_DISPATCH,
        display_key=f"{EVAL_STAGE}:{submission_id}",
        client=client,
        max_dependency_length=max_dependency_length,
    )
    dequeue_instances(state, set(materialized.selected_instance_ids))
    state.current_stage = EVAL_STAGE
    sync_materialized_statuses(state, materialized.batch)
    record_workflow_event(
        pipeline_root,
        "dispatch_submitted",
        submission_id=submission_id,
        stage=EVAL_STAGE,
        instance_ids=list(materialized.selected_instance_ids),
    )


def _has_stage_submission(state: WorkflowState, stage_name: str) -> bool:
    return any(
        submission.stage_name == stage_name for submission in state.submissions.values()
    )
