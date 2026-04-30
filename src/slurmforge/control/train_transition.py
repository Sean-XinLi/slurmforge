from __future__ import annotations

from pathlib import Path

from ..errors import ConfigContractError
from ..slurm import SlurmClientProtocol
from ..storage.plan_reader import load_stage_batch_plan
from ..submission.dependency_tree import dependency_sink_group_ids
from ..workflow_contract import (
    EVAL_SHARD_GATE,
    TRAIN_STAGE,
    TRAIN_GROUP_EVAL_GATE_SUBMITTED,
    TRAIN_GROUP_RECONCILED,
    TRAIN_GROUP_WAITING_TRAIN,
    WORKFLOW_STREAMING,
)
from .eval_materialization import ensure_eval_shard_materialized
from .final_gate import ensure_final_gate_submitted
from .gate_ledger import gate_ledger_key
from .gates import submit_control_gate
from .stage_submit import ensure_stage_submitted
from .state import save_workflow_state
from .state_model import PipelineAdvanceResult, result_from_state, set_workflow_status
from .state_records import WorkflowState
from .train_group import (
    group_terminal,
    reconcile_train_group,
)


def advance_train_group(
    pipeline_root: Path,
    plan,
    state: WorkflowState,
    group_id: str,
    *,
    client: SlurmClientProtocol,
    missing_output_grace_seconds: int,
    max_dependency_length: int,
) -> PipelineAdvanceResult:
    groups = state.train_groups
    if group_id not in groups:
        raise ConfigContractError(f"Unknown train group for pipeline: {group_id}")
    record = groups[group_id]
    if record.terminal_dependency_gate_key:
        ensure_final_gate_submitted(
            pipeline_root,
            state,
            plan,
            client=client,
            max_dependency_length=max_dependency_length,
        )
        return result_from_state(pipeline_root, state)

    train_batch = load_stage_batch_plan(
        Path(plan.stage_batches[TRAIN_STAGE].submission_root)
    )
    reconcile_train_group(
        pipeline_root,
        train_batch,
        group_id,
        client=client,
        missing_output_grace_seconds=missing_output_grace_seconds,
    )
    if not group_terminal(train_batch, group_id):
        record.state = TRAIN_GROUP_WAITING_TRAIN
        state.state = WORKFLOW_STREAMING
        save_workflow_state(pipeline_root, state)
        set_workflow_status(
            pipeline_root,
            state,
            WORKFLOW_STREAMING,
            reason=f"train group `{group_id}` is not terminal after reconciliation",
        )
        return result_from_state(pipeline_root, state)

    record.state = TRAIN_GROUP_RECONCILED
    eval_batch = ensure_eval_shard_materialized(pipeline_root, plan, state, group_id)
    if eval_batch is None:
        ensure_final_gate_submitted(
            pipeline_root,
            state,
            plan,
            client=client,
            max_dependency_length=max_dependency_length,
        )
        return result_from_state(pipeline_root, state)

    eval_submission_job_ids = ensure_stage_submitted(
        pipeline_root,
        eval_batch,
        client=client,
        state_group_id=group_id,
    )
    gate_key = gate_ledger_key(EVAL_SHARD_GATE, group_id=group_id)
    submit_control_gate(
        pipeline_root,
        state,
        plan,
        EVAL_SHARD_GATE,
        group_id=group_id,
        dependency_job_ids=tuple(
            eval_submission_job_ids[group]
            for group in dependency_sink_group_ids(eval_batch)
        ),
        client=client,
        max_dependency_length=max_dependency_length,
    )
    record.eval_shard_gate_key = gate_key
    record.terminal_dependency_gate_key = gate_key
    record.state = TRAIN_GROUP_EVAL_GATE_SUBMITTED
    state.state = WORKFLOW_STREAMING
    save_workflow_state(pipeline_root, state)
    set_workflow_status(
        pipeline_root,
        state,
        WORKFLOW_STREAMING,
        reason=f"eval shard for train group `{group_id}` submitted",
    )
    ensure_final_gate_submitted(
        pipeline_root,
        state,
        plan,
        client=client,
        max_dependency_length=max_dependency_length,
    )
    return result_from_state(pipeline_root, state)
