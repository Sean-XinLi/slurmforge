from __future__ import annotations

from pathlib import Path

from ..errors import ConfigContractError
from ..slurm import SlurmClientProtocol
from ..storage.plan_reader import load_execution_stage_batch_plan
from ..workflow_contract import (
    TRAIN_GROUP_EVAL_MISSING,
    TRAIN_GROUP_TERMINAL,
    TRAIN_GROUP_WAITING_EVAL,
    WORKFLOW_STREAMING,
)
from .eval_reconcile import reconcile_eval_shard
from .eval_selection import eval_shard_root
from .final_gate import ensure_final_gate_submitted
from .stage_runtime import batch_terminal
from .state import save_workflow_state
from .state_model import (
    PipelineAdvanceResult,
    result_from_state,
    set_workflow_status,
)
from .state_records import WorkflowState


def advance_eval_shard(
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
        raise ConfigContractError(f"Unknown eval shard for pipeline: {group_id}")
    record = groups[group_id]
    shard_root = Path(record.eval_shard_root or eval_shard_root(plan, group_id))
    if not (shard_root / "manifest.json").exists():
        record.state = TRAIN_GROUP_EVAL_MISSING
        save_workflow_state(pipeline_root, state)
        return result_from_state(pipeline_root, state)
    reconcile_eval_shard(
        pipeline_root,
        shard_root,
        client=client,
        missing_output_grace_seconds=missing_output_grace_seconds,
    )
    eval_batch = load_execution_stage_batch_plan(shard_root)
    if not batch_terminal(shard_root):
        record.state = TRAIN_GROUP_WAITING_EVAL
        state.state = WORKFLOW_STREAMING
        save_workflow_state(pipeline_root, state)
        set_workflow_status(
            pipeline_root,
            state,
            WORKFLOW_STREAMING,
            reason=f"eval shard for train group `{group_id}` is not terminal",
        )
        return result_from_state(pipeline_root, state)
    record.state = TRAIN_GROUP_TERMINAL
    record.eval_shard_group_count = len(eval_batch.group_plans)
    save_workflow_state(pipeline_root, state)
    ensure_final_gate_submitted(
        pipeline_root,
        state,
        plan,
        client=client,
        max_dependency_length=max_dependency_length,
    )
    return result_from_state(pipeline_root, state)
