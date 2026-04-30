from __future__ import annotations

from pathlib import Path
from typing import Any

from ..errors import ConfigContractError
from ..slurm import SlurmClientProtocol
from ..storage.plan_reader import load_execution_stage_batch_plan
from .eval_reconcile import reconcile_eval_shard
from .eval_selection import eval_shard_root
from .final_gate import ensure_final_gate_submitted
from .stage_runtime import batch_terminal
from .state import save_workflow_state
from .state_model import (
    PipelineAdvanceResult,
    result_from_state,
    set_workflow_status,
    train_groups,
)


def advance_eval_shard(
    pipeline_root: Path,
    plan,
    state: dict[str, Any],
    group_id: str,
    *,
    client: SlurmClientProtocol,
    missing_output_grace_seconds: int,
    max_dependency_length: int,
) -> PipelineAdvanceResult:
    groups = train_groups(state)
    if group_id not in groups:
        raise ConfigContractError(f"Unknown eval shard for pipeline: {group_id}")
    record = groups[group_id]
    shard_root = Path(str(record.get("eval_shard_root") or eval_shard_root(plan, group_id)))
    if not (shard_root / "manifest.json").exists():
        record["state"] = "eval_missing"
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
        record["state"] = "waiting_eval"
        state["state"] = "streaming"
        save_workflow_state(pipeline_root, state)
        set_workflow_status(
            pipeline_root,
            state,
            "streaming",
            reason=f"eval shard for train group `{group_id}` is not terminal",
        )
        return result_from_state(pipeline_root, state)
    record["state"] = "terminal"
    record["eval_shard_group_count"] = len(eval_batch.group_plans)
    save_workflow_state(pipeline_root, state)
    ensure_final_gate_submitted(
        pipeline_root,
        state,
        plan,
        client=client,
        max_dependency_length=max_dependency_length,
    )
    return result_from_state(pipeline_root, state)
