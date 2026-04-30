from __future__ import annotations

from pathlib import Path

from ..plans.train_eval import TRAIN_GROUP_GATE
from ..slurm import SlurmClientProtocol
from ..storage.plan_reader import load_stage_batch_plan
from .gate_ledger import gate_ledger_key
from .gates import submit_control_gate
from .stage_submit import ensure_stage_submitted
from .state import load_workflow_state, save_workflow_state
from .state_model import (
    TRAIN_STAGE,
    PipelineAdvanceResult,
    result_from_state,
    set_workflow_status,
    train_groups,
)
from .train_group import initialize_train_groups


def submit_initial_pipeline_locked(
    plan,
    *,
    client: SlurmClientProtocol,
    max_dependency_length: int,
) -> PipelineAdvanceResult:
    pipeline_root = Path(plan.root_dir).resolve()
    state = load_workflow_state(pipeline_root, plan)
    train_batch = load_stage_batch_plan(
        Path(plan.stage_batches[TRAIN_STAGE].submission_root)
    )
    group_job_ids = ensure_stage_submitted(
        pipeline_root,
        train_batch,
        client=client,
    )
    initialize_train_groups(state, train_batch)
    for group in train_batch.group_plans:
        record = train_groups(state)[group.group_id]
        gate_key = gate_ledger_key(TRAIN_GROUP_GATE, group_id=group.group_id)
        submit_control_gate(
            pipeline_root,
            state,
            plan,
            TRAIN_GROUP_GATE,
            group_id=group.group_id,
            dependency_job_ids=(group_job_ids[group.group_id],),
            client=client,
            max_dependency_length=max_dependency_length,
        )
        record["train_group_gate_key"] = gate_key
        record["state"] = "train_group_gate_submitted"
    state["state"] = "streaming"
    state["current_stage"] = TRAIN_STAGE
    save_workflow_state(pipeline_root, state)
    set_workflow_status(
        pipeline_root,
        state,
        "streaming",
        reason="train groups submitted; per-group train gates are queued",
    )
    return result_from_state(pipeline_root, state)
