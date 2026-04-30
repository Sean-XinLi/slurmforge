from __future__ import annotations

from pathlib import Path

from ..slurm import SlurmClientProtocol
from ..workflow_contract import TRAIN_GROUP_EVAL_SKIPPED, TRAIN_GROUP_TERMINAL
from .eval_transition import advance_eval_shard
from .final_gate import advance_final, ensure_final_gate_submitted
from .state_model import PipelineAdvanceResult, result_from_state
from .state_records import WorkflowState
from .train_transition import advance_train_group


def advance_next_ready(
    pipeline_root: Path,
    plan,
    state: WorkflowState,
    *,
    client: SlurmClientProtocol,
    missing_output_grace_seconds: int,
    max_dependency_length: int,
) -> PipelineAdvanceResult:
    for next_group_id, record in sorted(state.train_groups.items()):
        if not record.terminal_dependency_gate_key:
            return advance_train_group(
                pipeline_root,
                plan,
                state,
                next_group_id,
                client=client,
                missing_output_grace_seconds=missing_output_grace_seconds,
                max_dependency_length=max_dependency_length,
            )
    for next_group_id, record in sorted(state.train_groups.items()):
        if (
            record.eval_shard_root
            and record.state not in {TRAIN_GROUP_TERMINAL, TRAIN_GROUP_EVAL_SKIPPED}
        ):
            return advance_eval_shard(
                pipeline_root,
                plan,
                state,
                next_group_id,
                client=client,
                missing_output_grace_seconds=missing_output_grace_seconds,
                max_dependency_length=max_dependency_length,
            )
    ensure_final_gate_submitted(
        pipeline_root,
        state,
        plan,
        client=client,
        max_dependency_length=max_dependency_length,
    )
    if state.train_groups and all(
        record.state in {TRAIN_GROUP_TERMINAL, TRAIN_GROUP_EVAL_SKIPPED}
        for record in state.train_groups.values()
    ):
        advance_final(
            pipeline_root,
            plan,
            state,
            client=client,
            missing_output_grace_seconds=missing_output_grace_seconds,
        )
    return result_from_state(pipeline_root, state)
