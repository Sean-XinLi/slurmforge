from __future__ import annotations

from pathlib import Path
from typing import Any

from ..slurm import SlurmClientProtocol
from .eval_transition import advance_eval_shard
from .final_gate import advance_final, ensure_final_gate_submitted
from .state_model import PipelineAdvanceResult, result_from_state, train_groups
from .train_transition import advance_train_group


def advance_next_ready(
    pipeline_root: Path,
    plan,
    state: dict[str, Any],
    *,
    client: SlurmClientProtocol,
    missing_output_grace_seconds: int,
    max_dependency_length: int,
) -> PipelineAdvanceResult:
    for next_group_id, record in sorted(train_groups(state).items()):
        if not record.get("terminal_dependency_gate_key"):
            return advance_train_group(
                pipeline_root,
                plan,
                state,
                next_group_id,
                client=client,
                missing_output_grace_seconds=missing_output_grace_seconds,
                max_dependency_length=max_dependency_length,
            )
    for next_group_id, record in sorted(train_groups(state).items()):
        if (
            record.get("eval_shard_root")
            and record.get("state") not in {"terminal", "eval_skipped"}
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
    if train_groups(state) and all(
        record.get("state") in {"terminal", "eval_skipped"}
        for record in train_groups(state).values()
    ):
        advance_final(
            pipeline_root,
            plan,
            state,
            client=client,
            missing_output_grace_seconds=missing_output_grace_seconds,
        )
    return result_from_state(pipeline_root, state)
