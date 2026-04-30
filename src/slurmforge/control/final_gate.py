from __future__ import annotations

from pathlib import Path
from typing import Any

from ..plans.train_eval import FINAL_GATE
from ..root_model.snapshots import (
    refresh_stage_batch_status,
    refresh_train_eval_pipeline_status,
)
from ..slurm import SlurmClientProtocol
from ..storage.plan_reader import load_stage_batch_plan
from ..submission.reconcile import reconcile_batch_submission
from .eval_shard import reconcile_eval_shard
from .gates import submit_control_gate
from .stage_runtime import batch_terminal
from .state import save_workflow_state
from .state_model import (
    TRAIN_STAGE,
    final_gate_state,
    set_workflow_status,
    submitted_gate_job_ids,
    train_groups,
)
from .terminal import complete_pipeline
from .train_group import (
    all_groups_have_terminal_dependencies,
    terminal_gate_job_ids,
)


def ensure_final_gate_submitted(
    pipeline_root: Path,
    state: dict[str, Any],
    plan,
    *,
    client: SlurmClientProtocol,
    max_dependency_length: int,
) -> str | None:
    final = final_gate_state(state)
    existing_job_id = submitted_gate_job_ids(pipeline_root).get(FINAL_GATE)
    if existing_job_id:
        final["state"] = "submitted"
        final["gate_key"] = FINAL_GATE
        save_workflow_state(pipeline_root, state)
        return existing_job_id
    if not all_groups_have_terminal_dependencies(pipeline_root, state):
        return None
    dependency_job_ids = terminal_gate_job_ids(pipeline_root, state)
    if dependency_job_ids is None:
        return None
    final_job_id = submit_control_gate(
        pipeline_root,
        state,
        plan,
        FINAL_GATE,
        dependency_job_ids=dependency_job_ids,
        client=client,
        max_dependency_length=max_dependency_length,
    )
    final.update(
        {
            "state": "submitted",
            "gate_key": FINAL_GATE,
            "dependency_job_ids": list(dependency_job_ids),
        }
    )
    state["state"] = "final_gate_submitted"
    save_workflow_state(pipeline_root, state)
    set_workflow_status(
        pipeline_root,
        state,
        "final_gate_submitted",
        reason="all train groups have terminal dependencies; final gate is queued",
    )
    return final_job_id


def advance_final(
    pipeline_root: Path,
    plan,
    state: dict[str, Any],
    *,
    client: SlurmClientProtocol,
    missing_output_grace_seconds: int,
) -> None:
    state["state"] = "finalizing"
    state["current_stage"] = None
    save_workflow_state(pipeline_root, state)
    train_batch = load_stage_batch_plan(
        Path(plan.stage_batches[TRAIN_STAGE].submission_root)
    )
    reconcile_batch_submission(
        Path(train_batch.submission_root),
        client=client,
        missing_output_grace_seconds=missing_output_grace_seconds,
    )
    refresh_stage_batch_status(Path(train_batch.submission_root))
    for _, record in sorted(train_groups(state).items()):
        shard_root_raw = record.get("eval_shard_root")
        if not shard_root_raw:
            continue
        shard_root = Path(str(shard_root_raw))
        if not (shard_root / "manifest.json").exists():
            continue
        reconcile_eval_shard(
            pipeline_root,
            shard_root,
            client=client,
            missing_output_grace_seconds=missing_output_grace_seconds,
        )
        if batch_terminal(shard_root):
            record["state"] = "terminal"
    refresh_train_eval_pipeline_status(pipeline_root)
    complete_pipeline(pipeline_root, state, notification_plan=plan.notification_plan)
