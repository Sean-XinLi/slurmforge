from __future__ import annotations

from contextlib import contextmanager
import fcntl
from pathlib import Path
from typing import Any, Iterator

from ..control_paths import workflow_lock_path, workflow_traceback_path
from ..errors import ConfigContractError
from ..io import write_exception_diagnostic
from ..plans.train_eval import EVAL_SHARD_GATE, FINAL_GATE, TRAIN_GROUP_GATE
from ..slurm import SlurmClient, SlurmClientProtocol
from ..storage.plan_reader import (
    load_execution_stage_batch_plan,
    load_stage_batch_plan,
    load_train_eval_pipeline_plan,
)
from ..submission.dependency_tree import (
    MAX_DEPENDENCY_LENGTH,
    dependency_sink_group_ids,
)
from .eval_shard import (
    ensure_eval_shard_materialized,
    eval_shard_root,
    reconcile_eval_shard,
)
from .final_gate import advance_final, ensure_final_gate_submitted
from .gate_ledger import gate_ledger_key
from .gates import submit_control_gate
from .stage_runtime import batch_terminal
from .stage_submit import ensure_stage_submitted
from .state import load_workflow_state, save_workflow_state
from .state_model import (
    EVAL_STAGE,
    TRAIN_STAGE,
    PipelineAdvanceResult,
    result_from_state,
    set_workflow_status,
    train_groups,
)
from .train_group import (
    group_terminal,
    initialize_train_groups,
    reconcile_train_group,
)


@contextmanager
def _pipeline_lock(pipeline_root: Path) -> Iterator[None]:
    lock_path = workflow_lock_path(pipeline_root)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w", encoding="utf-8") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _submit_initial_pipeline_locked(
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
        state,
        train_batch,
        client=client,
    )
    initialize_train_groups(state, train_batch, group_job_ids)
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


def submit_initial_pipeline(
    plan,
    *,
    client: SlurmClientProtocol | None = None,
    max_dependency_length: int = MAX_DEPENDENCY_LENGTH,
) -> PipelineAdvanceResult:
    pipeline_root = Path(plan.root_dir).resolve()
    with _pipeline_lock(pipeline_root):
        return _submit_initial_pipeline_locked(
            plan,
            client=client or SlurmClient(),
            max_dependency_length=max_dependency_length,
        )


def _advance_train_group_locked(
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
        raise ConfigContractError(f"Unknown train group for pipeline: {group_id}")
    record = groups[group_id]
    if record.get("terminal_dependency_gate_key"):
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
        train_job_id=str(record["train_job_id"]),
        client=client,
        missing_output_grace_seconds=missing_output_grace_seconds,
    )
    if not group_terminal(train_batch, group_id):
        record["state"] = "waiting_train"
        state["state"] = "streaming"
        save_workflow_state(pipeline_root, state)
        set_workflow_status(
            pipeline_root,
            state,
            "streaming",
            reason=f"train group `{group_id}` is not terminal after reconciliation",
        )
        return result_from_state(pipeline_root, state)

    record["state"] = "train_reconciled"
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

    eval_job_ids = ensure_stage_submitted(
        pipeline_root,
        state,
        eval_batch,
        client=client,
        state_group_id=group_id,
    )
    record["eval_job_ids"] = dict(eval_job_ids)
    gate_key = gate_ledger_key(EVAL_SHARD_GATE, group_id=group_id)
    submit_control_gate(
        pipeline_root,
        state,
        plan,
        EVAL_SHARD_GATE,
        group_id=group_id,
        dependency_job_ids=tuple(
            eval_job_ids[group] for group in dependency_sink_group_ids(eval_batch)
        ),
        client=client,
        max_dependency_length=max_dependency_length,
    )
    record["eval_shard_gate_key"] = gate_key
    record["terminal_dependency_gate_key"] = gate_key
    record["state"] = "eval_shard_gate_submitted"
    state["state"] = "streaming"
    save_workflow_state(pipeline_root, state)
    set_workflow_status(
        pipeline_root,
        state,
        "streaming",
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


def _advance_eval_shard_locked(
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


def _advance_final_locked(
    pipeline_root: Path,
    plan,
    state: dict[str, Any],
    *,
    client: SlurmClientProtocol,
    missing_output_grace_seconds: int,
) -> PipelineAdvanceResult:
    advance_final(
        pipeline_root,
        plan,
        state,
        client=client,
        missing_output_grace_seconds=missing_output_grace_seconds,
    )
    return result_from_state(pipeline_root, state)


def advance_pipeline_once(
    pipeline_root: Path,
    *,
    gate: str | None = None,
    group_id: str | None = None,
    client: SlurmClientProtocol | None = None,
    missing_output_grace_seconds: int = 300,
    max_dependency_length: int = MAX_DEPENDENCY_LENGTH,
) -> PipelineAdvanceResult:
    root = Path(pipeline_root).resolve()
    slurm = client or SlurmClient()
    with _pipeline_lock(root):
        plan = load_train_eval_pipeline_plan(root)
        state = load_workflow_state(root, plan)
        if gate not in {None, TRAIN_GROUP_GATE, EVAL_SHARD_GATE, FINAL_GATE}:
            raise ConfigContractError(f"Unsupported pipeline gate: {gate}")
        if gate in {TRAIN_GROUP_GATE, EVAL_SHARD_GATE} and not group_id:
            raise ConfigContractError(f"`--group-id` is required for {gate}")
        if state.get("current_stage") is None and str(state.get("state") or "") in {
            "success",
            "failed",
            "blocked",
        }:
            return result_from_state(root, state)
        try:
            if gate == TRAIN_GROUP_GATE:
                return _advance_train_group_locked(
                    root,
                    plan,
                    state,
                    str(group_id),
                    client=slurm,
                    missing_output_grace_seconds=missing_output_grace_seconds,
                    max_dependency_length=max_dependency_length,
                )
            if gate == EVAL_SHARD_GATE:
                return _advance_eval_shard_locked(
                    root,
                    plan,
                    state,
                    str(group_id),
                    client=slurm,
                    missing_output_grace_seconds=missing_output_grace_seconds,
                    max_dependency_length=max_dependency_length,
                )
            if gate == FINAL_GATE:
                return _advance_final_locked(
                    root,
                    plan,
                    state,
                    client=slurm,
                    missing_output_grace_seconds=missing_output_grace_seconds,
                )
            if not train_groups(state):
                return _submit_initial_pipeline_locked(
                    plan,
                    client=slurm,
                    max_dependency_length=max_dependency_length,
                )
            for next_group_id, record in sorted(train_groups(state).items()):
                if not record.get("terminal_dependency_gate_key"):
                    return _advance_train_group_locked(
                        root,
                        plan,
                        state,
                        next_group_id,
                        client=slurm,
                        missing_output_grace_seconds=missing_output_grace_seconds,
                        max_dependency_length=max_dependency_length,
                    )
            for next_group_id, record in sorted(train_groups(state).items()):
                if (
                    record.get("eval_shard_root")
                    and record.get("state") not in {"terminal", "eval_skipped"}
                ):
                    return _advance_eval_shard_locked(
                        root,
                        plan,
                        state,
                        next_group_id,
                        client=slurm,
                        missing_output_grace_seconds=missing_output_grace_seconds,
                        max_dependency_length=max_dependency_length,
                    )
            ensure_final_gate_submitted(
                root,
                state,
                plan,
                client=slurm,
                max_dependency_length=max_dependency_length,
            )
            if train_groups(state) and all(
                record.get("state") in {"terminal", "eval_skipped"}
                for record in train_groups(state).values()
            ):
                return _advance_final_locked(
                    root,
                    plan,
                    state,
                    client=slurm,
                    missing_output_grace_seconds=missing_output_grace_seconds,
                )
            return result_from_state(root, state)
        except Exception as exc:
            write_exception_diagnostic(workflow_traceback_path(root), exc)
            state["state"] = "failed"
            save_workflow_state(root, state)
            set_workflow_status(root, state, "failed", reason=str(exc))
            raise
