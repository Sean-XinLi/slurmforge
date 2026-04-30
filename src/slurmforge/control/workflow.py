from __future__ import annotations

from contextlib import contextmanager
import fcntl
from pathlib import Path
from typing import Iterator

from ..control_paths import workflow_lock_path, workflow_traceback_path
from ..errors import ConfigContractError
from ..io import write_exception_diagnostic
from ..slurm import SlurmClient, SlurmClientProtocol
from ..storage.plan_reader import load_train_eval_pipeline_plan
from ..submission.dependency_tree import MAX_DEPENDENCY_LENGTH
from ..workflow_contract import (
    EVAL_SHARD_GATE,
    FINAL_GATE,
    TRAIN_GROUP_GATE,
    WORKFLOW_FAILED,
    WORKFLOW_TERMINAL_STATES,
)
from .auto_advance import advance_next_ready
from .eval_transition import advance_eval_shard
from .final_gate import advance_final
from .initial_submit import submit_initial_pipeline_locked
from .state import load_workflow_state, save_workflow_state
from .state_model import (
    PipelineAdvanceResult,
    result_from_state,
    set_workflow_status,
)
from .train_transition import advance_train_group


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


def submit_initial_pipeline(
    plan,
    *,
    client: SlurmClientProtocol | None = None,
    max_dependency_length: int = MAX_DEPENDENCY_LENGTH,
) -> PipelineAdvanceResult:
    pipeline_root = Path(plan.root_dir).resolve()
    with _pipeline_lock(pipeline_root):
        return submit_initial_pipeline_locked(
            plan,
            client=client or SlurmClient(),
            max_dependency_length=max_dependency_length,
        )


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
        _validate_gate_request(gate, group_id=group_id)
        if state.current_stage is None and state.state in WORKFLOW_TERMINAL_STATES:
            return result_from_state(root, state)
        try:
            if gate == TRAIN_GROUP_GATE:
                return advance_train_group(
                    root,
                    plan,
                    state,
                    str(group_id),
                    client=slurm,
                    missing_output_grace_seconds=missing_output_grace_seconds,
                    max_dependency_length=max_dependency_length,
                )
            if gate == EVAL_SHARD_GATE:
                return advance_eval_shard(
                    root,
                    plan,
                    state,
                    str(group_id),
                    client=slurm,
                    missing_output_grace_seconds=missing_output_grace_seconds,
                    max_dependency_length=max_dependency_length,
                )
            if gate == FINAL_GATE:
                advance_final(
                    root,
                    plan,
                    state,
                    client=slurm,
                    missing_output_grace_seconds=missing_output_grace_seconds,
                )
                return result_from_state(root, state)
            if not state.train_groups:
                return submit_initial_pipeline_locked(
                    plan,
                    client=slurm,
                    max_dependency_length=max_dependency_length,
                )
            return advance_next_ready(
                root,
                plan,
                state,
                client=slurm,
                missing_output_grace_seconds=missing_output_grace_seconds,
                max_dependency_length=max_dependency_length,
            )
        except Exception as exc:
            write_exception_diagnostic(workflow_traceback_path(root), exc)
            state.state = WORKFLOW_FAILED
            save_workflow_state(root, state)
            set_workflow_status(root, state, WORKFLOW_FAILED, reason=str(exc))
            raise


def _validate_gate_request(gate: str | None, *, group_id: str | None) -> None:
    if gate not in {None, TRAIN_GROUP_GATE, EVAL_SHARD_GATE, FINAL_GATE}:
        raise ConfigContractError(f"Unsupported pipeline gate: {gate}")
    if gate in {TRAIN_GROUP_GATE, EVAL_SHARD_GATE} and not group_id:
        raise ConfigContractError(f"`--group-id` is required for {gate}")
