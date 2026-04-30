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
    DISPATCH_CATCHUP_GATE,
    FINAL_GATE,
    STAGE_INSTANCE_GATE,
    WORKFLOW_FAILED,
    WORKFLOW_STREAMING,
    WORKFLOW_TERMINAL_STATES,
)
from .dependencies import resolve_dependencies
from .dispatch_queue import dispatch_ready_instances
from .finalization import finalize_if_terminal
from .initial_submit import submit_initial_pipeline_locked
from .instance_reconcile import reconcile_instances
from .state import load_workflow_state, save_workflow_state
from .state_model import PipelineAdvanceResult, result_from_state, set_workflow_status


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
    event: str | None = None,
    stage_instance_id: str | None = None,
    client: SlurmClientProtocol | None = None,
    missing_output_grace_seconds: int = 300,
    max_dependency_length: int = MAX_DEPENDENCY_LENGTH,
) -> PipelineAdvanceResult:
    del group_id
    root = Path(pipeline_root).resolve()
    slurm = client or SlurmClient()
    with _pipeline_lock(root):
        plan = load_train_eval_pipeline_plan(root)
        state = load_workflow_state(root, plan)
        _validate_gate_request(gate, event=event, stage_instance_id=stage_instance_id)
        if state.state in WORKFLOW_TERMINAL_STATES:
            return result_from_state(root, state)
        try:
            reconcile_instances(
                root,
                state,
                client=slurm,
                missing_output_grace_seconds=missing_output_grace_seconds,
            )
            resolve_dependencies(plan, state)
            dispatch_ready_instances(
                root,
                plan,
                state,
                client=slurm,
                max_dependency_length=max_dependency_length,
            )
            reconcile_instances(
                root,
                state,
                client=slurm,
                missing_output_grace_seconds=missing_output_grace_seconds,
            )
            finalize_if_terminal(root, state)
            save_workflow_state(root, state)
            if state.state not in WORKFLOW_TERMINAL_STATES:
                set_workflow_status(
                    root,
                    state,
                    WORKFLOW_STREAMING,
                    reason="workflow reconciled; ready instances dispatched",
                )
            return result_from_state(root, state)
        except Exception as exc:
            write_exception_diagnostic(workflow_traceback_path(root), exc)
            state.state = WORKFLOW_FAILED
            save_workflow_state(root, state)
            set_workflow_status(root, state, WORKFLOW_FAILED, reason=str(exc))
            raise


def _validate_gate_request(
    gate: str | None,
    *,
    event: str | None,
    stage_instance_id: str | None,
) -> None:
    if gate not in {None, STAGE_INSTANCE_GATE, DISPATCH_CATCHUP_GATE, FINAL_GATE}:
        raise ConfigContractError(f"Unsupported pipeline gate: {gate}")
    if event and event != "stage-instance-finished":
        raise ConfigContractError(f"Unsupported pipeline event: {event}")
    if event == "stage-instance-finished" and not stage_instance_id:
        raise ConfigContractError(
            "`--stage-instance-id` is required for stage-instance-finished events"
        )
