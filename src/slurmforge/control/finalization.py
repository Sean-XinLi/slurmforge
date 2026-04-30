from __future__ import annotations

from pathlib import Path

from ..workflow_contract import (
    WORKFLOW_BLOCKED,
    WORKFLOW_FAILED,
    WORKFLOW_SUCCESS,
)
from .state_model import set_workflow_status
from .state_records import (
    INSTANCE_BLOCKED,
    INSTANCE_FAILED,
    INSTANCE_TERMINAL_STATES,
    WorkflowState,
)


def finalize_if_terminal(pipeline_root: Path, state: WorkflowState) -> WorkflowState:
    if not state.instances:
        return state
    if not all(
        instance.state in INSTANCE_TERMINAL_STATES
        for instance in state.instances.values()
    ):
        return state
    terminal_state = _terminal_state(state)
    state.state = terminal_state
    state.current_stage = ""
    state.final_gate.state = "completed"
    state.final_gate.reason = _terminal_reason(state, terminal_state)
    set_workflow_status(
        pipeline_root,
        state,
        terminal_state,
        reason=state.final_gate.reason,
    )
    return state


def _terminal_state(state: WorkflowState) -> str:
    instance_states = [instance.state for instance in state.instances.values()]
    if any(item == INSTANCE_FAILED for item in instance_states):
        return WORKFLOW_FAILED
    if any(item == INSTANCE_BLOCKED for item in instance_states):
        return WORKFLOW_BLOCKED
    return WORKFLOW_SUCCESS


def _terminal_reason(state: WorkflowState, terminal_state: str) -> str:
    if terminal_state == WORKFLOW_SUCCESS:
        return "all stage instances completed successfully"
    failed = [
        instance.stage_instance_id
        for instance in state.instances.values()
        if instance.state == INSTANCE_FAILED
    ]
    if failed:
        return f"failed stage instances: {', '.join(sorted(failed))}"
    blocked = [
        instance.stage_instance_id
        for instance in state.instances.values()
        if instance.state == INSTANCE_BLOCKED
    ]
    return f"blocked stage instances: {', '.join(sorted(blocked))}"
