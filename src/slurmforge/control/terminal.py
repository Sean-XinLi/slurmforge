from __future__ import annotations

from pathlib import Path

from ..root_model.snapshots import refresh_train_eval_pipeline_status
from ..storage.workflow import write_workflow_status
from .gate_ledger import submitted_gate_records
from .state import save_workflow_state
from .state_model import submitted_stage_job_ids
from .state_records import WorkflowState, workflow_state_to_dict


def complete_pipeline(pipeline_root: Path, state: WorkflowState) -> str:
    final_state = _pipeline_terminal_state(pipeline_root)
    state.state = final_state
    state.current_stage = None
    save_workflow_state(pipeline_root, state)
    state_payload = workflow_state_to_dict(state)
    write_workflow_status(
        pipeline_root,
        final_state,
        gate_jobs=submitted_gate_records(pipeline_root),
        stage_jobs=submitted_stage_job_ids(pipeline_root),
        train_groups=state_payload["train_groups"],
        final_gate=state_payload["final_gate"],
    )
    return final_state


def _pipeline_terminal_state(pipeline_root: Path) -> str:
    snapshot = refresh_train_eval_pipeline_status(pipeline_root)
    if snapshot.pipeline_status is None:
        return "missing"
    return snapshot.pipeline_status.state
