from __future__ import annotations

from pathlib import Path
from typing import Any

from ..notifications.delivery import deliver_notification
from ..root_model.notifications import load_notification_summary_input
from ..root_model.snapshots import refresh_train_eval_pipeline_status
from ..storage.workflow import write_workflow_status
from .gate_ledger import submitted_gate_records
from .state import record_workflow_event, save_workflow_state
from .state_model import submitted_stage_job_ids


def complete_pipeline(
    pipeline_root: Path, state: dict[str, Any], *, notification_plan
) -> str:
    final_state = _pipeline_terminal_state(pipeline_root)
    state["state"] = final_state
    state["current_stage"] = None
    save_workflow_state(pipeline_root, state)
    write_workflow_status(
        pipeline_root,
        final_state,
        gate_jobs=submitted_gate_records(pipeline_root),
        stage_jobs=submitted_stage_job_ids(pipeline_root),
        train_groups=state.get("train_groups") or {},
        final_gate=state.get("final_gate") or {},
    )
    record = deliver_notification(
        pipeline_root,
        event="train_eval_pipeline_finished",
        notification_plan=notification_plan,
        summary_input=load_notification_summary_input(
            pipeline_root, event="train_eval_pipeline_finished"
        ),
    )
    if record is not None:
        record_workflow_event(
            pipeline_root,
            "pipeline_notification",
            notification_event="train_eval_pipeline_finished",
            state=record.state,
            reason=record.reason,
        )
    return final_state


def _pipeline_terminal_state(pipeline_root: Path) -> str:
    snapshot = refresh_train_eval_pipeline_status(pipeline_root)
    if snapshot.pipeline_status is None:
        return "missing"
    return snapshot.pipeline_status.state
