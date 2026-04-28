from __future__ import annotations

from pathlib import Path
from typing import Any

from ..notifications import deliver_notification
from ..root_model import (
    load_notification_summary_input,
    refresh_train_eval_pipeline_status,
)
from ..storage.controller import write_controller_status
from .state import record_controller_event, save_controller_state


def complete_pipeline(
    pipeline_root: Path, state: dict[str, Any], *, notification_plan
) -> str:
    final_state = _pipeline_terminal_state(pipeline_root)
    state["state"] = final_state
    state["current_stage"] = None
    save_controller_state(pipeline_root, state)
    write_controller_status(pipeline_root, final_state)
    record = deliver_notification(
        pipeline_root,
        event="train_eval_pipeline_finished",
        notification_plan=notification_plan,
        summary_input=load_notification_summary_input(
            pipeline_root, event="train_eval_pipeline_finished"
        ),
    )
    if record is not None:
        record_controller_event(
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
