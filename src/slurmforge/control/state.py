from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..control_paths import workflow_events_path, workflow_state_path
from ..io import read_json, utc_now, write_json
from ..plans.train_eval import TrainEvalPipelinePlan
from ..storage.workflow import default_workflow_state


def load_workflow_state(pipeline_root: Path, plan: TrainEvalPipelinePlan) -> dict[str, Any]:
    path = workflow_state_path(pipeline_root)
    if path.exists():
        return read_json(path)
    state = default_workflow_state(plan)
    write_json(path, state)
    return state


def save_workflow_state(pipeline_root: Path, state: dict[str, Any]) -> None:
    write_json(workflow_state_path(pipeline_root), state)


def record_workflow_event(pipeline_root: Path, event: str, **payload: Any) -> None:
    path = workflow_events_path(pipeline_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps({"event": event, "at": utc_now(), **payload}, sort_keys=True)
            + "\n"
        )
