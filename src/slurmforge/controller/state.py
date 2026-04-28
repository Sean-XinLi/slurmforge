from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..io import SchemaVersion, read_json, write_json
from ..plans.train_eval import TRAIN_EVAL_PIPELINE_KIND, TrainEvalPipelinePlan


def _controller_state_path(pipeline_root: Path) -> Path:
    return Path(pipeline_root) / "controller" / "controller_state.json"


def _controller_events_path(pipeline_root: Path) -> Path:
    return Path(pipeline_root) / "controller" / "events.jsonl"


def default_controller_state(plan: TrainEvalPipelinePlan) -> dict[str, Any]:
    return {
        "schema_version": SchemaVersion.CONTROLLER_STATE,
        "pipeline_id": plan.pipeline_id,
        "pipeline_kind": getattr(plan, "pipeline_kind", TRAIN_EVAL_PIPELINE_KIND),
        "state": "ready",
        "current_stage": plan.stage_order[0] if plan.stage_order else None,
        "completed_stages": [],
        "materialized_stages": [],
    }


def load_controller_state(pipeline_root: Path, plan: TrainEvalPipelinePlan) -> dict[str, Any]:
    path = _controller_state_path(pipeline_root)
    if path.exists():
        return read_json(path)
    state = default_controller_state(plan)
    write_json(path, state)
    return state


def save_controller_state(pipeline_root: Path, state: dict[str, Any]) -> None:
    write_json(_controller_state_path(pipeline_root), state)


def record_controller_event(pipeline_root: Path, event: str, **payload: Any) -> None:
    path = _controller_events_path(pipeline_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"event": event, **payload}, sort_keys=True) + "\n")
