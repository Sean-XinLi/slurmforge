from __future__ import annotations

from pathlib import Path

from ..io import SchemaVersion, write_json
from ..plans import TrainEvalPipelinePlan


def write_initial_controller_state(root: Path, plan: TrainEvalPipelinePlan) -> None:
    write_json(root / "controller" / "controller_plan.json", plan.controller_plan)
    write_json(
        root / "controller" / "controller_state.json",
        {
            "schema_version": SchemaVersion.CONTROLLER_STATE,
            "pipeline_id": plan.pipeline_id,
            "pipeline_kind": plan.pipeline_kind,
            "state": "planned",
            "current_stage": plan.stage_order[0] if plan.stage_order else None,
            "completed_stages": [],
            "materialized_stages": [],
        },
    )
    write_json(
        root / "controller" / "controller_status.json",
        {"schema_version": SchemaVersion.CONTROLLER_STATUS, "state": "planned"},
    )
    (root / "controller" / "events.jsonl").touch()
