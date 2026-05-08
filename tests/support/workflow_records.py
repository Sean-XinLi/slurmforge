from __future__ import annotations

import json
from pathlib import Path

from slurmforge.storage.workflow import read_workflow_status
from slurmforge.storage.workflow_state_models import WorkflowState
from slurmforge.storage.workflow_state_serde import workflow_state_from_dict
from slurmforge.storage.workflow_status_records import WorkflowStatusRecord


def read_workflow_state_payload(pipeline_root: Path) -> dict:
    return json.loads(
        (Path(pipeline_root) / "control" / "workflow_state.json").read_text(
            encoding="utf-8"
        )
    )


def read_workflow_state_record(pipeline_root: Path) -> WorkflowState:
    return workflow_state_from_dict(read_workflow_state_payload(pipeline_root))


def read_workflow_status_record(pipeline_root: Path) -> WorkflowStatusRecord:
    record = read_workflow_status(Path(pipeline_root))
    assert record is not None
    return record
