from __future__ import annotations

from pathlib import Path


def control_dir(pipeline_root: Path) -> Path:
    return Path(pipeline_root) / "control"


def workflow_status_path(pipeline_root: Path) -> Path:
    return control_dir(pipeline_root) / "workflow_status.json"


def workflow_state_path(pipeline_root: Path) -> Path:
    return control_dir(pipeline_root) / "workflow_state.json"


def workflow_events_path(pipeline_root: Path) -> Path:
    return control_dir(pipeline_root) / "events.jsonl"


def workflow_lock_path(pipeline_root: Path) -> Path:
    return control_dir(pipeline_root) / "workflow_state.lock"


def workflow_traceback_path(pipeline_root: Path) -> Path:
    return control_dir(pipeline_root) / "workflow_traceback.log"


def control_submissions_path(pipeline_root: Path) -> Path:
    return control_dir(pipeline_root) / "control_submissions.json"
