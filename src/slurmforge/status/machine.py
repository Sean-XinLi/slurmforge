"""Stage status state machine.

This module is the **only** writer of ``status.json`` and ``attempt.json``.
External callers must enter through ``commit_stage_status`` /
``commit_attempt`` (re-exported from ``slurmforge.status``); never call the
underscored helpers from outside this file.

Parent-status aggregation lives outside this module. The state machine writes
only per-stage records so it stays a leaf component.
"""

from __future__ import annotations

import json
from pathlib import Path

from ..io import to_jsonable, utc_now, write_json
from .models import (
    StageAttemptRecord,
    StageStatusRecord,
    TERMINAL_STATES,
)
from .reader import attempt_path, read_stage_status, status_path


def _status_events_path(run_dir: Path) -> Path:
    return Path(run_dir) / "status_events.jsonl"


# ---------------------------------------------------------------------------
# state machine internals (package-private; never call from outside status/)
# ---------------------------------------------------------------------------


_STATE_RANK = {"planned": 0, "queued": 1, "running": 2}


def _state_rank(state: str) -> int:
    if state in TERMINAL_STATES:
        return 3
    return _STATE_RANK.get(state, 0)


def _write_stage_status(run_dir: Path, status: StageStatusRecord) -> None:
    write_json(status_path(run_dir), status)


def _append_status_event(
    run_dir: Path,
    *,
    previous: StageStatusRecord | None,
    current: StageStatusRecord,
    source: str,
) -> None:
    path = _status_events_path(run_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "stage_instance_id": current.stage_instance_id,
        "run_id": current.run_id,
        "stage_name": current.stage_name,
        "from": None if previous is None else previous.state,
        "to": current.state,
        "reason": current.reason,
        "failure_class": current.failure_class,
        "source": source,
        "at": utc_now(),
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(to_jsonable(payload), sort_keys=True) + "\n")


def _transition_stage_status(
    run_dir: Path,
    status: StageStatusRecord,
    *,
    allow_new_attempt: bool,
    source: str,
) -> StageStatusRecord:
    current = read_stage_status(run_dir)
    if current is None:
        _write_stage_status(run_dir, status)
        _append_status_event(run_dir, previous=None, current=status, source=source)
        return status
    if current.state in TERMINAL_STATES and status.state not in TERMINAL_STATES:
        is_new_attempt = (
            allow_new_attempt
            and status.latest_attempt_id is not None
            and status.latest_attempt_id != current.latest_attempt_id
        )
        if not is_new_attempt:
            return current
    if current.state in TERMINAL_STATES and status.state in TERMINAL_STATES:
        is_new_attempt = (
            allow_new_attempt
            and status.latest_attempt_id is not None
            and status.latest_attempt_id != current.latest_attempt_id
        )
        if (
            not is_new_attempt
            and current.state == "success"
            and status.state != "success"
        ):
            return current
    if _state_rank(status.state) < _state_rank(current.state):
        return current
    merged = StageStatusRecord(
        schema_version=status.schema_version,
        stage_instance_id=status.stage_instance_id,
        run_id=status.run_id,
        stage_name=status.stage_name,
        state=status.state,
        latest_attempt_id=status.latest_attempt_id or current.latest_attempt_id,
        latest_output_digest=status.latest_output_digest
        or current.latest_output_digest,
        failure_class=status.failure_class,
        reason=status.reason or current.reason,
    )
    _write_stage_status(run_dir, merged)
    if (
        merged.state != current.state
        or merged.reason != current.reason
        or merged.failure_class != current.failure_class
    ):
        _append_status_event(run_dir, previous=current, current=merged, source=source)
    return merged


def _write_attempt(run_dir: Path, attempt: StageAttemptRecord) -> Path:
    path = attempt_path(run_dir, attempt.attempt_id)
    write_json(path, attempt)
    return path


# ---------------------------------------------------------------------------
# public commit API
# ---------------------------------------------------------------------------


def commit_stage_status(
    run_dir: Path,
    status: StageStatusRecord,
    *,
    allow_new_attempt: bool = False,
    source: str = "machine",
) -> StageStatusRecord:
    """Sole entry point for writing stage status.

    Drives only the per-stage state machine. Parent rollups are storage-owned.
    """
    committed = _transition_stage_status(
        run_dir, status, allow_new_attempt=allow_new_attempt, source=source
    )
    return committed


def commit_attempt(run_dir: Path, attempt: StageAttemptRecord) -> Path:
    return _write_attempt(run_dir, attempt)
