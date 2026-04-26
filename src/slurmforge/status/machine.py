"""Stage status state machine.

This module is the **only** writer of ``status.json`` and ``attempt.json``.
External callers must enter through ``commit_stage_status`` /
``commit_attempt`` (re-exported from ``slurmforge.status``); never call the
underscored helpers from outside this file.

Parent-status aggregation lives outside this module. The state machine writes
only per-stage records so it stays a leaf component.
"""
from __future__ import annotations

import contextlib
import json
from pathlib import Path
from typing import Any, Iterator

from ..io import SchemaVersion, read_json, require_schema, to_jsonable, utc_now, write_json
from .models import (
    StageAttemptRecord,
    StageStatusRecord,
    TERMINAL_STATES,
)


def _status_path(run_dir: Path) -> Path:
    return Path(run_dir) / "status.json"


def _status_events_path(run_dir: Path) -> Path:
    return Path(run_dir) / "status_events.jsonl"


def _attempts_dir(run_dir: Path) -> Path:
    return Path(run_dir) / "attempts"


def _attempt_path(run_dir: Path, attempt_id: str) -> Path:
    return _attempts_dir(run_dir) / attempt_id / "attempt.json"


# ---------------------------------------------------------------------------
# from-dict
# ---------------------------------------------------------------------------


def stage_status_from_dict(payload: dict[str, Any]) -> StageStatusRecord:
    require_schema(payload, name="stage_status", version=SchemaVersion.STATUS)
    return StageStatusRecord(
        schema_version=int(payload["schema_version"]),
        stage_instance_id=str(payload["stage_instance_id"]),
        run_id=str(payload["run_id"]),
        stage_name=str(payload["stage_name"]),
        state=str(payload.get("state") or "planned"),
        latest_attempt_id=None if payload.get("latest_attempt_id") in (None, "") else str(payload.get("latest_attempt_id")),
        latest_output_digest=None
        if payload.get("latest_output_digest") in (None, "")
        else str(payload.get("latest_output_digest")),
        failure_class=None if payload.get("failure_class") in (None, "") else str(payload.get("failure_class")),
        reason=str(payload.get("reason") or ""),
    )


def attempt_from_dict(payload: dict[str, Any]) -> StageAttemptRecord:
    require_schema(payload, name="stage_attempt", version=SchemaVersion.STATUS)
    return StageAttemptRecord(
        attempt_id=str(payload["attempt_id"]),
        stage_instance_id=str(payload["stage_instance_id"]),
        attempt_source=str(payload.get("attempt_source") or "executor"),
        attempt_state=str(payload.get("attempt_state") or "starting"),
        scheduler_job_id=str(payload.get("scheduler_job_id") or ""),
        scheduler_array_job_id=str(payload.get("scheduler_array_job_id") or ""),
        scheduler_array_task_id=str(payload.get("scheduler_array_task_id") or ""),
        scheduler_state=str(payload.get("scheduler_state") or ""),
        scheduler_exit_code=str(payload.get("scheduler_exit_code") or ""),
        node_list=str(payload.get("node_list") or ""),
        started_by_executor=bool(payload.get("started_by_executor", True)),
        executor_started_at=str(payload.get("executor_started_at") or ""),
        executor_finished_at=str(payload.get("executor_finished_at") or ""),
        started_at=str(payload.get("started_at") or ""),
        finished_at=str(payload.get("finished_at") or ""),
        exit_code=None if payload.get("exit_code") is None else int(payload.get("exit_code")),
        failure_class=None if payload.get("failure_class") in (None, "") else str(payload.get("failure_class")),
        reason=str(payload.get("reason") or ""),
        log_paths=tuple(str(item) for item in payload.get("log_paths", ())),
        artifact_paths=tuple(str(item) for item in payload.get("artifact_paths", ())),
        artifact_manifest_path=str(payload.get("artifact_manifest_path") or ""),
        schema_version=int(payload["schema_version"]),
    )


# ---------------------------------------------------------------------------
# read
# ---------------------------------------------------------------------------


def read_stage_status(run_dir: Path) -> StageStatusRecord | None:
    path = _status_path(run_dir)
    if not path.exists():
        return None
    return stage_status_from_dict(read_json(path))


def list_attempts(run_dir: Path) -> list[StageAttemptRecord]:
    root = _attempts_dir(run_dir)
    if not root.exists():
        return []
    return [attempt_from_dict(read_json(path)) for path in sorted(root.glob("*/attempt.json"))]


# ---------------------------------------------------------------------------
# state machine internals (package-private; never call from outside status/)
# ---------------------------------------------------------------------------


_STATE_RANK = {"planned": 0, "queued": 1, "running": 2}


def _state_rank(state: str) -> int:
    if state in TERMINAL_STATES:
        return 3
    return _STATE_RANK.get(state, 0)


def _write_stage_status(run_dir: Path, status: StageStatusRecord) -> None:
    write_json(_status_path(run_dir), status)


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
        if not is_new_attempt and current.state == "success" and status.state != "success":
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
        latest_output_digest=status.latest_output_digest or current.latest_output_digest,
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
    path = _attempt_path(run_dir, attempt.attempt_id)
    write_json(path, attempt)
    return path


# ---------------------------------------------------------------------------
# compatibility bulk-write context
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def batched_commits() -> Iterator[None]:
    """Group stage writes without giving status ownership of parent rollups."""
    yield


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


# ---------------------------------------------------------------------------
# query helpers (read-only, used by aggregation and CLI)
# ---------------------------------------------------------------------------


def state_matches(status: StageStatusRecord | None, query: str) -> bool:
    if query in {"", "all", "*"}:
        return True
    if status is None:
        return query in {"missing", "state=missing"}
    if "=" in query:
        key, value = query.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key == "state":
            return status.state == value
        if key == "failure_class":
            return status.failure_class == value
        if key == "stage":
            return status.stage_name == value
        if key == "run_id":
            return status.run_id == value
        return False
    if query == "failed":
        return status.state == "failed"
    if query == "non_success":
        return status.state != "success"
    return status.state == query or status.failure_class == query
