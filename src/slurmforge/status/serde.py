from __future__ import annotations

from typing import Any

from ..io import SchemaVersion, require_schema
from .models import StageAttemptRecord, StageStatusRecord


def stage_status_from_dict(payload: dict[str, Any]) -> StageStatusRecord:
    require_schema(payload, name="stage_status", version=SchemaVersion.STATUS)
    return StageStatusRecord(
        schema_version=int(payload["schema_version"]),
        stage_instance_id=str(payload["stage_instance_id"]),
        run_id=str(payload["run_id"]),
        stage_name=str(payload["stage_name"]),
        state=str(payload.get("state") or "planned"),
        latest_attempt_id=None
        if payload.get("latest_attempt_id") in (None, "")
        else str(payload.get("latest_attempt_id")),
        latest_output_digest=None
        if payload.get("latest_output_digest") in (None, "")
        else str(payload.get("latest_output_digest")),
        failure_class=None
        if payload.get("failure_class") in (None, "")
        else str(payload.get("failure_class")),
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
        exit_code=None
        if payload.get("exit_code") is None
        else int(payload.get("exit_code")),
        failure_class=None
        if payload.get("failure_class") in (None, "")
        else str(payload.get("failure_class")),
        reason=str(payload.get("reason") or ""),
        log_paths=tuple(str(item) for item in payload.get("log_paths", ())),
        artifact_paths=tuple(str(item) for item in payload.get("artifact_paths", ())),
        artifact_manifest_path=str(payload.get("artifact_manifest_path") or ""),
        schema_version=int(payload["schema_version"]),
    )
