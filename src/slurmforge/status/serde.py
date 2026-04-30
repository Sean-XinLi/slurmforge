from __future__ import annotations

from typing import Any

from ..io import SchemaVersion, require_schema
from ..record_fields import (
    required_bool,
    required_int,
    required_nullable_int,
    required_nullable_string,
    required_string,
    required_string_tuple,
)
from .models import StageAttemptRecord, StageStatusRecord


def stage_status_from_dict(payload: dict[str, Any]) -> StageStatusRecord:
    require_schema(payload, name="stage_status", version=SchemaVersion.STATUS)
    return StageStatusRecord(
        schema_version=required_int(payload, "schema_version", label="stage_status"),
        stage_instance_id=required_string(
            payload, "stage_instance_id", label="stage_status", non_empty=True
        ),
        run_id=required_string(payload, "run_id", label="stage_status", non_empty=True),
        stage_name=required_string(
            payload, "stage_name", label="stage_status", non_empty=True
        ),
        state=required_string(payload, "state", label="stage_status", non_empty=True),
        latest_attempt_id=required_nullable_string(
            payload, "latest_attempt_id", label="stage_status"
        ),
        latest_output_digest=required_nullable_string(
            payload, "latest_output_digest", label="stage_status"
        ),
        failure_class=required_nullable_string(
            payload, "failure_class", label="stage_status"
        ),
        reason=required_string(payload, "reason", label="stage_status"),
    )


def attempt_from_dict(payload: dict[str, Any]) -> StageAttemptRecord:
    require_schema(payload, name="stage_attempt", version=SchemaVersion.STATUS)
    return StageAttemptRecord(
        attempt_id=required_string(
            payload, "attempt_id", label="stage_attempt", non_empty=True
        ),
        stage_instance_id=required_string(
            payload, "stage_instance_id", label="stage_attempt", non_empty=True
        ),
        attempt_source=required_string(
            payload, "attempt_source", label="stage_attempt", non_empty=True
        ),
        attempt_state=required_string(
            payload, "attempt_state", label="stage_attempt", non_empty=True
        ),
        scheduler_job_id=required_string(
            payload, "scheduler_job_id", label="stage_attempt"
        ),
        scheduler_array_job_id=required_string(
            payload, "scheduler_array_job_id", label="stage_attempt"
        ),
        scheduler_array_task_id=required_string(
            payload, "scheduler_array_task_id", label="stage_attempt"
        ),
        scheduler_state=required_string(
            payload, "scheduler_state", label="stage_attempt"
        ),
        scheduler_exit_code=required_string(
            payload, "scheduler_exit_code", label="stage_attempt"
        ),
        node_list=required_string(payload, "node_list", label="stage_attempt"),
        started_by_executor=required_bool(
            payload, "started_by_executor", label="stage_attempt"
        ),
        executor_started_at=required_string(
            payload, "executor_started_at", label="stage_attempt"
        ),
        executor_finished_at=required_string(
            payload, "executor_finished_at", label="stage_attempt"
        ),
        started_at=required_string(payload, "started_at", label="stage_attempt"),
        finished_at=required_string(payload, "finished_at", label="stage_attempt"),
        exit_code=required_nullable_int(payload, "exit_code", label="stage_attempt"),
        failure_class=required_nullable_string(
            payload, "failure_class", label="stage_attempt"
        ),
        reason=required_string(payload, "reason", label="stage_attempt"),
        log_paths=required_string_tuple(payload, "log_paths", label="stage_attempt"),
        artifact_paths=required_string_tuple(
            payload, "artifact_paths", label="stage_attempt"
        ),
        artifact_manifest_path=required_string(
            payload, "artifact_manifest_path", label="stage_attempt"
        ),
        schema_version=required_int(payload, "schema_version", label="stage_attempt"),
    )
