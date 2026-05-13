from __future__ import annotations

from typing import Any

from ..contracts import input_source_from_dict
from ..io import SchemaVersion, require_schema
from ..record_fields import (
    required_bool,
    required_nullable_bool,
    required_nullable_int,
    required_nullable_string,
    required_object,
    required_object_array,
    required_string,
)
from .models import InputVerificationRecord, StageInputVerificationReport


def input_verification_record_from_dict(
    payload: dict[str, Any],
) -> InputVerificationRecord:
    label = "input_verification_record"
    return InputVerificationRecord(
        input_name=required_string(payload, "input_name", label=label, non_empty=True),
        source=input_source_from_dict(required_object(payload, "source", label=label)),
        expects=required_string(payload, "expects", label=label, non_empty=True),
        required=required_bool(payload, "required", label=label),
        resolved_kind=required_string(payload, "resolved_kind", label=label),
        resolved_path=required_nullable_string(payload, "resolved_path", label=label),
        path_kind=required_string(payload, "path_kind", label=label, non_empty=True),
        exists=required_nullable_bool(payload, "exists", label=label),
        readable=required_nullable_bool(payload, "readable", label=label),
        size_bytes=required_nullable_int(payload, "size_bytes", label=label),
        digest=required_string(payload, "digest", label=label),
        value_digest=required_string(payload, "value_digest", label=label),
        expected_digest=required_string(payload, "expected_digest", label=label),
        producer_digest=required_string(payload, "producer_digest", label=label),
        producer_stage_instance_id=required_string(
            payload, "producer_stage_instance_id", label=label
        ),
        verified_at=required_string(payload, "verified_at", label=label),
        phase=required_string(payload, "phase", label=label, non_empty=True),
        state=required_string(payload, "state", label=label, non_empty=True),
        failure_class=required_nullable_string(payload, "failure_class", label=label),
        reason=required_string(payload, "reason", label=label),
    )


def stage_input_verification_report_from_dict(
    payload: dict[str, Any],
) -> StageInputVerificationReport:
    version = require_schema(
        payload,
        name="input_verification_report",
        version=SchemaVersion.INPUT_VERIFICATION,
    )
    return StageInputVerificationReport(
        schema_version=version,
        stage_instance_id=required_string(
            payload, "stage_instance_id", label="input_verification_report"
        ),
        run_id=required_string(payload, "run_id", label="input_verification_report"),
        stage_name=required_string(
            payload, "stage_name", label="input_verification_report"
        ),
        phase=required_string(
            payload, "phase", label="input_verification_report", non_empty=True
        ),
        state=required_string(
            payload, "state", label="input_verification_report", non_empty=True
        ),
        records=tuple(
            input_verification_record_from_dict(item)
            for item in required_object_array(
                payload, "records", label="input_verification_report"
            )
        ),
    )
