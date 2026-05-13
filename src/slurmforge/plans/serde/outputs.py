from __future__ import annotations

from typing import Any

from ...errors import RecordContractError
from ...io import SchemaVersion, require_schema
from ...record_fields import (
    required_bool,
    required_field,
    required_nullable_bool,
    required_nullable_int,
    required_nullable_string,
    required_object,
    required_string,
    required_string_tuple,
)
from ..outputs import ArtifactStorePlan, OutputRef, StageOutputsRecord


def output_ref_from_dict(payload: dict[str, Any]) -> OutputRef:
    require_schema(payload, name="output_ref", version=SchemaVersion.OUTPUT_RECORD)
    return OutputRef(
        output_name=required_string(
            payload, "output_name", label="output_ref", non_empty=True
        ),
        kind=required_string(payload, "kind", label="output_ref", non_empty=True),
        path=required_string(payload, "path", label="output_ref"),
        producer_stage_instance_id=required_string(
            payload, "producer_stage_instance_id", label="output_ref", non_empty=True
        ),
        cardinality=required_string(
            payload, "cardinality", label="output_ref", non_empty=True
        ),
        producer_attempt_id=required_string(
            payload, "producer_attempt_id", label="output_ref"
        ),
        digest=required_string(payload, "digest", label="output_ref"),
        source_path=required_string(payload, "source_path", label="output_ref"),
        managed=required_bool(payload, "managed", label="output_ref"),
        strategy=required_string(payload, "strategy", label="output_ref"),
        source_digest=required_string(payload, "source_digest", label="output_ref"),
        managed_digest=required_string(payload, "managed_digest", label="output_ref"),
        verified=required_nullable_bool(payload, "verified", label="output_ref"),
        size_bytes=required_nullable_int(payload, "size_bytes", label="output_ref"),
        selection_reason=required_string(
            payload, "selection_reason", label="output_ref"
        ),
        value=required_field(payload, "value", label="output_ref"),
    )


def stage_outputs_record_from_dict(payload: dict[str, Any]) -> StageOutputsRecord:
    require_schema(payload, name="stage_outputs", version=SchemaVersion.OUTPUT_RECORD)
    outputs = required_object(payload, "outputs", label="stage_outputs")
    return StageOutputsRecord(
        stage_instance_id=required_string(
            payload, "stage_instance_id", label="stage_outputs", non_empty=True
        ),
        producer_attempt_id=required_string(
            payload, "producer_attempt_id", label="stage_outputs", non_empty=True
        ),
        outputs={
            _output_key(name): _output_ref_for_key(name, item)
            for name, item in outputs.items()
        },
        artifacts=required_string_tuple(payload, "artifacts", label="stage_outputs"),
        artifact_manifest=required_string(
            payload, "artifact_manifest", label="stage_outputs"
        ),
    )


def _output_key(name: Any) -> str:
    if not isinstance(name, str) or not name:
        raise RecordContractError("stage_outputs.outputs keys must be non-empty strings")
    return name


def _output_ref_for_key(name: Any, payload: Any) -> OutputRef:
    output_name = _output_key(name)
    if not isinstance(payload, dict):
        raise RecordContractError(
            f"stage_outputs.outputs.{output_name} must be an object"
        )
    output = output_ref_from_dict(payload)
    if output.output_name != output_name:
        raise RecordContractError(
            f"stage_outputs output key `{output_name}` does not match `{output.output_name}`"
        )
    return output


def artifact_store_plan_from_dict(payload: dict[str, Any]) -> ArtifactStorePlan:
    return ArtifactStorePlan(
        strategy=required_string(
            payload, "strategy", label="artifact_store_plan", non_empty=True
        ),
        fallback_strategy=required_nullable_string(
            payload, "fallback_strategy", label="artifact_store_plan"
        ),
        verify_digest=required_bool(
            payload, "verify_digest", label="artifact_store_plan"
        ),
        fail_on_verify_error=required_bool(
            payload, "fail_on_verify_error", label="artifact_store_plan"
        ),
    )
