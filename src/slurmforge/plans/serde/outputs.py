from __future__ import annotations

from typing import Any

from ...io import SchemaVersion, require_schema
from ..outputs import ArtifactStorePlan, OutputRef, StageOutputsRecord


def output_ref_from_dict(payload: dict[str, Any]) -> OutputRef:
    require_schema(payload, name="output_ref", version=SchemaVersion.OUTPUT_RECORD)
    return OutputRef(
        output_name=str(payload["output_name"]),
        kind=str(payload["kind"]),
        path=str(payload["path"]),
        producer_stage_instance_id=str(payload["producer_stage_instance_id"]),
        cardinality=str(payload.get("cardinality") or "one"),
        producer_attempt_id=str(payload.get("producer_attempt_id") or ""),
        digest=str(payload.get("digest") or ""),
        source_path=str(payload.get("source_path") or ""),
        managed=bool(payload.get("managed", False)),
        strategy=str(payload.get("strategy") or ""),
        source_digest=str(payload.get("source_digest") or ""),
        managed_digest=str(payload.get("managed_digest") or ""),
        verified=None if payload.get("verified") is None else bool(payload.get("verified")),
        size_bytes=None if payload.get("size_bytes") is None else int(payload.get("size_bytes")),
        selection_reason=str(payload.get("selection_reason") or ""),
        value=payload.get("value"),
    )


def stage_outputs_record_from_dict(payload: dict[str, Any]) -> StageOutputsRecord:
    require_schema(payload, name="stage_outputs", version=SchemaVersion.OUTPUT_RECORD)
    return StageOutputsRecord(
        stage_instance_id=str(payload["stage_instance_id"]),
        producer_attempt_id=str(payload["producer_attempt_id"]),
        outputs={str(name): output_ref_from_dict(dict(item)) for name, item in dict(payload.get("outputs") or {}).items()},
        artifacts=tuple(str(item) for item in payload.get("artifacts", ())),
        artifact_manifest=str(payload["artifact_manifest"]),
    )


def artifact_store_plan_from_dict(payload: dict[str, Any]) -> ArtifactStorePlan:
    return ArtifactStorePlan(
        strategy=str(payload["strategy"]),
        fallback_strategy=None if payload["fallback_strategy"] in (None, "") else str(payload["fallback_strategy"]),
        verify_digest=bool(payload["verify_digest"]),
        fail_on_verify_error=bool(payload["fail_on_verify_error"]),
    )
