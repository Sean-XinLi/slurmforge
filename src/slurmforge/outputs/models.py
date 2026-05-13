from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..io import SchemaVersion, require_schema
from ..plans.outputs import OutputRef
from ..record_fields import (
    required_bool,
    required_int,
    required_nullable_bool,
    required_object_array,
    required_string,
)


def _require_output_schema(payload: dict[str, Any], *, name: str) -> None:
    require_schema(payload, name=name, version=SchemaVersion.OUTPUT_RECORD)


@dataclass(frozen=True)
class ArtifactRef:
    name: str
    kind: str
    source_path: str
    managed_path: str
    strategy: str
    managed: bool
    digest: str
    source_digest: str
    managed_digest: str
    verified: bool | None
    size_bytes: int
    optional: bool = False
    schema_version: int = SchemaVersion.OUTPUT_RECORD


@dataclass(frozen=True)
class ArtifactManifestRecord:
    stage_instance_id: str
    attempt_id: str
    artifacts: tuple[ArtifactRef, ...]
    schema_version: int = SchemaVersion.OUTPUT_RECORD


def output_ref_from_artifact(
    artifact: ArtifactRef,
    *,
    output_name: str,
    producer_stage_instance_id: str,
    producer_attempt_id: str,
    selection_reason: str,
) -> OutputRef:
    return OutputRef(
        output_name=output_name,
        kind=artifact.kind,
        path=artifact.managed_path,
        producer_stage_instance_id=producer_stage_instance_id,
        cardinality="one",
        producer_attempt_id=producer_attempt_id,
        digest=artifact.digest,
        source_path=artifact.source_path,
        managed=artifact.managed,
        strategy=artifact.strategy,
        source_digest=artifact.source_digest,
        managed_digest=artifact.managed_digest,
        verified=artifact.verified,
        size_bytes=artifact.size_bytes,
        selection_reason=selection_reason,
    )


def artifact_ref_from_dict(payload: dict[str, Any]) -> ArtifactRef:
    _require_output_schema(payload, name="artifact_ref")
    return ArtifactRef(
        name=required_string(payload, "name", label="artifact_ref", non_empty=True),
        kind=required_string(payload, "kind", label="artifact_ref", non_empty=True),
        source_path=required_string(
            payload, "source_path", label="artifact_ref", non_empty=True
        ),
        managed_path=required_string(
            payload, "managed_path", label="artifact_ref", non_empty=True
        ),
        strategy=required_string(
            payload, "strategy", label="artifact_ref", non_empty=True
        ),
        managed=required_bool(payload, "managed", label="artifact_ref"),
        digest=required_string(payload, "digest", label="artifact_ref", non_empty=True),
        source_digest=required_string(
            payload, "source_digest", label="artifact_ref", non_empty=True
        ),
        managed_digest=required_string(
            payload, "managed_digest", label="artifact_ref", non_empty=True
        ),
        verified=required_nullable_bool(payload, "verified", label="artifact_ref"),
        size_bytes=required_int(payload, "size_bytes", label="artifact_ref"),
        optional=required_bool(payload, "optional", label="artifact_ref"),
    )


def artifact_manifest_record_from_dict(
    payload: dict[str, Any],
) -> ArtifactManifestRecord:
    _require_output_schema(payload, name="artifact_manifest")
    return ArtifactManifestRecord(
        stage_instance_id=required_string(
            payload, "stage_instance_id", label="artifact_manifest", non_empty=True
        ),
        attempt_id=required_string(
            payload, "attempt_id", label="artifact_manifest", non_empty=True
        ),
        artifacts=tuple(
            artifact_ref_from_dict(item)
            for item in required_object_array(
                payload, "artifacts", label="artifact_manifest"
            )
        ),
    )
