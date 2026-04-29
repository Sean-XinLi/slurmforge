from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..io import SchemaVersion, require_schema
from ..plans.outputs import OutputRef


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
        name=str(payload["name"]),
        kind=str(payload["kind"]),
        source_path=str(payload["source_path"]),
        managed_path=str(payload["managed_path"]),
        strategy=str(payload["strategy"]),
        managed=bool(payload["managed"]),
        digest=str(payload["digest"]),
        source_digest=str(payload.get("source_digest") or payload["digest"]),
        managed_digest=str(payload.get("managed_digest") or payload["digest"]),
        verified=None
        if payload.get("verified") is None
        else bool(payload.get("verified")),
        size_bytes=int(payload["size_bytes"]),
        optional=bool(payload.get("optional", False)),
    )


def artifact_manifest_record_from_dict(
    payload: dict[str, Any],
) -> ArtifactManifestRecord:
    _require_output_schema(payload, name="artifact_manifest")
    return ArtifactManifestRecord(
        stage_instance_id=str(payload["stage_instance_id"]),
        attempt_id=str(payload["attempt_id"]),
        artifacts=tuple(
            artifact_ref_from_dict(dict(item)) for item in payload.get("artifacts", ())
        ),
    )
