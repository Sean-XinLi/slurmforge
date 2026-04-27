from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..io import SchemaVersion


@dataclass(frozen=True)
class ArtifactStorePlan:
    strategy: str = "copy"
    fallback_strategy: str | None = None
    verify_digest: bool = True
    fail_on_verify_error: bool = True


@dataclass(frozen=True)
class OutputRef:
    output_name: str
    kind: str
    path: str
    producer_stage_instance_id: str
    cardinality: str = "one"
    producer_attempt_id: str = ""
    digest: str = ""
    source_path: str = ""
    managed: bool = False
    strategy: str = ""
    source_digest: str = ""
    managed_digest: str = ""
    verified: bool | None = None
    size_bytes: int | None = None
    selection_reason: str = ""
    value: Any = None
    schema_version: int = SchemaVersion.PLAN


@dataclass(frozen=True)
class StageOutputsRecord:
    stage_instance_id: str
    producer_attempt_id: str
    outputs: dict[str, OutputRef]
    artifacts: tuple[str, ...]
    artifact_manifest: str
    schema_version: int = SchemaVersion.OUTPUT_RECORD
