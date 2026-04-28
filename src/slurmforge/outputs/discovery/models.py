from __future__ import annotations

from dataclasses import dataclass

from ...plans import OutputRef, StageOutputsRecord
from ..models import ArtifactManifestRecord, ArtifactRef


@dataclass(frozen=True)
class OutputDiscoveryItem:
    output_name: str
    output_ref: OutputRef | None = None
    artifacts: tuple[ArtifactRef, ...] = ()
    missing_required_reason: str = ""


@dataclass(frozen=True)
class StageOutputDiscoveryResult:
    stage_outputs: StageOutputsRecord
    artifact_manifest: ArtifactManifestRecord
    artifact_paths: tuple[str, ...]
    failure_reason: str | None = None
