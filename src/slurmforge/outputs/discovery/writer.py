from __future__ import annotations

from pathlib import Path

from ...io import SchemaVersion, file_digest, write_json
from ...plans import OutputRef, StageOutputsRecord
from ..artifact_store import artifact_payload
from ..models import ArtifactRef
from .context import OutputDiscoveryContext


def _stage_outputs_path(run_dir: Path) -> Path:
    return run_dir / "stage_outputs.json"


def write_files_output_manifest(
    output_name: str,
    artifacts: tuple[ArtifactRef, ...],
    context: OutputDiscoveryContext,
) -> OutputRef:
    payload = {
        "schema_version": SchemaVersion.OUTPUT_RECORD,
        "output_name": output_name,
        "kind": "files",
        "cardinality": "many",
        "producer_stage_instance_id": context.instance.stage_instance_id,
        "producer_attempt_id": context.attempt_id,
        "refs": [artifact_payload(item) for item in artifacts],
    }
    manifest_path = context.attempt_dir / "artifacts" / "output_manifests" / f"{output_name}.json"
    write_json(manifest_path, payload)
    digest = file_digest(manifest_path)
    return OutputRef(
        output_name=output_name,
        kind="files",
        path=str(manifest_path.resolve()),
        producer_stage_instance_id=context.instance.stage_instance_id,
        cardinality="many",
        producer_attempt_id=context.attempt_id,
        digest=digest,
        managed=True,
        strategy="manifest",
        managed_digest=digest,
        verified=True,
        size_bytes=manifest_path.stat().st_size,
        selection_reason="all_matches_manifest",
        value=payload["refs"],
    )


def write_stage_outputs_record(record: StageOutputsRecord, *, run_dir: Path, attempt_dir: Path) -> None:
    write_json(_stage_outputs_path(run_dir), record)
    write_json(attempt_dir / "outputs" / "stage_outputs.json", record)
