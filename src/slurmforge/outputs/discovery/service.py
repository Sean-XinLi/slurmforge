from __future__ import annotations

from pathlib import Path

from ...io import write_json
from ...plans import StageInstancePlan, StageOutputsRecord
from ..models import ArtifactManifestRecord, ArtifactRef
from .context import OutputDiscoveryContext
from .models import StageOutputDiscoveryResult
from .registry import handler_for_kind


def discover_stage_outputs(
    instance: StageInstancePlan,
    workdir: Path,
    *,
    attempt_id: str,
    attempt_dir: Path,
) -> StageOutputDiscoveryResult:
    context = OutputDiscoveryContext(
        instance=instance,
        workdir=workdir,
        attempt_id=attempt_id,
        attempt_dir=attempt_dir,
        store_plan=instance.artifact_store_plan,
    )
    outputs = {}
    artifact_refs: list[ArtifactRef] = []
    missing_required: list[str] = []

    for output_name, output_cfg in sorted(instance.output_contract.outputs.items()):
        item = handler_for_kind(output_cfg.kind)(output_name, output_cfg, context)
        if item.output_ref is not None:
            outputs[output_name] = item.output_ref
        artifact_refs.extend(item.artifacts)
        if item.missing_required_reason:
            missing_required.append(item.missing_required_reason)

    artifact_manifest = ArtifactManifestRecord(
        stage_instance_id=instance.stage_instance_id,
        attempt_id=attempt_id,
        artifacts=tuple(artifact_refs),
    )
    manifest_path = attempt_dir / "artifacts" / "artifact_manifest.json"
    write_json(manifest_path, artifact_manifest)
    stage_outputs = StageOutputsRecord(
        stage_instance_id=instance.stage_instance_id,
        producer_attempt_id=attempt_id,
        outputs=outputs,
        artifacts=tuple(item.managed_path for item in artifact_refs),
        artifact_manifest=str(manifest_path.resolve()),
    )
    return StageOutputDiscoveryResult(
        stage_outputs=stage_outputs,
        artifact_manifest=artifact_manifest,
        artifact_paths=tuple(item.managed_path for item in artifact_refs),
        failure_reason="; ".join(missing_required) if missing_required else None,
    )
