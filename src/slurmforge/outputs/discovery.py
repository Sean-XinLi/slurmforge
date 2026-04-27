from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ..io import SchemaVersion, file_digest, write_json
from ..plans import OutputRef, StageInstancePlan, StageOutputsRecord
from .artifact_store import artifact_payload, manage_file
from .models import ArtifactManifestRecord, ArtifactRef, output_ref_from_artifact
from .selection import glob_paths, json_path_value, resolve_file, select_file


@dataclass(frozen=True)
class StageOutputDiscoveryResult:
    stage_outputs: StageOutputsRecord
    artifact_manifest: ArtifactManifestRecord
    artifact_paths: tuple[str, ...]
    failure_reason: str | None = None


def _stage_outputs_path(run_dir: Path) -> Path:
    return run_dir / "stage_outputs.json"


def _write_files_output_ref(
    output_name: str,
    artifacts: tuple[ArtifactRef, ...],
    *,
    instance: StageInstancePlan,
    attempt_id: str,
    attempt_dir: Path,
) -> OutputRef:
    payload = {
        "schema_version": SchemaVersion.OUTPUT_RECORD,
        "output_name": output_name,
        "kind": "files",
        "cardinality": "many",
        "producer_stage_instance_id": instance.stage_instance_id,
        "producer_attempt_id": attempt_id,
        "refs": [artifact_payload(item) for item in artifacts],
    }
    manifest_path = attempt_dir / "artifacts" / "output_manifests" / f"{output_name}.json"
    write_json(manifest_path, payload)
    digest = file_digest(manifest_path)
    return OutputRef(
        output_name=output_name,
        kind="files",
        path=str(manifest_path.resolve()),
        producer_stage_instance_id=instance.stage_instance_id,
        cardinality="many",
        producer_attempt_id=attempt_id,
        digest=digest,
        managed=True,
        strategy="manifest",
        managed_digest=digest,
        verified=True,
        size_bytes=manifest_path.stat().st_size,
        selection_reason="all_matches_manifest",
        value=payload["refs"],
    )


def discover_stage_outputs(
    instance: StageInstancePlan,
    workdir: Path,
    *,
    attempt_id: str,
    attempt_dir: Path,
) -> StageOutputDiscoveryResult:
    contract = instance.output_contract
    outputs = {}
    artifact_refs: list[ArtifactRef] = []
    store_plan = instance.artifact_store_plan
    missing_required: list[str] = []
    for output_name, output_cfg in sorted(contract.outputs.items()):
        if output_cfg.kind == "file":
            paths = glob_paths(workdir, list(output_cfg.discover.globs))
            selected, reason = select_file(paths, output_cfg.discover.select)
            if selected:
                artifact = manage_file(
                    selected,
                    attempt_dir=attempt_dir,
                    kind="file",
                    output_name=output_name,
                    optional=not output_cfg.required,
                    store_plan=store_plan,
                )
                artifact_refs.append(artifact)
                outputs[output_name] = output_ref_from_artifact(
                    artifact,
                    output_name=output_name,
                    producer_stage_instance_id=instance.stage_instance_id,
                    producer_attempt_id=attempt_id,
                    selection_reason=reason,
                )
            elif output_cfg.required:
                missing_required.append(f"required output `{output_name}` was not produced")
            continue
        if output_cfg.kind == "files":
            paths = glob_paths(workdir, list(output_cfg.discover.globs))
            if not paths and output_cfg.required:
                missing_required.append(f"required output `{output_name}` was not produced")
                continue
            artifacts = tuple(
                manage_file(
                    path,
                    attempt_dir=attempt_dir,
                    kind="files",
                    output_name=output_name,
                    optional=not output_cfg.required,
                    store_plan=store_plan,
                )
                for path in paths
            )
            artifact_refs.extend(artifacts)
            if artifacts:
                outputs[output_name] = _write_files_output_ref(
                    output_name,
                    artifacts,
                    instance=instance,
                    attempt_id=attempt_id,
                    attempt_dir=attempt_dir,
                )
            continue
        if output_cfg.kind == "metric":
            metric_file = resolve_file(workdir, output_cfg.file).resolve()
            if not metric_file.exists() or not metric_file.is_file():
                if output_cfg.required:
                    missing_required.append(f"required output `{output_name}` was not produced")
                continue
            try:
                with metric_file.open("r", encoding="utf-8") as handle:
                    value = json_path_value(json.load(handle), output_cfg.json_path)
            except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
                if output_cfg.required:
                    missing_required.append(f"required output `{output_name}` did not resolve: {exc}")
                continue
            artifact = manage_file(
                str(metric_file),
                attempt_dir=attempt_dir,
                kind="metric",
                output_name=output_name,
                optional=not output_cfg.required,
                store_plan=store_plan,
            )
            artifact_refs.append(artifact)
            outputs[output_name] = OutputRef(
                output_name=output_name,
                kind="metric",
                path=artifact.managed_path,
                producer_stage_instance_id=instance.stage_instance_id,
                producer_attempt_id=attempt_id,
                digest=artifact.digest,
                source_path=artifact.source_path,
                managed=artifact.managed,
                strategy=artifact.strategy,
                source_digest=artifact.source_digest,
                managed_digest=artifact.managed_digest,
                verified=artifact.verified,
                size_bytes=artifact.size_bytes,
                value=value,
                selection_reason=f"json_path:{output_cfg.json_path}",
            )
            continue
        manifest_file = resolve_file(workdir, output_cfg.file).resolve()
        if not manifest_file.exists() or not manifest_file.is_file():
            if output_cfg.required:
                missing_required.append(f"required output `{output_name}` was not produced")
            continue
        artifact = manage_file(
            str(manifest_file),
            attempt_dir=attempt_dir,
            kind="manifest",
            output_name=output_name,
            optional=not output_cfg.required,
            store_plan=store_plan,
        )
        artifact_refs.append(artifact)
        outputs[output_name] = output_ref_from_artifact(
            artifact,
            output_name=output_name,
            producer_stage_instance_id=instance.stage_instance_id,
            producer_attempt_id=attempt_id,
            selection_reason="manifest_file",
        )
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


def write_stage_outputs_record(record: StageOutputsRecord, *, run_dir: Path, attempt_dir: Path) -> None:
    write_json(_stage_outputs_path(run_dir), record)
    write_json(attempt_dir / "outputs" / "stage_outputs.json", record)
