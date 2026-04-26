from __future__ import annotations

import glob
import json
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..io import SchemaVersion, file_digest, write_json
from ..plans import OutputRef, StageInstancePlan, StageOutputsRecord
from ..storage import stage_outputs_path
from .models import ArtifactManifestRecord, ArtifactRef, output_ref_from_artifact


class ArtifactIntegrityError(RuntimeError):
    pass


@dataclass(frozen=True)
class StageOutputDiscoveryResult:
    stage_outputs: StageOutputsRecord
    artifact_manifest: ArtifactManifestRecord
    artifact_paths: tuple[str, ...]
    failure_reason: str | None = None


def _glob_paths(workdir: Path, patterns: list[str]) -> list[str]:
    paths: set[str] = set()
    for pattern in patterns:
        if not pattern:
            continue
        expanded = pattern if Path(pattern).is_absolute() else str(workdir / pattern)
        for match in glob.glob(expanded, recursive=True):
            if Path(match).is_file():
                paths.add(str(Path(match).resolve()))
    return sorted(paths)


def _step_number(path: str) -> int | None:
    numbers = [int(item) for item in re.findall(r"(?<![A-Za-z])(\d+)(?![A-Za-z])", Path(path).stem)]
    return max(numbers) if numbers else None


def _select_file(paths: list[str], selector: str) -> tuple[str | None, str]:
    if not paths:
        return None, "no_match"
    if selector == "latest_step":
        with_steps = [(path, _step_number(path)) for path in paths]
        if any(step is not None for _path, step in with_steps):
            selected = max(with_steps, key=lambda item: (-1 if item[1] is None else item[1], item[0]))[0]
            return selected, "latest_step"
        return paths[-1], "lexicographic_last"
    if selector == "first":
        return paths[0], "first_match"
    return paths[-1], "last_match"


def _resolve_file(workdir: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else workdir / path


def _json_path_value(payload: object, path: str) -> object:
    if path == "$":
        return payload
    if not path.startswith("$."):
        raise ValueError(f"unsupported metric json_path: {path}")
    cursor = payload
    for part in path[2:].split("."):
        if not isinstance(cursor, dict) or part not in cursor:
            raise KeyError(f"json_path `{path}` did not resolve at `{part}`")
        cursor = cursor[part]
    return cursor


def _managed_name(path: Path, digest: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", path.name)
    return f"{digest[:12]}_{safe}"


def _store_file(source: Path, managed: Path, *, strategy: str) -> tuple[str, bool]:
    if strategy == "register_only":
        return str(source), False
    managed.parent.mkdir(parents=True, exist_ok=True)
    if managed.exists() or managed.is_symlink():
        managed.unlink()
    if strategy == "copy":
        shutil.copy2(source, managed)
    elif strategy == "hardlink":
        os.link(source, managed)
    elif strategy == "symlink":
        managed.symlink_to(source)
    else:
        raise ValueError(f"Unsupported artifact store strategy: {strategy}")
    return str(managed), True


def _manage_file(
    path: str,
    *,
    attempt_dir: Path,
    kind: str,
    output_name: str | None = None,
    optional: bool = False,
    store_plan: dict[str, Any],
) -> ArtifactRef:
    source = Path(path).resolve()
    source_digest = file_digest(source)
    strategy = str(store_plan.get("strategy") or "copy")
    fallback_strategy = store_plan.get("fallback_strategy")
    verify_digest = bool(store_plan.get("verify_digest", True))
    fail_on_verify_error = bool(store_plan.get("fail_on_verify_error", True))
    files_dir = attempt_dir / "artifacts" / "files"
    managed = files_dir / _managed_name(source, source_digest)
    try:
        managed_path, is_managed = _store_file(source, managed, strategy=strategy)
        strategy_applied = strategy
    except OSError:
        if not fallback_strategy:
            raise
        managed_path, is_managed = _store_file(source, managed, strategy=str(fallback_strategy))
        strategy_applied = str(fallback_strategy)
    managed_digest = source_digest
    verified = None
    verify_error = ""
    if verify_digest:
        try:
            managed_digest = file_digest(Path(managed_path))
            verified = managed_digest == source_digest
        except OSError as exc:
            verified = False
            verify_error = str(exc)
        if verified is False and fail_on_verify_error:
            detail = verify_error or f"source_digest={source_digest} managed_digest={managed_digest}"
            raise ArtifactIntegrityError(f"artifact digest verification failed for {managed_path}: {detail}")
    return ArtifactRef(
        name=output_name or source.name,
        kind=kind,
        source_path=str(source),
        managed_path=managed_path,
        strategy=strategy_applied,
        managed=is_managed,
        digest=source_digest,
        source_digest=source_digest,
        managed_digest=managed_digest,
        verified=verified,
        size_bytes=source.stat().st_size,
        optional=optional,
    )


def _artifact_payload(artifact: ArtifactRef) -> dict[str, Any]:
    return {
        "schema_version": artifact.schema_version,
        "name": artifact.name,
        "kind": artifact.kind,
        "source_path": artifact.source_path,
        "managed_path": artifact.managed_path,
        "strategy": artifact.strategy,
        "managed": artifact.managed,
        "digest": artifact.digest,
        "source_digest": artifact.source_digest,
        "managed_digest": artifact.managed_digest,
        "verified": artifact.verified,
        "size_bytes": artifact.size_bytes,
        "optional": artifact.optional,
    }


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
        "refs": [_artifact_payload(item) for item in artifacts],
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
    store_plan = dict(instance.artifact_store_plan or {})
    missing_required: list[str] = []
    for output_name, output_cfg in sorted(contract.outputs.items()):
        if output_cfg.kind == "file":
            paths = _glob_paths(workdir, list(output_cfg.discover.globs))
            selected, reason = _select_file(paths, output_cfg.discover.select)
            if selected:
                artifact = _manage_file(
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
            paths = _glob_paths(workdir, list(output_cfg.discover.globs))
            if not paths and output_cfg.required:
                missing_required.append(f"required output `{output_name}` was not produced")
                continue
            artifacts = tuple(
                _manage_file(
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
            metric_file = _resolve_file(workdir, output_cfg.file).resolve()
            if not metric_file.exists() or not metric_file.is_file():
                if output_cfg.required:
                    missing_required.append(f"required output `{output_name}` was not produced")
                continue
            try:
                with metric_file.open("r", encoding="utf-8") as handle:
                    value = _json_path_value(json.load(handle), output_cfg.json_path)
            except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
                if output_cfg.required:
                    missing_required.append(f"required output `{output_name}` did not resolve: {exc}")
                continue
            artifact = _manage_file(
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
        manifest_file = _resolve_file(workdir, output_cfg.file).resolve()
        if not manifest_file.exists() or not manifest_file.is_file():
            if output_cfg.required:
                missing_required.append(f"required output `{output_name}` was not produced")
            continue
        artifact = _manage_file(
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
    write_json(stage_outputs_path(run_dir), record)
    write_json(attempt_dir / "outputs" / "stage_outputs.json", record)
