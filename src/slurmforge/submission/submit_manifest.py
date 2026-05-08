from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..io import SchemaVersion, read_json, require_schema
from ..record_fields import (
    required_int,
    required_object_array,
    required_string,
    required_string_array,
)


@dataclass(frozen=True)
class SubmitManifestGroup:
    group_id: str
    group_index: int
    sbatch_path: str
    array_size: int
    stage_instance_ids: tuple[str, ...]


@dataclass(frozen=True)
class SubmitManifestDependency:
    from_groups: tuple[str, ...]
    to_group: str
    type: str
    from_wave: str
    to_wave: str

    def to_record(self) -> dict[str, object]:
        return {
            "from_groups": list(self.from_groups),
            "to_group": self.to_group,
            "type": self.type,
            "from_wave": self.from_wave,
            "to_wave": self.to_wave,
        }


@dataclass(frozen=True)
class SubmitManifestNotification:
    event: str
    sbatch_path: str


@dataclass(frozen=True)
class SubmitManifest:
    batch_id: str
    stage_name: str
    generation_id: str
    generation_dir: str
    submit_script: str
    groups: tuple[SubmitManifestGroup, ...]
    dependencies: tuple[SubmitManifestDependency, ...]
    notifications: tuple[SubmitManifestNotification, ...]
    schema_version: int = SchemaVersion.SUBMIT_MANIFEST


def submit_manifest_path(batch_root: Path) -> Path:
    return Path(batch_root) / "submit" / "submit_manifest.json"


def load_submit_manifest(batch_root: Path) -> SubmitManifest:
    return submit_manifest_from_dict(read_json(submit_manifest_path(batch_root)))


def submit_manifest_from_dict(payload: dict[str, Any]) -> SubmitManifest:
    version = require_schema(
        payload, name="submit_manifest", version=SchemaVersion.SUBMIT_MANIFEST
    )
    return SubmitManifest(
        schema_version=version,
        batch_id=required_string(
            payload, "batch_id", label="submit_manifest", non_empty=True
        ),
        stage_name=required_string(
            payload, "stage_name", label="submit_manifest", non_empty=True
        ),
        generation_id=required_string(
            payload, "generation_id", label="submit_manifest", non_empty=True
        ),
        generation_dir=required_string(
            payload, "generation_dir", label="submit_manifest", non_empty=True
        ),
        submit_script=required_string(
            payload, "submit_script", label="submit_manifest", non_empty=True
        ),
        groups=tuple(
            _submit_manifest_group_from_dict(group)
            for group in required_object_array(
                payload, "groups", label="submit_manifest"
            )
        ),
        dependencies=tuple(
            _submit_manifest_dependency_from_dict(dependency)
            for dependency in required_object_array(
                payload, "dependencies", label="submit_manifest"
            )
        ),
        notifications=tuple(
            _submit_manifest_notification_from_dict(notification)
            for notification in required_object_array(
                payload, "notifications", label="submit_manifest"
            )
        ),
    )


def _submit_manifest_group_from_dict(
    payload: dict[str, Any],
) -> SubmitManifestGroup:
    label = "submit_manifest.groups"
    return SubmitManifestGroup(
        group_id=required_string(payload, "group_id", label=label, non_empty=True),
        group_index=required_int(payload, "group_index", label=label),
        sbatch_path=required_string(payload, "sbatch_path", label=label, non_empty=True),
        array_size=required_int(payload, "array_size", label=label),
        stage_instance_ids=required_string_array(
            payload, "stage_instance_ids", label=label
        ),
    )


def _submit_manifest_dependency_from_dict(
    payload: dict[str, Any],
) -> SubmitManifestDependency:
    label = "submit_manifest.dependencies"
    return SubmitManifestDependency(
        from_groups=required_string_array(payload, "from_groups", label=label),
        to_group=required_string(payload, "to_group", label=label, non_empty=True),
        type=required_string(payload, "type", label=label, non_empty=True),
        from_wave=required_string(payload, "from_wave", label=label),
        to_wave=required_string(payload, "to_wave", label=label),
    )


def _submit_manifest_notification_from_dict(
    payload: dict[str, Any],
) -> SubmitManifestNotification:
    label = "submit_manifest.notifications"
    return SubmitManifestNotification(
        event=required_string(payload, "event", label=label, non_empty=True),
        sbatch_path=required_string(payload, "sbatch_path", label=label, non_empty=True),
    )


def submit_manifest_dependency_records(
    manifest: SubmitManifest,
) -> tuple[dict[str, object], ...]:
    return tuple(dependency.to_record() for dependency in manifest.dependencies)


def submit_manifest_group_paths(manifest: SubmitManifest) -> dict[str, str]:
    return {group.group_id: group.sbatch_path for group in manifest.groups}
