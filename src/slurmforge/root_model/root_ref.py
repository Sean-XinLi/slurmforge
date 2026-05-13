"""Stage run to stage_batch / train-eval pipeline root indirection."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..errors import RecordContractError
from ..io import SchemaVersion, read_json_object, require_schema, write_json_object
from ..record_fields import required_nullable_string, required_string
from ..storage.paths import root_ref_path


@dataclass(frozen=True)
class StageRootRef:
    stage_batch_root: str
    pipeline_root: str | None = None
    schema_version: int = SchemaVersion.ROOT_REF


def write_root_ref(
    run_dir: Path,
    *,
    stage_batch_root: Path,
    pipeline_root: Path | None = None,
) -> StageRootRef:
    ref = StageRootRef(
        stage_batch_root=str(Path(stage_batch_root).resolve()),
        pipeline_root=None
        if pipeline_root is None
        else str(Path(pipeline_root).resolve()),
    )
    write_json_object(root_ref_path(run_dir), ref)
    return ref


def read_root_ref(run_dir: Path) -> StageRootRef | None:
    path = root_ref_path(run_dir)
    if not path.exists():
        return None
    payload = read_json_object(path)
    version = require_schema(payload, name="root_ref", version=SchemaVersion.ROOT_REF)
    pipeline_root = required_nullable_string(
        payload, "pipeline_root", label="root_ref"
    )
    if pipeline_root == "":
        raise RecordContractError("root_ref.pipeline_root must be null or non-empty")
    return StageRootRef(
        stage_batch_root=required_string(
            payload, "stage_batch_root", label="root_ref", non_empty=True
        ),
        pipeline_root=pipeline_root,
        schema_version=version,
    )
