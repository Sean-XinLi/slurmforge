"""Stage run -> stage_batch / train-eval pipeline root indirection."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..io import SchemaVersion, read_json, require_schema, write_json
from ..root_paths import parent_pipeline_root_for_stage_batch


def _root_ref_path(run_dir: Path) -> Path:
    return Path(run_dir) / "root_ref.json"


@dataclass(frozen=True)
class StageRootRef:
    stage_batch_root: str
    pipeline_root: str | None = None
    schema_version: int = SchemaVersion.ROOT_REF


def infer_stage_root_ref(run_dir: Path) -> StageRootRef | None:
    target = Path(run_dir).resolve()
    if target.parent.name != "runs":
        return None
    stage_batch_root = target.parent.parent.resolve()
    pipeline_root = parent_pipeline_root_for_stage_batch(stage_batch_root)
    return StageRootRef(
        stage_batch_root=str(stage_batch_root),
        pipeline_root=None if pipeline_root is None else str(pipeline_root),
    )


def write_root_ref(
    run_dir: Path,
    *,
    stage_batch_root: Path,
    pipeline_root: Path | None = None,
) -> StageRootRef:
    ref = StageRootRef(
        stage_batch_root=str(Path(stage_batch_root).resolve()),
        pipeline_root=None if pipeline_root is None else str(Path(pipeline_root).resolve()),
    )
    write_json(_root_ref_path(run_dir), ref)
    return ref


def read_root_ref(run_dir: Path) -> StageRootRef | None:
    path = _root_ref_path(run_dir)
    if not path.exists():
        return infer_stage_root_ref(run_dir)
    payload = read_json(path)
    version = require_schema(payload, name="root_ref", version=SchemaVersion.ROOT_REF)
    return StageRootRef(
        stage_batch_root=str(payload["stage_batch_root"]),
        pipeline_root=None if payload.get("pipeline_root") in (None, "") else str(payload["pipeline_root"]),
        schema_version=version,
    )
