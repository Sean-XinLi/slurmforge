from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from ..io import SchemaVersion
from ..plans.stage import StageBatchPlan
from .batch_registry import (
    initialize_batch_registry,
    iter_batch_records,
    iter_batch_roots,
    read_batch_registry,
    upsert_batch_record,
    write_batch_registry,
)
from .paths import runtime_batches_path


def read_runtime_batches(pipeline_root: Path) -> dict[str, Any]:
    return read_batch_registry(runtime_batches_path(pipeline_root))


def write_runtime_batches(pipeline_root: Path, registry: dict[str, Any]) -> None:
    write_batch_registry(
        runtime_batches_path(pipeline_root),
        registry,
        schema_version=SchemaVersion.RUNTIME_BATCHES,
    )


def initialize_runtime_batches(pipeline_root: Path, *, pipeline_id: str) -> None:
    initialize_batch_registry(
        runtime_batches_path(pipeline_root),
        pipeline_id=pipeline_id,
        schema_version=SchemaVersion.RUNTIME_BATCHES,
    )


def upsert_runtime_batch(
    pipeline_root: Path,
    batch: StageBatchPlan,
    *,
    role: str,
    shard_id: str = "",
    source_train_group_id: str = "",
) -> None:
    upsert_batch_record(
        runtime_batches_path(pipeline_root),
        batch,
        schema_version=SchemaVersion.RUNTIME_BATCHES,
        role=role,
        shard_id=shard_id,
        source_train_group_id=source_train_group_id,
    )


def iter_runtime_batch_records(
    pipeline_root: Path, *, stage: str | None = None
) -> Iterable[dict[str, Any]]:
    yield from iter_batch_records(runtime_batches_path(pipeline_root), stage=stage)


def iter_runtime_batch_roots(
    pipeline_root: Path, *, stage: str | None = None
) -> Iterable[Path]:
    yield from iter_batch_roots(runtime_batches_path(pipeline_root), stage=stage)
