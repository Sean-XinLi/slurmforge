from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from ..io import read_json, require_schema, utc_now, write_json
from ..plans.stage import StageBatchPlan
from ..workflow_contract import TRAIN_EVAL_STAGE_ORDER


@dataclass
class BatchRegistryRecord:
    stage_name: str
    role: str
    dispatch_id: str
    stage_batch_root: str
    batch_id: str
    source_ref: str
    source_dispatch_id: str
    run_ids: tuple[str, ...]
    stage_instance_ids: tuple[str, ...]
    group_ids: tuple[str, ...]
    updated_at: str


@dataclass
class BatchRegistry:
    schema_version: int
    pipeline_id: str
    updated_at: str
    batches: list[BatchRegistryRecord] = field(default_factory=list)


def empty_batch_registry(pipeline_id: str, *, schema_version: int) -> BatchRegistry:
    return BatchRegistry(
        schema_version=schema_version,
        pipeline_id=pipeline_id,
        updated_at=utc_now(),
        batches=[],
    )


def read_batch_registry(path: Path, *, schema_version: int) -> BatchRegistry:
    if not path.exists():
        raise FileNotFoundError(f"pipeline batch registry does not exist: {path}")
    payload = read_json(path)
    version = require_schema(
        payload, name="pipeline_batch_registry", version=schema_version
    )
    return BatchRegistry(
        schema_version=version,
        pipeline_id=str(payload["pipeline_id"]),
        updated_at=str(payload["updated_at"]),
        batches=[
            batch_registry_record_from_dict(dict(item))
            for item in payload["batches"]
        ],
    )


def write_batch_registry(
    path: Path, registry: BatchRegistry, *, schema_version: int
) -> None:
    registry.schema_version = schema_version
    registry.updated_at = utc_now()
    write_json(path, registry)


def initialize_batch_registry(
    path: Path, *, pipeline_id: str, schema_version: int
) -> None:
    write_batch_registry(
        path,
        empty_batch_registry(pipeline_id, schema_version=schema_version),
        schema_version=schema_version,
    )


def upsert_batch_record(
    path: Path,
    batch: StageBatchPlan,
    *,
    schema_version: int,
    role: str,
    dispatch_id: str = "",
    source_dispatch_id: str = "",
) -> None:
    try:
        registry = read_batch_registry(path, schema_version=schema_version)
    except FileNotFoundError:
        registry = empty_batch_registry("", schema_version=schema_version)

    root = str(Path(batch.submission_root).resolve())
    record = BatchRegistryRecord(
        stage_name=batch.stage_name,
        role=role,
        dispatch_id=dispatch_id,
        stage_batch_root=root,
        batch_id=batch.batch_id,
        source_ref=batch.source_ref,
        source_dispatch_id=source_dispatch_id,
        run_ids=tuple(batch.selected_runs),
        stage_instance_ids=tuple(
            instance.stage_instance_id for instance in batch.stage_instances
        ),
        group_ids=tuple(group.group_id for group in batch.group_plans),
        updated_at=utc_now(),
    )
    registry.batches = sorted(
        [
            item
            for item in registry.batches
            if not (
                item.stage_name == record.stage_name
                and item.role == record.role
                and item.dispatch_id == record.dispatch_id
                and item.stage_batch_root == record.stage_batch_root
            )
        ]
        + [record],
        key=_batch_record_sort_key,
    )
    write_batch_registry(path, registry, schema_version=schema_version)


def iter_batch_records(
    path: Path, *, schema_version: int, stage: str | None = None
) -> Iterable[BatchRegistryRecord]:
    registry = read_batch_registry(path, schema_version=schema_version)
    for item in registry.batches:
        if stage is not None and item.stage_name != stage:
            continue
        yield item


def iter_batch_roots(
    path: Path, *, schema_version: int, stage: str | None = None
) -> Iterable[Path]:
    for record in iter_batch_records(
        path, schema_version=schema_version, stage=stage
    ):
        yield Path(record.stage_batch_root).resolve()


def batch_registry_record_from_dict(payload: dict[str, Any]) -> BatchRegistryRecord:
    return BatchRegistryRecord(
        stage_name=str(payload["stage_name"]),
        role=str(payload["role"]),
        dispatch_id=str(payload.get("dispatch_id") or ""),
        stage_batch_root=str(payload["stage_batch_root"]),
        batch_id=str(payload["batch_id"]),
        source_ref=str(payload["source_ref"]),
        source_dispatch_id=str(payload.get("source_dispatch_id") or ""),
        run_ids=tuple(str(item) for item in payload["run_ids"]),
        stage_instance_ids=tuple(
            str(item) for item in payload.get("stage_instance_ids") or ()
        ),
        group_ids=tuple(str(item) for item in payload["group_ids"]),
        updated_at=str(payload["updated_at"]),
    )


def _batch_record_sort_key(item: BatchRegistryRecord) -> tuple[int, str, str, str]:
    stage_order = {stage: index for index, stage in enumerate(TRAIN_EVAL_STAGE_ORDER)}
    return (
        stage_order.get(item.stage_name, 99),
        item.role,
        item.dispatch_id,
        item.stage_batch_root,
    )
