from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from ..errors import RecordContractError
from ..io import read_json, require_schema, utc_now, write_json
from ..plans.stage import StageBatchPlan
from ..workflow_contract import TRAIN_EVAL_STAGE_ORDER
from ..record_fields import required_array, required_string


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
        pipeline_id=required_string(
            payload, "pipeline_id", label="pipeline_batch_registry"
        ),
        updated_at=required_string(
            payload, "updated_at", label="pipeline_batch_registry", non_empty=True
        ),
        batches=[
            batch_registry_record_from_dict(dict(item))
            for item in _batch_records(payload)
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
        stage_name=required_string(
            payload, "stage_name", label="pipeline_batch_registry record", non_empty=True
        ),
        role=required_string(
            payload, "role", label="pipeline_batch_registry record", non_empty=True
        ),
        dispatch_id=required_string(
            payload, "dispatch_id", label="pipeline_batch_registry record"
        ),
        stage_batch_root=required_string(
            payload,
            "stage_batch_root",
            label="pipeline_batch_registry record",
            non_empty=True,
        ),
        batch_id=required_string(
            payload, "batch_id", label="pipeline_batch_registry record", non_empty=True
        ),
        source_ref=required_string(
            payload, "source_ref", label="pipeline_batch_registry record", non_empty=True
        ),
        source_dispatch_id=required_string(
            payload, "source_dispatch_id", label="pipeline_batch_registry record"
        ),
        run_ids=_required_string_array(payload, "run_ids"),
        stage_instance_ids=tuple(
            _required_string_array(payload, "stage_instance_ids")
        ),
        group_ids=_required_string_array(payload, "group_ids"),
        updated_at=required_string(
            payload,
            "updated_at",
            label="pipeline_batch_registry record",
            non_empty=True,
        ),
    )


def _batch_record_sort_key(item: BatchRegistryRecord) -> tuple[int, str, str, str]:
    stage_order = {stage: index for index, stage in enumerate(TRAIN_EVAL_STAGE_ORDER)}
    return (
        stage_order.get(item.stage_name, 99),
        item.role,
        item.dispatch_id,
        item.stage_batch_root,
    )


def _batch_records(payload: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    records = []
    for item in required_array(payload, "batches", label="pipeline_batch_registry"):
        if not isinstance(item, dict):
            raise RecordContractError("pipeline_batch_registry.batches items must be objects")
        records.append(dict(item))
    return tuple(records)


def _required_string_array(
    payload: dict[str, Any], field_name: str
) -> tuple[str, ...]:
    values = required_array(
        payload, field_name, label="pipeline_batch_registry record"
    )
    result = []
    for item in values:
        if not isinstance(item, str):
            raise RecordContractError(
                f"pipeline_batch_registry record.{field_name} items must be strings"
            )
        result.append(item)
    return tuple(result)
