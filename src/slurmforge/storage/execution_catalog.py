from __future__ import annotations

from pathlib import Path
from typing import Iterable

from ..io import SchemaVersion
from ..plans.stage import StageBatchPlan
from ..workflow_contract import BATCH_ROLE_PIPELINE_STAGE
from .batch_registry import (
    BatchRegistry,
    BatchRegistryRecord,
    initialize_batch_registry,
    iter_batch_records,
    iter_batch_roots,
    read_batch_registry,
    upsert_batch_record,
    write_batch_registry,
)
from .paths import stage_catalog_path


def read_stage_catalog(pipeline_root: Path) -> BatchRegistry:
    return read_batch_registry(
        stage_catalog_path(pipeline_root),
        schema_version=SchemaVersion.STAGE_CATALOG,
    )


def write_stage_catalog(pipeline_root: Path, catalog: BatchRegistry) -> None:
    write_batch_registry(
        stage_catalog_path(pipeline_root),
        catalog,
        schema_version=SchemaVersion.STAGE_CATALOG,
    )


def initialize_stage_catalog(pipeline_root: Path, *, pipeline_id: str) -> None:
    initialize_batch_registry(
        stage_catalog_path(pipeline_root),
        pipeline_id=pipeline_id,
        schema_version=SchemaVersion.STAGE_CATALOG,
    )


def upsert_catalog_batch(
    pipeline_root: Path,
    batch: StageBatchPlan,
    *,
    role: str = BATCH_ROLE_PIPELINE_STAGE,
    dispatch_id: str = "",
    source_dispatch_id: str = "",
) -> None:
    upsert_batch_record(
        stage_catalog_path(pipeline_root),
        batch,
        schema_version=SchemaVersion.STAGE_CATALOG,
        role=role,
        dispatch_id=dispatch_id,
        source_dispatch_id=source_dispatch_id,
    )


def iter_catalog_batch_records(
    pipeline_root: Path, *, stage: str | None = None
) -> Iterable[BatchRegistryRecord]:
    yield from iter_batch_records(
        stage_catalog_path(pipeline_root),
        schema_version=SchemaVersion.STAGE_CATALOG,
        stage=stage,
    )


def iter_catalog_batch_roots(
    pipeline_root: Path, *, stage: str | None = None
) -> Iterable[Path]:
    yield from iter_batch_roots(
        stage_catalog_path(pipeline_root),
        schema_version=SchemaVersion.STAGE_CATALOG,
        stage=stage,
    )
