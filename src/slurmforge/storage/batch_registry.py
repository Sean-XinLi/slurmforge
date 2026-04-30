from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from ..io import read_json, utc_now, write_json
from ..plans.stage import StageBatchPlan


def empty_batch_registry(pipeline_id: str, *, schema_version: int) -> dict[str, Any]:
    return {
        "schema_version": schema_version,
        "pipeline_id": pipeline_id,
        "updated_at": utc_now(),
        "batches": [],
    }


def read_batch_registry(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"pipeline batch registry does not exist: {path}")
    payload = read_json(path)
    if not isinstance(payload.get("batches"), list):
        payload["batches"] = []
    return payload


def write_batch_registry(
    path: Path, registry: dict[str, Any], *, schema_version: int
) -> None:
    registry["schema_version"] = schema_version
    registry["updated_at"] = utc_now()
    if not isinstance(registry.get("batches"), list):
        registry["batches"] = []
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
    shard_id: str = "",
    source_train_group_id: str = "",
) -> None:
    try:
        registry = read_batch_registry(path)
    except FileNotFoundError:
        registry = empty_batch_registry("", schema_version=schema_version)

    root = str(Path(batch.submission_root).resolve())
    key = {
        "stage_name": batch.stage_name,
        "role": role,
        "shard_id": shard_id,
        "stage_batch_root": root,
    }
    record = {
        **key,
        "batch_id": batch.batch_id,
        "source_ref": batch.source_ref,
        "source_train_group_id": source_train_group_id,
        "run_ids": list(batch.selected_runs),
        "group_ids": [group.group_id for group in batch.group_plans],
        "updated_at": utc_now(),
    }
    registry["batches"] = sorted(
        [
            item
            for item in registry.get("batches", [])
            if not (
                isinstance(item, dict)
                and item.get("stage_name") == key["stage_name"]
                and item.get("role") == key["role"]
                and item.get("shard_id") == key["shard_id"]
                and item.get("stage_batch_root") == key["stage_batch_root"]
            )
        ]
        + [record],
        key=_batch_record_sort_key,
    )
    write_batch_registry(path, registry, schema_version=schema_version)


def iter_batch_records(
    path: Path, *, stage: str | None = None
) -> Iterable[dict[str, Any]]:
    registry = read_batch_registry(path)
    for item in registry.get("batches", []):
        if not isinstance(item, dict):
            continue
        if stage is not None and item.get("stage_name") != stage:
            continue
        yield dict(item)


def iter_batch_roots(path: Path, *, stage: str | None = None) -> Iterable[Path]:
    for record in iter_batch_records(path, stage=stage):
        root = record.get("stage_batch_root")
        if root:
            yield Path(str(root)).resolve()


def _batch_record_sort_key(item: dict[str, Any]) -> tuple[int, str, str, str]:
    stage_order = {"train": 0, "eval": 1}
    return (
        stage_order.get(str(item.get("stage_name") or ""), 99),
        str(item.get("role") or ""),
        str(item.get("shard_id") or ""),
        str(item.get("stage_batch_root") or ""),
    )
