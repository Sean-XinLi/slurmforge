from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from ..io import SchemaVersion, read_json, utc_now, write_json
from ..plans.stage import StageBatchPlan
from .paths import execution_index_path


def empty_execution_index(pipeline_id: str) -> dict[str, Any]:
    return {
        "schema_version": SchemaVersion.EXECUTION_INDEX,
        "pipeline_id": pipeline_id,
        "updated_at": utc_now(),
        "batches": [],
    }


def read_execution_index(pipeline_root: Path) -> dict[str, Any]:
    path = execution_index_path(pipeline_root)
    if not path.exists():
        raise FileNotFoundError(f"execution index does not exist: {path}")
    payload = read_json(path)
    if not isinstance(payload.get("batches"), list):
        payload["batches"] = []
    return payload


def write_execution_index(pipeline_root: Path, index: dict[str, Any]) -> None:
    index["schema_version"] = SchemaVersion.EXECUTION_INDEX
    index["updated_at"] = utc_now()
    if not isinstance(index.get("batches"), list):
        index["batches"] = []
    write_json(execution_index_path(pipeline_root), index)


def initialize_execution_index(pipeline_root: Path, *, pipeline_id: str) -> None:
    write_execution_index(pipeline_root, empty_execution_index(pipeline_id))


def upsert_execution_batch(
    pipeline_root: Path,
    batch: StageBatchPlan,
    *,
    role: str,
    shard_id: str = "",
    source_train_group_id: str = "",
    status_scope: bool = True,
) -> None:
    try:
        index = read_execution_index(pipeline_root)
    except FileNotFoundError:
        index = empty_execution_index("")

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
        "status_scope": status_scope,
        "run_ids": list(batch.selected_runs),
        "group_ids": [group.group_id for group in batch.group_plans],
        "updated_at": utc_now(),
    }
    batches = [
        item
        for item in index.get("batches", [])
        if not (
            isinstance(item, dict)
            and item.get("stage_name") == key["stage_name"]
            and item.get("role") == key["role"]
            and item.get("shard_id") == key["shard_id"]
            and item.get("stage_batch_root") == key["stage_batch_root"]
        )
    ]
    batches.append(record)
    stage_order = {"train": 0, "eval": 1}
    index["batches"] = sorted(
        batches,
        key=lambda item: (
            stage_order.get(str(item.get("stage_name") or ""), 99),
            str(item.get("role") or ""),
            str(item.get("shard_id") or ""),
            str(item.get("stage_batch_root") or ""),
        ),
    )
    write_execution_index(pipeline_root, index)


def iter_execution_batch_records(
    pipeline_root: Path,
    *,
    stage: str | None = None,
    status_scope: bool | None = None,
) -> Iterable[dict[str, Any]]:
    index = read_execution_index(pipeline_root)
    for item in index.get("batches", []):
        if not isinstance(item, dict):
            continue
        if stage is not None and item.get("stage_name") != stage:
            continue
        if (
            status_scope is not None
            and bool(item.get("status_scope", True)) != status_scope
        ):
            continue
        yield dict(item)


def iter_execution_batch_roots(
    pipeline_root: Path,
    *,
    stage: str | None = None,
    status_scope: bool | None = None,
) -> Iterable[Path]:
    for record in iter_execution_batch_records(
        pipeline_root, stage=stage, status_scope=status_scope
    ):
        root = record.get("stage_batch_root")
        if root:
            yield Path(str(root)).resolve()
