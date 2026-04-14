"""Atomic read primitives for planning files.

Owns the on-disk I/O for run plans, run manifests, and run snapshots.
Backends import from here instead of ``pipeline.records.batch_io`` /
``pipeline.records.snapshot_io``.

Note: codecs (serialize/deserialize) remain in ``pipeline.records.codecs`` —
they are pure data transformations, not I/O.
"""
from __future__ import annotations

import json
from pathlib import Path

from ...pipeline.records.batch_paths import bind_run_plan_to_batch
from ...pipeline.records.codecs.run_plan import deserialize_run_plan
from ...pipeline.records.codecs.run_snapshot import deserialize_run_snapshot
from ...pipeline.records.models.run_plan import RunPlan
from ...pipeline.records.models.run_snapshot import RunSnapshot
from .paths import run_snapshot_path, runs_manifest_path, task_record_path


def load_batch_run_plans(batch_root: Path) -> list[RunPlan]:
    """Load all RunPlans for a batch from the filesystem.

    Fast path: reads ``meta/runs_manifest.jsonl``.
    Fallback: scans ``records/group_*/task_*.json``.
    """
    resolved_root = batch_root.resolve()
    manifest = runs_manifest_path(resolved_root)
    if manifest.exists():
        return _load_from_manifest(manifest, batch_root=resolved_root)
    plans = _load_from_record_files(resolved_root)
    if plans:
        return plans
    raise FileNotFoundError(f"No run records found under batch_root={resolved_root}")


def _load_from_manifest(manifest_path: Path, *, batch_root: Path) -> list[RunPlan]:
    plans: list[RunPlan] = []
    for line in manifest_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise TypeError(f"Invalid run record line in {manifest_path}")
        plans.append(bind_run_plan_to_batch(batch_root, deserialize_run_plan(payload)))
    return plans


def _load_from_record_files(batch_root: Path) -> list[RunPlan]:
    records = sorted(batch_root.glob("records/group_*/task_*.json"))
    plans: list[RunPlan] = []
    for record_path in records:
        payload = json.loads(record_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise TypeError(f"Invalid run record file: {record_path}")
        plans.append(bind_run_plan_to_batch(batch_root, deserialize_run_plan(payload)))
    return plans


def load_run_snapshot(run_dir: Path) -> RunSnapshot:
    """Load a RunSnapshot from its canonical filesystem path."""
    path = run_snapshot_path(run_dir)
    if not path.exists():
        raise FileNotFoundError(f"Run snapshot not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    return deserialize_run_snapshot(payload)


def load_run_plan_file(path: Path, *, batch_root: Path) -> RunPlan:
    """Load a single RunPlan from a JSON file."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"Invalid run plan file: {path}")
    return bind_run_plan_to_batch(batch_root.resolve(), deserialize_run_plan(payload))
