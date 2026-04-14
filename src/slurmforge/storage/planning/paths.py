"""Canonical path resolution for planning files.

Backends import from here instead of ``pipeline.records.batch_paths`` /
``pipeline.records.snapshot_io``.
"""
from __future__ import annotations

from pathlib import Path


def task_record_path(batch_root: Path, group_index: int, task_index: int) -> Path:
    return batch_root.resolve() / "records" / f"group_{group_index:02d}" / f"task_{task_index:06d}.json"


def runs_manifest_path(batch_root: Path) -> Path:
    return batch_root.resolve() / "meta" / "runs_manifest.jsonl"


def run_snapshot_path(run_dir: Path) -> Path:
    return run_dir / "meta" / "run_snapshot.json"
