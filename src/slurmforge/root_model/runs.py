from __future__ import annotations

from pathlib import Path
from typing import Iterable

from ..status.models import StageStatusRecord
from ..status.reader import read_stage_status
from ..storage.execution_catalog import iter_catalog_batch_roots
from ..storage.runtime_batches import iter_runtime_batch_roots
from .detection import detect_root


def iter_runtime_stage_run_dirs(root: Path) -> Iterable[Path]:
    descriptor = detect_root(root)
    if descriptor.kind == "stage_batch":
        yield from sorted((descriptor.root / "runs").glob("*"))
        return
    yield from _iter_run_dirs_from_batch_roots(
        iter_runtime_batch_roots(descriptor.root)
    )


def iter_all_stage_run_dirs(root: Path) -> Iterable[Path]:
    descriptor = detect_root(root)
    if descriptor.kind == "stage_batch":
        yield from sorted((descriptor.root / "runs").glob("*"))
        return
    yield from _iter_run_dirs_from_batch_roots(
        [
            *iter_runtime_batch_roots(descriptor.root),
            *iter_catalog_batch_roots(descriptor.root),
        ]
    )


def collect_stage_statuses(root: Path) -> list[StageStatusRecord]:
    statuses: list[StageStatusRecord] = []
    for run_dir in iter_runtime_stage_run_dirs(root):
        status = read_stage_status(run_dir)
        if status is not None:
            statuses.append(status)
    return statuses


def _iter_run_dirs_from_batch_roots(batch_roots: Iterable[Path]) -> Iterable[Path]:
    seen: set[Path] = set()
    for stage_root in batch_roots:
        runs_dir = stage_root / "runs"
        if not runs_dir.exists():
            continue
        for run_dir in sorted(runs_dir.glob("*")):
            resolved = run_dir.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            yield run_dir
