from __future__ import annotations

from pathlib import Path
from typing import Iterable

from ..status.models import StageStatusRecord
from ..status.reader import read_stage_status
from ..storage.execution_index import iter_execution_batch_roots
from .detection import detect_root


def iter_stage_run_dirs(
    root: Path, *, status_scope: bool | None = None
) -> Iterable[Path]:
    descriptor = detect_root(root)
    if descriptor.kind == "stage_batch":
        yield from sorted((descriptor.root / "runs").glob("*"))
        return
    for stage_root in iter_execution_batch_roots(
        descriptor.root, status_scope=status_scope
    ):
        runs_dir = stage_root / "runs"
        if runs_dir.exists():
            yield from sorted(runs_dir.glob("*"))


def collect_stage_statuses(
    root: Path, *, status_scope: bool | None = None
) -> list[StageStatusRecord]:
    statuses: list[StageStatusRecord] = []
    for run_dir in iter_stage_run_dirs(root, status_scope=status_scope):
        status = read_stage_status(run_dir)
        if status is not None:
            statuses.append(status)
    return statuses
