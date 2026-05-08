"""Lineage index queries."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from .paths import load_lineage_index
from .records import (
    LineageInputSourceRecord,
    StageBatchLineageRecord,
    TrainEvalPipelineLineageRecord,
)


def iter_lineage_source_roots(root: Path, *, max_depth: int = 4) -> Iterable[Path]:
    seen: set[Path] = set()

    def visit(current: Path, depth: int) -> Iterable[Path]:
        if depth > max_depth:
            return
        index = load_lineage_index(current)
        if not index:
            return
        for raw in index.source_roots:
            candidate = Path(raw).expanduser().resolve()
            if candidate in seen:
                continue
            seen.add(candidate)
            yield candidate
            yield from visit(candidate, depth + 1)
        if isinstance(index, TrainEvalPipelineLineageRecord):
            for batch in index.stage_batches.values():
                candidate = Path(batch.root).expanduser().resolve()
                if candidate in seen:
                    continue
                seen.add(candidate)
                yield candidate
                yield from visit(candidate, depth + 1)

    yield from visit(root.resolve(), 0)


def find_bound_input(
    root: Path,
    *,
    run_id: str,
    input_name: str,
    lineage_ref: str | None = None,
) -> LineageInputSourceRecord | None:
    index = load_lineage_index(root)
    if not isinstance(index, StageBatchLineageRecord):
        return None
    for item in index.input_sources:
        if item.run_id != run_id:
            continue
        if item.input_name != input_name:
            continue
        resolution = item.resolution
        item_lineage_ref = (
            resolution["lineage_ref"] if "lineage_ref" in resolution else ""
        )
        if lineage_ref is not None and item_lineage_ref not in {
            lineage_ref,
            "",
        }:
            continue
        return item
    return None
