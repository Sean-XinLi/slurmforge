"""Lineage index queries."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from .paths import load_lineage_index


def iter_lineage_source_roots(root: Path, *, max_depth: int = 4) -> Iterable[Path]:
    seen: set[Path] = set()

    def visit(current: Path, depth: int) -> Iterable[Path]:
        if depth > max_depth:
            return
        index = load_lineage_index(current)
        if not index:
            return
        for raw in index.get("source_roots", ()):
            candidate = Path(str(raw)).expanduser().resolve()
            if candidate in seen:
                continue
            seen.add(candidate)
            yield candidate
            yield from visit(candidate, depth + 1)
        for batch in dict(index.get("stage_batches") or {}).values():
            if not isinstance(batch, dict) or not batch.get("root"):
                continue
            candidate = Path(str(batch["root"])).expanduser().resolve()
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
) -> dict[str, Any] | None:
    index = load_lineage_index(root)
    if not index:
        return None
    for item in index.get("input_sources", ()):
        if not isinstance(item, dict):
            continue
        if item.get("run_id") != run_id:
            continue
        if item.get("input_name") != input_name:
            continue
        resolution = dict(item.get("resolution") or {})
        if lineage_ref is not None and resolution.get("lineage_ref") not in {lineage_ref, None, ""}:
            continue
        return dict(item)
    return None
