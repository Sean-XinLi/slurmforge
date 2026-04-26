"""Lineage index file path + low-level read/write."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from ..io import read_json, write_json


LINEAGE_INDEX_FILENAME = "lineage_index.json"


def lineage_index_path(root: Path) -> Path:
    return root / LINEAGE_INDEX_FILENAME


def load_lineage_index(root: Path) -> dict[str, Any] | None:
    path = lineage_index_path(root)
    if not path.exists():
        return None
    return read_json(path)


def write_lineage_index(root: Path, payload: dict[str, Any]) -> Path:
    path = lineage_index_path(root)
    write_json(path, payload)
    return path
