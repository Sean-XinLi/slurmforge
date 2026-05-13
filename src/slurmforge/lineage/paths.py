"""Lineage index file path + low-level read/write."""

from __future__ import annotations

from pathlib import Path

from ..io import read_json_object, write_json_object
from .records import LineageIndexRecord, lineage_index_from_dict, lineage_index_to_dict


LINEAGE_INDEX_FILENAME = "lineage_index.json"


def lineage_index_path(root: Path) -> Path:
    return root / LINEAGE_INDEX_FILENAME


def load_lineage_index(root: Path) -> LineageIndexRecord | None:
    path = lineage_index_path(root)
    if not path.exists():
        return None
    return lineage_index_from_dict(read_json_object(path))


def write_lineage_index(root: Path, record: LineageIndexRecord) -> Path:
    path = lineage_index_path(root)
    write_json_object(path, lineage_index_to_dict(record))
    return path
