from __future__ import annotations

import shutil
from pathlib import Path


def failure_record(
    *,
    category: str,
    pattern: str,
    stage: str,
    error: Exception,
    source: Path | None = None,
) -> dict[str, str]:
    record = {
        "category": category,
        "pattern": pattern,
        "stage": stage,
        "error_type": type(error).__name__,
        "error": str(error),
    }
    if source is not None:
        record["source"] = str(source)
    return record


def copy_item(src: Path, dst_root: Path, rel_root: Path) -> str:
    try:
        rel = src.relative_to(rel_root)
    except ValueError:
        rel = Path(src.name)
    dst = dst_root / rel
    dst.parent.mkdir(parents=True, exist_ok=True)
    if src.is_dir():
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst)
    else:
        shutil.copy2(src, dst)
    return str(dst)
