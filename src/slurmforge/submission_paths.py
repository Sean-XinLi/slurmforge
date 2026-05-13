from __future__ import annotations

from pathlib import Path


def submit_manifest_path(batch_root: Path) -> Path:
    return Path(batch_root) / "submit" / "submit_manifest.json"
