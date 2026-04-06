from __future__ import annotations

import json
from pathlib import Path

from .codecs.run_snapshot import deserialize_run_snapshot
from .models.run_snapshot import RunSnapshot


def run_snapshot_path_for_run(run_dir: Path) -> Path:
    return run_dir / "meta" / "run_snapshot.json"


def load_run_snapshot(run_dir: Path) -> RunSnapshot:
    path = run_snapshot_path_for_run(run_dir)
    if not path.exists():
        raise FileNotFoundError(f"Run snapshot not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    return deserialize_run_snapshot(payload)
