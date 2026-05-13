from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any

from ..io import SchemaVersion, read_json_object, utc_now, write_json_object
from ..record_fields import required_string


def append_scheduler_observation(batch_root: Path, payload: dict[str, Any]) -> None:
    path = Path(batch_root) / "scheduler_observations.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _age_seconds(timestamp: str) -> float:
    started = datetime.datetime.fromisoformat(timestamp)
    now = datetime.datetime.now(datetime.timezone.utc)
    return (now - started).total_seconds()


def missing_output_expired(
    run_dir: Path, *, slurm_state: str, grace_seconds: int
) -> bool:
    path = Path(run_dir) / "reconcile.json"
    if path.exists():
        payload = read_json_object(path)
        first_missing = required_string(
            payload,
            "first_missing_output_at",
            label="scheduler_reconcile",
            non_empty=True,
        )
    else:
        first_missing = utc_now()
        write_json_object(
            path,
            {
                "schema_version": SchemaVersion.STATUS,
                "first_missing_output_at": first_missing,
                "grace_seconds": grace_seconds,
                "slurm_state": slurm_state,
            },
        )
    return _age_seconds(first_missing) >= grace_seconds
