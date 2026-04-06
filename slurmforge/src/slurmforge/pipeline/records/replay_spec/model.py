from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


CURRENT_REPLAY_SCHEMA_VERSION = 5


@dataclass(frozen=True)
class ReplaySpec:
    schema_version: int = CURRENT_REPLAY_SCHEMA_VERSION
    replay_cfg: dict[str, Any] = field(default_factory=dict)
    planning_root: str = ""
    source_batch_root: str | None = None
    source_run_id: str | None = None
    source_record_path: str | None = None
