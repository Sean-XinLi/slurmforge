from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..io import SchemaVersion

JsonObject = dict[str, Any]


@dataclass(frozen=True)
class RunDefinition:
    run_id: str
    run_index: int
    run_overrides: JsonObject
    spec_snapshot_digest: str
    schema_version: int = SchemaVersion.PLAN
