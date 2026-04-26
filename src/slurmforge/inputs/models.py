from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


JsonDict = dict[str, Any]


@dataclass(frozen=True)
class InputVerificationRecord:
    input_name: str
    source: JsonDict
    expects: str
    required: bool
    resolved_kind: str = "unresolved"
    resolved_path: str | None = None
    path_kind: str = "unknown"
    exists: bool | None = None
    readable: bool | None = None
    size_bytes: int | None = None
    digest: str = ""
    value_digest: str = ""
    expected_digest: str = ""
    producer_digest: str = ""
    producer_stage_instance_id: str = ""
    verified_at: str = ""
    phase: str = ""
    state: str = "skipped"
    failure_class: str | None = None
    reason: str = ""


@dataclass(frozen=True)
class StageInputVerificationReport:
    schema_version: int
    stage_instance_id: str
    run_id: str
    stage_name: str
    phase: str
    state: str
    records: tuple[InputVerificationRecord, ...] = field(default_factory=tuple)
