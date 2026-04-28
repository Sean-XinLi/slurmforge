from __future__ import annotations

from dataclasses import dataclass, field

from ..contracts import InputBinding
from ..io import SchemaVersion
from ..contracts import RunDefinition


@dataclass(frozen=True)
class ResolvedStageInputs:
    stage_name: str
    selected_runs: tuple[RunDefinition, ...]
    input_bindings_by_run: dict[str, tuple[InputBinding, ...]]
    blocked_run_ids: tuple[str, ...] = ()
    blocked_reasons: dict[str, str] = field(default_factory=dict)
    schema_version: int = SchemaVersion.INPUT_CONTRACT
