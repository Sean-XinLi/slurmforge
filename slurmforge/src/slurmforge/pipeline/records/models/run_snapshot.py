from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..replay_spec import ReplaySpec
from .metadata import GeneratedBy


@dataclass(frozen=True)
class RunSnapshot:
    run_index: int
    total_runs: int
    run_id: str
    project: str
    experiment_name: str
    model_name: str
    train_mode: str
    replay_spec: ReplaySpec
    sweep_case_name: str | None = None
    sweep_assignments: dict[str, Any] = field(default_factory=dict)
    generated_by: GeneratedBy = field(default_factory=GeneratedBy)
