from __future__ import annotations

import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..planning.contracts import PlanDiagnostic, ensure_plan_diagnostic


@dataclass(frozen=True)
class FailedCompiledRun:
    run_index: int
    total_runs: int
    project: str
    experiment_name: str
    model_name: str
    train_mode: str
    phase: str
    source_label: str | None = None
    sweep_case_name: str | None = None
    sweep_assignments: dict[str, Any] = field(default_factory=dict)
    diagnostics: tuple[PlanDiagnostic, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_index", int(self.run_index))
        object.__setattr__(self, "total_runs", int(self.total_runs))
        object.__setattr__(self, "project", str(self.project))
        object.__setattr__(self, "experiment_name", str(self.experiment_name))
        object.__setattr__(self, "model_name", str(self.model_name))
        object.__setattr__(self, "train_mode", str(self.train_mode))
        object.__setattr__(self, "phase", str(self.phase))
        object.__setattr__(self, "source_label", None if self.source_label is None else str(self.source_label))
        object.__setattr__(self, "sweep_assignments", copy.deepcopy(dict(self.sweep_assignments or {})))
        object.__setattr__(
            self,
            "diagnostics",
            tuple(ensure_plan_diagnostic(item, name="failed_run.diagnostics[]") for item in self.diagnostics),
        )


@dataclass(frozen=True)
class SourceRef:
    config_label: str
    config_path: Path | None = None
    planning_root: str | None = None
    source_batch_root: Path | None = None
    source_run_id: str | None = None
    source_record_path: Path | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "config_label", str(self.config_label))
        object.__setattr__(self, "config_path", None if self.config_path is None else Path(self.config_path))
        object.__setattr__(self, "planning_root", None if self.planning_root is None else str(self.planning_root))
        object.__setattr__(
            self,
            "source_batch_root",
            None if self.source_batch_root is None else Path(self.source_batch_root),
        )
        object.__setattr__(
            self,
            "source_run_id",
            None if self.source_run_id is None else str(self.source_run_id),
        )
        object.__setattr__(
            self,
            "source_record_path",
            None if self.source_record_path is None else Path(self.source_record_path),
        )


@dataclass(frozen=True)
class SourceRunInput:
    source_kind: str
    source_index: int
    run_cfg: dict[str, Any]
    source: SourceRef
    sweep_case_name: str | None = None
    sweep_assignments: dict[str, Any] = field(default_factory=dict)
    original_run_index: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_kind", str(self.source_kind))
        object.__setattr__(self, "source_index", int(self.source_index))
        object.__setattr__(self, "run_cfg", copy.deepcopy(dict(self.run_cfg or {})))
        object.__setattr__(self, "source", self.source if isinstance(self.source, SourceRef) else SourceRef(**self.source))
        object.__setattr__(self, "sweep_case_name", None if self.sweep_case_name is None else str(self.sweep_case_name))
        object.__setattr__(self, "sweep_assignments", copy.deepcopy(dict(self.sweep_assignments or {})))
        object.__setattr__(
            self,
            "original_run_index",
            None if self.original_run_index is None else int(self.original_run_index),
        )


@dataclass(frozen=True)
class SourceInputBatch:
    source_inputs: tuple[SourceRunInput, ...]
    failed_runs: tuple[FailedCompiledRun, ...] = field(default_factory=tuple)
    checked_inputs: int = 0
    batch_diagnostics: tuple[PlanDiagnostic, ...] = field(default_factory=tuple)
    manifest_extras: dict[str, Any] = field(default_factory=dict)
    source_summary: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_inputs", tuple(self.source_inputs))
        object.__setattr__(self, "failed_runs", tuple(self.failed_runs))
        object.__setattr__(self, "checked_inputs", int(self.checked_inputs))
        object.__setattr__(
            self,
            "batch_diagnostics",
            tuple(ensure_plan_diagnostic(item, name="source_batch.batch_diagnostics[]") for item in self.batch_diagnostics),
        )
        object.__setattr__(self, "manifest_extras", copy.deepcopy(self.manifest_extras or {}))
        object.__setattr__(self, "source_summary", str(self.source_summary or ""))
