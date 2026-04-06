from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any

from ...config.runtime import NotifyConfig
from ...planning import BatchIdentity, PlannedRun
from ...planning.contracts import PlanDiagnostic, ensure_plan_diagnostic
from ...sources.models import FailedCompiledRun, SourceInputBatch, SourceRunInput
from ..requests import SourceRequest


@dataclass(frozen=True)
class SourceCollectionReport:
    request: SourceRequest
    batch: SourceInputBatch

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "batch",
            self.batch if isinstance(self.batch, SourceInputBatch) else SourceInputBatch(**self.batch),
        )

    @property
    def source_inputs(self) -> tuple[SourceRunInput, ...]:
        return self.batch.source_inputs

    @property
    def checked_inputs(self) -> int:
        return self.batch.checked_inputs

    @property
    def manifest_extras(self) -> dict[str, Any]:
        return self.batch.manifest_extras

    @property
    def source_failures(self) -> tuple[FailedCompiledRun, ...]:
        return self.batch.failed_runs

    @property
    def batch_diagnostics(self) -> tuple[PlanDiagnostic, ...]:
        return self.batch.batch_diagnostics

    @property
    def source_summary(self) -> str:
        return self.batch.source_summary


@dataclass(frozen=True)
class BatchCompileReport:
    identity: BatchIdentity | None
    successful_runs: tuple[PlannedRun, ...]
    failed_runs: tuple[FailedCompiledRun, ...]
    batch_diagnostics: tuple[PlanDiagnostic, ...] = field(default_factory=tuple)
    checked_runs: int = 0
    notify_cfg: NotifyConfig | None = None
    submit_dependencies: dict[str, list[str]] = field(default_factory=dict)
    manifest_extras: dict[str, Any] = field(default_factory=dict)
    source_summary: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "successful_runs", tuple(self.successful_runs))
        object.__setattr__(self, "failed_runs", tuple(self.failed_runs))
        object.__setattr__(
            self,
            "batch_diagnostics",
            tuple(ensure_plan_diagnostic(item, name="batch_diagnostics[]") for item in self.batch_diagnostics),
        )
        object.__setattr__(self, "checked_runs", int(self.checked_runs or 0))
        object.__setattr__(self, "submit_dependencies", copy.deepcopy(self.submit_dependencies or {}))
        object.__setattr__(self, "manifest_extras", copy.deepcopy(self.manifest_extras or {}))
        object.__setattr__(self, "source_summary", str(self.source_summary or ""))
