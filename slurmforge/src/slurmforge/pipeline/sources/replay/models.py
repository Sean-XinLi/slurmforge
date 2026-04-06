from __future__ import annotations

from dataclasses import dataclass, field

from ...records import RunPlan, RunSnapshot
from ...status import ExecutionStatus
from ..models import FailedCompiledRun


@dataclass(frozen=True)
class RetryCandidate:
    plan: RunPlan
    snapshot: RunSnapshot
    status: ExecutionStatus | None


@dataclass(frozen=True)
class SelectedRetryCandidate:
    source_index: int
    candidate: RetryCandidate


@dataclass(frozen=True)
class RetrySelectionResult:
    matched_count: int
    candidates: tuple[SelectedRetryCandidate, ...] = ()
    failed_runs: tuple[FailedCompiledRun, ...] = ()
    selected_run_indices: tuple[int, ...] = ()
    selected_run_ids: tuple[str, ...] = ()
    selected_failure_counts: dict[str, int] = field(default_factory=dict)
