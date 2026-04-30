from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from ..plans.sources import SourcedStageBatchPlan


ExecutionMode = Literal["preview", "emit", "submit"]


@dataclass(frozen=True)
class StageBatchExecutionResult:
    root: str
    mode: ExecutionMode
    submitted: bool = False
    scheduler_job_ids: dict[str, str] = field(default_factory=dict)
    notification_job_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class SourcedStageBatchExecutionResult:
    plan: SourcedStageBatchPlan
    root: str
    mode: ExecutionMode
    submitted: bool = False
    scheduler_job_ids: dict[str, str] = field(default_factory=dict)
    notification_job_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class TrainEvalPipelineExecutionResult:
    root: str
    mode: ExecutionMode
    submitted: bool = False
    stage_job_ids: dict[str, dict[str, str]] = field(default_factory=dict)
    gate_job_ids: dict[str, str] = field(default_factory=dict)
