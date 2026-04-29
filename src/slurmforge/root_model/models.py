from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from ..io import SchemaVersion
from ..plans.train_eval import TRAIN_EVAL_PIPELINE_KIND
from ..status.models import (
    RunStatusRecord,
    StageStatusRecord,
    TrainEvalPipelineStatusRecord,
)
from ..contracts import NotificationSummaryInput

STAGE_BATCH_KIND = "stage_batch"
RootKind = Literal["stage_batch", "train_eval_pipeline"]


@dataclass(frozen=True)
class RootDescriptor:
    root: Path
    kind: RootKind
    manifest: dict[str, Any]


@dataclass(frozen=True)
class RootStatusSnapshot:
    root: Path
    kind: RootKind
    stage_statuses: tuple[StageStatusRecord, ...]
    run_statuses: tuple[RunStatusRecord, ...]
    pipeline_status: TrainEvalPipelineStatusRecord | None = None
    schema_version: int = SchemaVersion.STATUS


@dataclass(frozen=True)
class RootNotificationSnapshot:
    root: Path
    kind: RootKind
    notification_plan: Any
    summary_input: NotificationSummaryInput
    status: RootStatusSnapshot


def root_kind_from_manifest(value: object) -> RootKind | None:
    if value == STAGE_BATCH_KIND:
        return STAGE_BATCH_KIND
    if value == TRAIN_EVAL_PIPELINE_KIND:
        return "train_eval_pipeline"
    return None
