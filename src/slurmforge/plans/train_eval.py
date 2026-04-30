from __future__ import annotations

from dataclasses import dataclass

from ..io import SchemaVersion
from ..workflow_contract import EVAL_SHARD_GATE as EVAL_SHARD_GATE
from ..workflow_contract import FINAL_GATE as FINAL_GATE
from ..workflow_contract import PIPELINE_GATES as PIPELINE_GATES
from ..workflow_contract import TRAIN_EVAL_PIPELINE_KIND as TRAIN_EVAL_PIPELINE_KIND
from ..workflow_contract import TRAIN_GROUP_GATE as TRAIN_GROUP_GATE
from .notifications import NotificationPlan
from .resources import ControlResourcesPlan
from .runtime import EnvironmentPlan, RuntimePlan
from .stage import StageBatchPlan


@dataclass(frozen=True)
class TrainEvalControlPlan:
    pipeline_id: str
    stage_order: tuple[str, ...]
    config_path: str
    root_dir: str
    pipeline_kind: str
    resources: ControlResourcesPlan
    environment_name: str
    environment_plan: EnvironmentPlan
    runtime_plan: RuntimePlan
    schema_version: int = SchemaVersion.PLAN


@dataclass(frozen=True)
class TrainEvalPipelinePlan:
    pipeline_id: str
    stage_order: tuple[str, ...]
    run_set: tuple[str, ...]
    root_dir: str
    control_plan: TrainEvalControlPlan
    stage_batches: dict[str, StageBatchPlan]
    spec_snapshot_digest: str
    pipeline_kind: str
    notification_plan: NotificationPlan
    schema_version: int = SchemaVersion.PLAN
