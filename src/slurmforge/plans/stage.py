from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..io import SchemaVersion
from ..contracts import InputBinding
from ..sizing.models import GpuSizingResolution
from ..contracts.outputs import StageOutputContract
from .budget import BudgetPlan
from .launcher import BeforeStepPlan, LauncherPlan
from .notifications import NotificationPlan
from .outputs import ArtifactStorePlan
from .resources import ResourcePlan
from .runtime import EnvironmentPlan, RuntimePlan


@dataclass(frozen=True)
class EntryPlan:
    type: str
    script: str | None
    command: str | list[str] | None
    workdir: str
    args: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StageInstanceLineage:
    project: str
    experiment: str
    config_path: str
    project_root: str
    source_ref: str


@dataclass(frozen=True)
class StageInstancePlan:
    stage_instance_id: str
    run_id: str
    run_index: int
    stage_name: str
    stage_kind: str
    entry: EntryPlan
    resources: ResourcePlan
    runtime_plan: RuntimePlan
    environment_name: str
    environment_plan: EnvironmentPlan
    before_steps: tuple[BeforeStepPlan, ...]
    launcher_plan: LauncherPlan
    artifact_store_plan: ArtifactStorePlan
    input_bindings: tuple[InputBinding, ...]
    output_contract: StageOutputContract
    lineage: StageInstanceLineage
    run_overrides: dict[str, Any]
    resource_sizing: GpuSizingResolution
    spec_snapshot_digest: str
    run_dir_rel: str
    schema_version: int = SchemaVersion.PLAN

    @property
    def binding_map(self) -> dict[str, InputBinding]:
        return {binding.input_name: binding for binding in self.input_bindings}


@dataclass(frozen=True)
class GroupPlan:
    group_id: str
    group_index: int
    resource_key: str
    resources: ResourcePlan
    stage_instance_ids: tuple[str, ...]
    run_ids: tuple[str, ...]
    array_size: int
    array_throttle: int | None = None
    gpus_per_task: int = 0
    schema_version: int = SchemaVersion.PLAN


@dataclass(frozen=True)
class StageBatchPlan:
    batch_id: str
    stage_name: str
    project: str
    experiment: str
    selected_runs: tuple[str, ...]
    stage_instances: tuple[StageInstancePlan, ...]
    group_plans: tuple[GroupPlan, ...]
    submission_root: str
    source_ref: str
    spec_snapshot_digest: str
    budget_plan: BudgetPlan
    notification_plan: NotificationPlan
    schema_version: int = SchemaVersion.PLAN
