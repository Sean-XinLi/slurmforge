from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from ...config_contract.workflows import STAGE_EVAL, STAGE_TRAIN
from .common import JsonObject
from .environment import EnvironmentSpec
from .notifications import NotificationsSpec
from .orchestration import DispatchSpec, OrchestrationSpec
from .runs import RunsSpec
from .runtime import RuntimeSpec
from .sizing import HardwareSpec, SizingSpec
from .stages import StageSpec
from .storage import ArtifactStoreSpec, StorageSpec


@dataclass(frozen=True)
class ExperimentSpec:
    project: str
    experiment: str
    storage: StorageSpec
    stages: dict[str, StageSpec]
    hardware: HardwareSpec
    environments: dict[str, EnvironmentSpec]
    sizing: SizingSpec
    runs: RunsSpec
    notifications: NotificationsSpec
    project_root: Path
    config_path: Path
    spec_snapshot_digest: str
    raw: JsonObject
    runtime: RuntimeSpec = field(default_factory=RuntimeSpec)
    artifact_store: ArtifactStoreSpec = field(default_factory=ArtifactStoreSpec)
    dispatch: DispatchSpec = field(default_factory=DispatchSpec)
    orchestration: OrchestrationSpec = field(default_factory=OrchestrationSpec)

    @property
    def enabled_stages(self) -> dict[str, StageSpec]:
        return {name: stage for name, stage in self.stages.items() if stage.enabled}

    @property
    def storage_root(self) -> Path:
        return self.storage.root_path(self.project_root)

    def stage_order(self, selected: set[str] | None = None) -> tuple[str, ...]:
        stages = self.enabled_stages
        if selected is not None:
            stages = {name: stage for name, stage in stages.items() if name in selected}
        order = [name for name in (STAGE_TRAIN, STAGE_EVAL) if name in stages]
        return tuple(order)
