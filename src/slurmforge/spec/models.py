from __future__ import annotations

import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..schema import InputInjection, InputSource
from .output_contract import StageOutputContract


JsonDict = dict[str, Any]


@dataclass(frozen=True)
class EntrySpec:
    type: str
    workdir: str
    args: JsonDict = field(default_factory=dict)
    script: str | None = None
    command: str | list[str] | None = None


@dataclass(frozen=True)
class ResourceSpec:
    partition: str | None = None
    account: str | None = None
    qos: str | None = None
    time_limit: str | None = None
    nodes: int = 1
    gpus_per_node: int = 0
    cpus_per_task: int = 1
    mem: str | None = None
    constraint: str | None = None
    extra_sbatch_args: tuple[str, ...] = ()


@dataclass(frozen=True)
class PythonRuntimeSpec:
    bin: str = "python3"
    min_version: str = "3.10"


@dataclass(frozen=True)
class ExecutorRuntimeSpec:
    python: PythonRuntimeSpec = field(default_factory=PythonRuntimeSpec)
    executor_module: str = "slurmforge.executor.stage"
    bootstrap_scope: str = "sbatch"
    bootstrap_steps: tuple[JsonDict, ...] = ()
    env: JsonDict = field(default_factory=dict)


@dataclass(frozen=True)
class UserRuntimeSpec:
    python: PythonRuntimeSpec = field(default_factory=PythonRuntimeSpec)
    env: JsonDict = field(default_factory=dict)


@dataclass(frozen=True)
class RuntimeSpec:
    executor: ExecutorRuntimeSpec = field(default_factory=ExecutorRuntimeSpec)
    user: dict[str, UserRuntimeSpec] = field(default_factory=lambda: {"default": UserRuntimeSpec()})


@dataclass(frozen=True)
class ArtifactStoreSpec:
    strategy: str = "copy"
    fallback_strategy: str | None = None
    verify_digest: bool = True
    fail_on_verify_error: bool = True


@dataclass(frozen=True)
class LauncherSpec:
    type: str = "single"
    options: JsonDict = field(default_factory=dict)


@dataclass(frozen=True)
class StageInputSpec:
    name: str
    source: InputSource
    expects: str
    required: bool = False
    inject: InputInjection = field(default_factory=InputInjection)


@dataclass(frozen=True)
class StageSpec:
    name: str
    kind: str
    enabled: bool
    entry: EntrySpec
    resources: ResourceSpec
    launcher: LauncherSpec = field(default_factory=LauncherSpec)
    runtime: str = "default"
    depends_on: tuple[str, ...] = ()
    inputs: dict[str, StageInputSpec] = field(default_factory=dict)
    outputs: StageOutputContract = field(default_factory=StageOutputContract)


@dataclass(frozen=True)
class StorageSpec:
    root: str

    def root_path(self, project_root: Path) -> Path:
        path = Path(self.root)
        return path if path.is_absolute() else (project_root / path)


@dataclass(frozen=True)
class DispatchSpec:
    max_available_gpus: int = 0
    overflow_policy: str = "serialize_groups"


@dataclass(frozen=True)
class OrchestrationSpec:
    controller_partition: str | None = None
    controller_cpus: int = 1
    controller_mem: str | None = "2G"
    controller_time_limit: str | None = None


@dataclass(frozen=True)
class ExperimentSpec:
    project: str
    experiment: str
    storage: StorageSpec
    stages: dict[str, StageSpec]
    project_root: Path
    config_path: Path
    spec_snapshot_digest: str
    raw: JsonDict
    matrix_axes: tuple[tuple[str, tuple[Any, ...]], ...] = ()
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
        order = [name for name in ("train", "eval") if name in stages]
        return tuple(order)

    def with_raw(self, raw: JsonDict, digest: str) -> "ExperimentSpec":
        from .parser import parse_experiment_spec

        return parse_experiment_spec(
            copy.deepcopy(raw),
            config_path=self.config_path,
            project_root=self.project_root,
            forced_digest=digest,
        )
