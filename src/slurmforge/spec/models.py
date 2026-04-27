from __future__ import annotations

import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..schema import InputInjection, InputSource
from .output_contract import StageOutputContract


JsonObject = dict[str, Any]


@dataclass(frozen=True)
class EntrySpec:
    type: str
    workdir: str
    args: JsonObject = field(default_factory=dict)
    script: str | None = None
    command: str | list[str] | None = None


@dataclass(frozen=True)
class ResourceSpec:
    partition: str | None = None
    account: str | None = None
    qos: str | None = None
    time_limit: str | None = None
    gpu_type: str = ""
    nodes: int = 1
    gpus_per_node: int | str = 0
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


@dataclass(frozen=True)
class UserRuntimeSpec:
    python: PythonRuntimeSpec = field(default_factory=PythonRuntimeSpec)
    env: JsonObject = field(default_factory=dict)


@dataclass(frozen=True)
class RuntimeSpec:
    executor: ExecutorRuntimeSpec = field(default_factory=ExecutorRuntimeSpec)
    user: dict[str, UserRuntimeSpec] = field(default_factory=lambda: {"default": UserRuntimeSpec()})


@dataclass(frozen=True)
class EnvironmentSourceSpec:
    path: str
    args: tuple[str, ...] = ()


@dataclass(frozen=True)
class EnvironmentSpec:
    name: str
    modules: tuple[str, ...] = ()
    source: tuple[EnvironmentSourceSpec, ...] = ()
    env: JsonObject = field(default_factory=dict)


@dataclass(frozen=True)
class EmailNotificationSpec:
    enabled: bool = False
    to: tuple[str, ...] = ()
    events: tuple[str, ...] = ()
    mode: str = "summary"
    from_address: str = "slurmforge@localhost"
    sendmail: str = "/usr/sbin/sendmail"
    subject_prefix: str = "SlurmForge"


@dataclass(frozen=True)
class NotificationsSpec:
    email: EmailNotificationSpec = field(default_factory=EmailNotificationSpec)


@dataclass(frozen=True)
class GpuTypeSpec:
    name: str
    memory_gb: float
    usable_memory_fraction: float
    max_gpus_per_node: int | None = None
    slurm: JsonObject = field(default_factory=dict)


@dataclass(frozen=True)
class HardwareSpec:
    gpu_types: dict[str, GpuTypeSpec] = field(default_factory=dict)


@dataclass(frozen=True)
class GpuSizingDefaultsSpec:
    safety_factor: float = 1.0
    round_to: int = 1


@dataclass(frozen=True)
class SizingSpec:
    gpu: GpuSizingDefaultsSpec = field(default_factory=GpuSizingDefaultsSpec)


@dataclass(frozen=True)
class StageGpuSizingSpec:
    estimator: str
    target_memory_gb: float
    min_gpus_per_job: int = 1
    max_gpus_per_job: int | None = None
    safety_factor: float | None = None
    round_to: int | None = None


@dataclass(frozen=True)
class RunCaseSpec:
    name: str
    set: JsonObject = field(default_factory=dict)


@dataclass(frozen=True)
class RunsSpec:
    type: str = "single"
    axes: tuple[tuple[str, tuple[Any, ...]], ...] = ()
    cases: tuple[RunCaseSpec, ...] = ()


@dataclass(frozen=True)
class ArtifactStoreSpec:
    strategy: str = "copy"
    fallback_strategy: str | None = None
    verify_digest: bool = True
    fail_on_verify_error: bool = True


@dataclass(frozen=True)
class LauncherSpec:
    type: str = "single"
    options: JsonObject = field(default_factory=dict)


@dataclass(frozen=True)
class StageInputSpec:
    name: str
    source: InputSource
    expects: str
    required: bool = False
    inject: InputInjection = field(default_factory=InputInjection)


@dataclass(frozen=True)
class BeforeStepSpec:
    run: str
    name: str = ""


@dataclass(frozen=True)
class StageSpec:
    name: str
    kind: str
    enabled: bool
    entry: EntrySpec
    resources: ResourceSpec
    launcher: LauncherSpec = field(default_factory=LauncherSpec)
    runtime: str = "default"
    environment: str = ""
    gpu_sizing: StageGpuSizingSpec | None = None
    before: tuple[BeforeStepSpec, ...] = ()
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
class ControllerSpec:
    partition: str | None = None
    cpus: int = 1
    mem: str | None = "2G"
    time_limit: str | None = None
    environment: str = ""


@dataclass(frozen=True)
class OrchestrationSpec:
    controller: ControllerSpec = field(default_factory=ControllerSpec)


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
        order = [name for name in ("train", "eval") if name in stages]
        return tuple(order)

    def with_raw(self, raw: JsonObject, digest: str) -> "ExperimentSpec":
        from .parser import parse_experiment_spec

        return parse_experiment_spec(
            copy.deepcopy(raw),
            config_path=self.config_path,
            project_root=self.project_root,
            forced_digest=digest,
        )
