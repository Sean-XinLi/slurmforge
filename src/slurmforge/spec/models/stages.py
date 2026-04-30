from __future__ import annotations

from dataclasses import dataclass, field

from ...contracts import InputInjection, InputSource
from ...contracts.outputs import StageOutputContract
from .common import JsonObject
from .entry import EntrySpec
from .resources import ResourceSpec
from .sizing import StageGpuSizingSpec


@dataclass(frozen=True)
class LauncherSpec:
    type: str = ""
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
    runtime: str = ""
    environment: str = ""
    gpu_sizing: StageGpuSizingSpec | None = None
    before: tuple[BeforeStepSpec, ...] = ()
    depends_on: tuple[str, ...] = ()
    inputs: dict[str, StageInputSpec] = field(default_factory=dict)
    outputs: StageOutputContract = field(default_factory=StageOutputContract)
