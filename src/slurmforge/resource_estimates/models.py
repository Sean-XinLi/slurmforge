from __future__ import annotations

from dataclasses import dataclass

from ..io import SchemaVersion
from ..sizing.models import GpuSizingResolution


@dataclass(frozen=True)
class ResourceGroupEstimate:
    group_id: str
    runs: int
    gpus_per_task: int
    array_throttle: int | None
    peak_concurrent_gpus: int
    schema_version: int = SchemaVersion.RESOURCE_ESTIMATE


@dataclass(frozen=True)
class StageResourceEstimate:
    stage_name: str
    runs: int
    max_available_gpus: int
    total_requested_gpus: int
    peak_concurrent_gpus: int
    waves: int
    resource_groups: tuple[ResourceGroupEstimate, ...] = ()
    run_sizing: tuple[GpuSizingResolution, ...] = ()
    warnings: tuple[str, ...] = ()
    schema_version: int = SchemaVersion.RESOURCE_ESTIMATE


@dataclass(frozen=True)
class ExperimentResourceEstimate:
    project: str
    experiment: str
    runs: int
    max_available_gpus: int
    stages: tuple[StageResourceEstimate, ...] = ()
    schema_version: int = SchemaVersion.RESOURCE_ESTIMATE
