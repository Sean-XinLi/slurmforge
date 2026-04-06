from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..config.runtime import ClusterConfig, EnvConfig


@dataclass(frozen=True)
class MaterializationResult:
    submit_script: Path
    manifest_path: Path
    array_groups_meta: tuple[dict[str, Any], ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "submit_script", Path(self.submit_script))
        object.__setattr__(self, "manifest_path", Path(self.manifest_path))
        object.__setattr__(self, "array_groups_meta", tuple(self.array_groups_meta))


@dataclass
class ArrayGroupState:
    group_index: int
    cluster: ClusterConfig
    env: EnvConfig
    group_signature: str
    grouping_fields: dict[str, Any]
    group_reason: str
    array_sbatch: Path
    records_dir: Path
    count: int = 0
    run_indices: list[int] = field(default_factory=list)


@dataclass(frozen=True)
class MaterializationLayout:
    final_batch_root: Path
    final_sbatch_dir: Path
    final_notify_sbatch: Path
    staging_root: Path
    submit_script: Path
    manifest_path: Path
    runs_manifest_path: Path
    array_log_dir: Path
