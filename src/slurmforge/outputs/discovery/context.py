from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...plans.outputs import ArtifactStorePlan
from ...plans.stage import StageInstancePlan


@dataclass(frozen=True)
class OutputDiscoveryContext:
    instance: StageInstancePlan
    workdir: Path
    attempt_id: str
    attempt_dir: Path
    store_plan: ArtifactStorePlan
