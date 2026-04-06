from __future__ import annotations

from dataclasses import dataclass

from ...records.models.run_plan import RunPlan
from ...records.replay_spec.model import ReplaySpec


@dataclass(frozen=True)
class RunPlanAssembly:
    plan: RunPlan
    replay_spec: ReplaySpec
