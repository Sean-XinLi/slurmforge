from __future__ import annotations

from dataclasses import dataclass

from ..io import SchemaVersion


@dataclass(frozen=True)
class BudgetDependencyPlan:
    from_groups: tuple[str, ...]
    to_group: str
    type: str
    from_wave: str = ""
    to_wave: str = ""


@dataclass(frozen=True)
class BudgetGroupPlan:
    group_id: str
    gpus_per_task: int
    array_size: int
    array_throttle: int | None
    budgeted_gpus: int | None = None


@dataclass(frozen=True)
class BudgetWaveGroupPlan:
    group_id: str
    gpus_per_task: int
    array_size: int
    array_throttle: int


@dataclass(frozen=True)
class BudgetWavePlan:
    wave_id: str
    groups: tuple[BudgetWaveGroupPlan, ...]
    total_wave_gpus: int


@dataclass(frozen=True)
class BudgetPlan:
    max_available_gpus: int
    overflow_policy: str
    policy_applied: str
    waves: tuple[BudgetWavePlan, ...] = ()
    groups: tuple[BudgetGroupPlan, ...] = ()
    cpu_groups: tuple[str, ...] = ()
    dependencies: tuple[BudgetDependencyPlan, ...] = ()
    warnings: tuple[str, ...] = ()
    schema_version: int = SchemaVersion.BUDGET_PLAN
