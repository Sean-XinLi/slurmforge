from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .batch import PlannedBatch, PlannedRun
from .batch_validator import validate_planned_batch_runs
from .fingerprint import run_id, user_run_id_payload
from .gpu_budget import (
    GpuBudgetGroup,
    GpuBudgetPlan,
    plan_gpu_budget,
    serialize_gpu_budget_plan,
)
from .identity import BatchIdentity, build_batch_identity

if TYPE_CHECKING:
    from .run.assembly import RunPlanAssembly


def build_run_plan(*args: Any, **kwargs: Any) -> "RunPlanAssembly":
    from .run import build_run_plan as _build_run_plan

    return _build_run_plan(*args, **kwargs)


def build_planned_run(*args: Any, **kwargs: Any) -> PlannedRun:
    from .run import build_planned_run as _build_planned_run

    return _build_planned_run(*args, **kwargs)

__all__ = [
    "BatchIdentity",
    "GpuBudgetGroup",
    "GpuBudgetPlan",
    "PlannedBatch",
    "PlannedRun",
    "build_batch_identity",
    "build_planned_run",
    "build_run_plan",
    "plan_gpu_budget",
    "run_id",
    "serialize_gpu_budget_plan",
    "user_run_id_payload",
    "validate_planned_batch_runs",
]
