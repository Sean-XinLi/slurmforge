"""GPU budget planner.

Consumes the final set of planned runs plus batch-wide resources/dispatch
config and produces a ``GpuBudgetPlan``:

    - group runs by ``array_group_signature``
    - compute per-group ``gpus_per_task`` (what Slurm actually requests)
    - allocate array ``%K`` throttle so total concurrent GPU consumption
      respects ``resources.max_available_gpus`` under ``dispatch.group_overflow_policy``

The planner is the single source of truth for throttle / serial chaining /
diagnostics.  Validate, dry-run, and materialize all consume the resulting
``GpuBudgetPlan`` — no one recomputes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Literal

from ...errors import ConfigContractError, InternalCompilerError, PlanningError
from ..config.runtime import DispatchConfig
from .contracts import PlanDiagnostic, serialize_plan_diagnostic
from .dispatch_grouping import array_group_signature


# Default Slurm MaxArraySize (see ``scontrol show config``).  If a throttle
# exceeds this the sbatch file would be rejected by the controller, so we
# clamp and surface a warning.
DEFAULT_SLURM_MAX_ARRAY_SIZE: int = 1001

# All budget-level diagnostics live under this category/stage so the compile
# report's diagnostic pipeline can flow them to validate/dry-run uniformly.
_BUDGET_CATEGORY = "resource"
_BUDGET_STAGE = "batch"
_BUDGET_FIELD_PATH = "dispatch.group_overflow_policy"


PolicyApplied = Literal["shared_budget", "serialized_groups", "best_effort"]


def _budget_warning(code: str, message: str) -> PlanDiagnostic:
    """Build a canonical warning-severity PlanDiagnostic for budget events."""
    return PlanDiagnostic(
        severity="warning",
        category=_BUDGET_CATEGORY,
        code=code,
        message=message,
        stage=_BUDGET_STAGE,
        field_path=_BUDGET_FIELD_PATH,
    )


@dataclass(frozen=True)
class GpuBudgetGroup:
    group_id: int
    task_count: int
    gpus_per_task: int
    throttle: int
    max_group_gpus: int
    limiting_run: str | None
    limiting_model: str | None
    max_estimated_gpus: int
    constrained: bool
    constraint_reason: str | None


@dataclass(frozen=True)
class GpuBudgetPlan:
    max_available_gpus: int
    group_overflow_policy: str
    policy_applied: PolicyApplied
    min_concurrent_gpus: int
    max_planned_concurrent_gpus: int
    strict_global_limit: bool
    groups: tuple[GpuBudgetGroup, ...] = field(default_factory=tuple)
    warnings: tuple[PlanDiagnostic, ...] = field(default_factory=tuple)


# ---------------------------------------------------------------------------
# Intermediate bookkeeping — a pre-throttle view of each group.
# ---------------------------------------------------------------------------


@dataclass
class _RawGroup:
    group_id: int
    signature: str
    run_ids: list[str]
    run_models: list[str]
    run_estimated_gpus: list[int]
    gpus_per_task: int
    first_seen_index: int

    @property
    def task_count(self) -> int:
        return len(self.run_ids)

    @property
    def max_estimated_gpus(self) -> int:
        return max(self.run_estimated_gpus) if self.run_estimated_gpus else 0

    @property
    def limiting_index(self) -> int:
        # Index of the run with the largest recommended_total_gpus; ties broken
        # by first-seen order (stable).
        if not self.run_estimated_gpus:
            return 0
        best = 0
        for i, value in enumerate(self.run_estimated_gpus):
            if value > self.run_estimated_gpus[best]:
                best = i
        return best


def _extract_raw_groups(planned_runs: Iterable) -> list[_RawGroup]:
    groups: dict[str, _RawGroup] = {}
    order: list[str] = []
    for index, planned_run in enumerate(planned_runs):
        plan = planned_run.plan
        signature = array_group_signature(plan.cluster, plan.env)
        stage = plan.train_stage
        allocation = stage.allocation
        gpus_per_task = int(allocation.nodes) * int(allocation.gpus_per_node)
        raw = groups.get(signature)
        if raw is None:
            raw = _RawGroup(
                group_id=0,  # assigned after ordering
                signature=signature,
                run_ids=[],
                run_models=[],
                run_estimated_gpus=[],
                gpus_per_task=gpus_per_task,
                first_seen_index=index,
            )
            groups[signature] = raw
            order.append(signature)
        elif raw.gpus_per_task != gpus_per_task:
            raise InternalCompilerError(
                f"runs in the same array-group signature disagree on gpus_per_task "
                f"({raw.gpus_per_task} vs {gpus_per_task}); grouping keys are inconsistent"
            )
        raw.run_ids.append(plan.run_id)
        raw.run_models.append(plan.model_name)
        raw.run_estimated_gpus.append(int(stage.estimate.recommended_total_gpus))

    ordered: list[_RawGroup] = [groups[sig] for sig in order]
    for idx, raw in enumerate(ordered, start=1):
        raw.group_id = idx
    return ordered


def _validate_group_budget(raw: _RawGroup, max_available_gpus: int) -> None:
    if raw.gpus_per_task <= 0:
        raise PlanningError(
            f"group {raw.group_id}: gpus_per_task={raw.gpus_per_task} (must be > 0); "
            f"this project is GPU-based and does not support CPU-only dispatch"
        )
    if raw.gpus_per_task > max_available_gpus:
        raise ConfigContractError(
            f"group {raw.group_id}: a single task requests {raw.gpus_per_task} GPUs which "
            f"exceeds resources.max_available_gpus={max_available_gpus}; cannot schedule"
        )
    if raw.task_count < 1:
        raise InternalCompilerError(
            f"group {raw.group_id}: task_count={raw.task_count} (must be >= 1)"
        )


def _single_group_throttle(gpus_per_task: int, task_count: int, budget: int) -> int:
    """Max throttle for one group fitting independently into ``budget`` GPUs."""
    return max(1, min(task_count, budget // gpus_per_task))


def _allocate_shared_budget(raw_groups: list[_RawGroup], max_available_gpus: int) -> dict[int, int]:
    """Allocate throttles so that total concurrent demand <= max_available_gpus.

    Ordering rule (from spec):
      1. Every group starts at throttle=1.
      2. Distribute remaining GPUs to groups in order of ascending
         gpus_per_task; ties broken by descending task_count (more tasks
         benefits more from throttle).
      3. A group's throttle is capped at task_count.
    """
    throttles: dict[int, int] = {raw.group_id: 1 for raw in raw_groups}
    used = sum(raw.gpus_per_task for raw in raw_groups)

    # Groups eligible for additional throttle — smallest gpus_per_task first,
    # then most tasks first.  Both ordering keys are stable so behavior is
    # deterministic across Python versions.
    candidates = sorted(
        raw_groups,
        key=lambda g: (g.gpus_per_task, -g.task_count, g.group_id),
    )

    progress = True
    while progress:
        progress = False
        for raw in candidates:
            if throttles[raw.group_id] >= raw.task_count:
                continue
            if used + raw.gpus_per_task > max_available_gpus:
                continue
            throttles[raw.group_id] += 1
            used += raw.gpus_per_task
            progress = True
    return throttles


def _clamp_to_max_array_size(
    throttles: dict[int, int],
    raw_groups: list[_RawGroup],
    slurm_max_array_size: int,
) -> list[PlanDiagnostic]:
    """Clamp per-group throttle to Slurm's MaxArraySize.

    Also clamps to ``task_count`` (defense-in-depth; shared-budget allocator
    already respects this).  Returns any warnings issued by the clamp.
    """
    warnings: list[PlanDiagnostic] = []
    for raw in raw_groups:
        original = throttles[raw.group_id]
        clamped = min(original, raw.task_count, slurm_max_array_size)
        if clamped < 1:
            clamped = 1
        if clamped != original:
            if original > slurm_max_array_size:
                warnings.append(
                    _budget_warning(
                        code="throttle_clamped_by_max_array_size",
                        message=(
                            f"group {raw.group_id} throttle clamped from {original} "
                            f"to {clamped} (Slurm MaxArraySize={slurm_max_array_size})"
                        ),
                    )
                )
        throttles[raw.group_id] = clamped
    return warnings


def _build_group(raw: _RawGroup, throttle: int, reason: str | None) -> GpuBudgetGroup:
    if raw.run_ids:
        idx = raw.limiting_index
        limiting_run = raw.run_ids[idx]
        limiting_model = raw.run_models[idx]
    else:
        limiting_run = None
        limiting_model = None
    constrained = throttle < raw.task_count
    return GpuBudgetGroup(
        group_id=raw.group_id,
        task_count=raw.task_count,
        gpus_per_task=raw.gpus_per_task,
        throttle=throttle,
        max_group_gpus=throttle * raw.gpus_per_task,
        limiting_run=limiting_run,
        limiting_model=limiting_model,
        max_estimated_gpus=raw.max_estimated_gpus,
        constrained=constrained,
        constraint_reason=reason if constrained else None,
    )


def plan_gpu_budget(
    planned_runs: Iterable,
    *,
    max_available_gpus: int,
    dispatch_cfg: DispatchConfig,
    slurm_max_array_size: int = DEFAULT_SLURM_MAX_ARRAY_SIZE,
) -> GpuBudgetPlan:
    """Compute the GPU budget plan for a batch.

    The planner consumes ONLY batch-scoped inputs: the GPU budget ceiling
    and the overflow policy.  Per-run resource knobs (``max_gpus_per_job``,
    ``auto_gpu``, estimator profile) must NOT be passed here — they are
    run-scoped and have already been applied when building each
    ``PlannedRun.plan.train_stage``.

    Raises
    ------
    ConfigContractError
        - a single task's gpus_per_task exceeds ``max_available_gpus``
        - policy=error and the minimum concurrent demand exceeds the budget
    PlanningError
        - gpus_per_task <= 0 (project is GPU-based)
    """
    raw_groups = _extract_raw_groups(planned_runs)
    max_available_gpus = int(max_available_gpus)
    if max_available_gpus < 1:
        raise ConfigContractError("resources.max_available_gpus must be >= 1")

    # If there are no runs there is no plan to produce; callers should not
    # invoke the planner in that situation.
    if not raw_groups:
        raise InternalCompilerError("plan_gpu_budget requires at least one planned run")

    for raw in raw_groups:
        _validate_group_budget(raw, max_available_gpus)

    policy = dispatch_cfg.group_overflow_policy
    min_concurrent_gpus = sum(raw.gpus_per_task for raw in raw_groups)
    warnings: list[PlanDiagnostic] = []

    fits_shared = min_concurrent_gpus <= max_available_gpus
    is_multi_group = len(raw_groups) >= 2

    if fits_shared:
        policy_applied: PolicyApplied = "shared_budget"
        throttles = _allocate_shared_budget(raw_groups, max_available_gpus)
        constraint_reason = "budget_exhausted"
    elif policy == "error":
        raise ConfigContractError(
            f"dispatch.group_overflow_policy=error: minimum concurrent GPU demand "
            f"{min_concurrent_gpus} exceeds resources.max_available_gpus={max_available_gpus} "
            f"across {len(raw_groups)} array group(s); raise max_available_gpus or change policy"
        )
    elif policy == "serial":
        # Fewer than 2 groups cannot be "serialized", fall back cleanly.
        if is_multi_group:
            policy_applied = "serialized_groups"
        else:
            policy_applied = "shared_budget"
        throttles = {
            raw.group_id: _single_group_throttle(
                raw.gpus_per_task, raw.task_count, max_available_gpus
            )
            for raw in raw_groups
        }
        constraint_reason = "budget_exhausted"
        if policy_applied == "serialized_groups" and len(raw_groups) >= 5:
            warnings.append(
                _budget_warning(
                    code="serial_chain_long",
                    message=(
                        f"serial policy chains {len(raw_groups)} groups; pending earlier "
                        f"groups can delay later groups"
                    ),
                )
            )
    elif policy == "best_effort":
        policy_applied = "best_effort"
        throttles = {
            raw.group_id: _single_group_throttle(
                raw.gpus_per_task, raw.task_count, max_available_gpus
            )
            for raw in raw_groups
        }
        constraint_reason = "budget_exhausted"
        warnings.append(
            _budget_warning(
                code="best_effort_no_strict_global_limit",
                message=(
                    "best_effort does not guarantee a strict global GPU limit "
                    "across multiple Slurm array groups"
                ),
            )
        )
    else:
        raise ConfigContractError(
            f"dispatch.group_overflow_policy must be one of: error, serial, best_effort "
            f"(got {policy!r})"
        )

    warnings.extend(_clamp_to_max_array_size(throttles, raw_groups, slurm_max_array_size))

    groups: list[GpuBudgetGroup] = []
    for raw in raw_groups:
        throttle = throttles[raw.group_id]
        reason = constraint_reason if throttle < raw.task_count else None
        groups.append(_build_group(raw, throttle, reason))

    if policy_applied == "shared_budget":
        max_planned = sum(g.max_group_gpus for g in groups)
    elif policy_applied == "serialized_groups":
        max_planned = max((g.max_group_gpus for g in groups), default=0)
    else:  # best_effort
        max_planned = sum(g.max_group_gpus for g in groups)

    strict_global_limit = max_planned <= max_available_gpus

    return GpuBudgetPlan(
        max_available_gpus=max_available_gpus,
        group_overflow_policy=policy,
        policy_applied=policy_applied,
        min_concurrent_gpus=min_concurrent_gpus,
        max_planned_concurrent_gpus=max_planned,
        strict_global_limit=strict_global_limit,
        groups=tuple(groups),
        warnings=tuple(warnings),
    )


def serialize_gpu_budget_group(group: GpuBudgetGroup) -> dict:
    return {
        "group_id": int(group.group_id),
        "task_count": int(group.task_count),
        "gpus_per_task": int(group.gpus_per_task),
        "throttle": int(group.throttle),
        "max_group_gpus": int(group.max_group_gpus),
        "limiting_run": group.limiting_run,
        "limiting_model": group.limiting_model,
        "max_estimated_gpus": int(group.max_estimated_gpus),
        "constrained": bool(group.constrained),
        "constraint_reason": group.constraint_reason,
    }


def serialize_gpu_budget_plan(plan: GpuBudgetPlan) -> dict:
    return {
        "max_available_gpus": int(plan.max_available_gpus),
        "group_overflow_policy": str(plan.group_overflow_policy),
        "policy_applied": str(plan.policy_applied),
        "min_concurrent_gpus": int(plan.min_concurrent_gpus),
        "max_planned_concurrent_gpus": int(plan.max_planned_concurrent_gpus),
        "strict_global_limit": bool(plan.strict_global_limit),
        "groups": [serialize_gpu_budget_group(g) for g in plan.groups],
        "warnings": [serialize_plan_diagnostic(w) for w in plan.warnings],
    }
