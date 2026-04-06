from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from ...errors import ConfigContractError
from .models.dispatch import DispatchInfo
from .models.run_plan import RunPlan


def batch_relative_path(batch_root: Path, path: Path | str | None) -> str | None:
    if path in {None, ""}:
        return None
    candidate = Path(path).expanduser()
    try:
        return candidate.resolve().relative_to(batch_root.resolve()).as_posix()
    except ValueError:
        return None


def resolve_batch_internal_path(
    batch_root: Path,
    *,
    rel_path: str | None,
    field_name: str,
) -> Path | None:
    resolved_root = batch_root.resolve()
    if rel_path in {None, ""}:
        return None
    candidate = Path(str(rel_path)).expanduser()
    if candidate.is_absolute():
        raise ConfigContractError(f"{field_name} must be batch-relative, got absolute path: {candidate}")
    return (resolved_root / candidate).resolve()


def resolve_run_dir(batch_root: Path, plan: RunPlan) -> Path:
    resolved = resolve_batch_internal_path(
        batch_root,
        rel_path=plan.run_dir_rel,
        field_name="run_dir_rel",
    )
    if resolved is None:
        raise ConfigContractError(f"RunPlan {plan.run_id} is missing required run_dir_rel")
    return resolved


def resolve_dispatch_record_path(batch_root: Path, dispatch: DispatchInfo) -> Path | None:
    return resolve_batch_internal_path(
        batch_root,
        rel_path=dispatch.record_path_rel,
        field_name="dispatch.record_path_rel",
    )


def resolve_dispatch_sbatch_path(batch_root: Path, dispatch: DispatchInfo) -> Path | None:
    return resolve_batch_internal_path(
        batch_root,
        rel_path=dispatch.sbatch_path_rel,
        field_name="dispatch.sbatch_path_rel",
    )


def bind_run_plan_to_batch(batch_root: Path, plan: RunPlan) -> RunPlan:
    resolved_sbatch_path = resolve_dispatch_sbatch_path(batch_root, plan.dispatch)
    resolved_record_path = resolve_dispatch_record_path(batch_root, plan.dispatch)
    resolved_dispatch = replace(
        plan.dispatch,
        sbatch_path="" if resolved_sbatch_path is None else str(resolved_sbatch_path),
        record_path=None if resolved_record_path is None else str(resolved_record_path),
    )
    return replace(
        plan,
        run_dir=str(resolve_run_dir(batch_root, plan)),
        dispatch=resolved_dispatch,
    )


def batch_root_from_record_path(record_path: Path) -> Path:
    resolved = record_path.resolve()
    for parent in resolved.parents:
        if parent.name == "records":
            return parent.parent
    raise ConfigContractError(f"record_path is not inside a batch records/ directory: {record_path}")
