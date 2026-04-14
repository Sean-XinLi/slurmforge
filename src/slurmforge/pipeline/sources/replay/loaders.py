from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from ...config.utils import ensure_dict
from ...records import (
    resolve_dispatch_record_path,
)
from ..models import SourceRef, SourceRunInput


def base_replay_cfg(snapshot: Any) -> dict[str, Any]:
    return copy.deepcopy(ensure_dict(snapshot.replay_spec.replay_cfg, "replay_spec.replay_cfg"))


def guess_batch_root_from_run_dir(run_dir: Path) -> Path | None:
    resolved_run_dir = run_dir.expanduser().resolve()
    parent = resolved_run_dir.parent
    if parent.name != "runs":
        return None
    return parent.parent.resolve()


def replay_input_from_snapshot(
    snapshot,
    *,
    config_path: Path | None,
    config_label: str,
    source_batch_root: Path | None,
    source_record_path: Path | None,
    selected_index: int | None = None,
) -> SourceRunInput:
    resolved_batch_root = None if source_batch_root is None else source_batch_root.resolve()
    resolved_record_path = None if source_record_path is None else source_record_path.resolve()
    return SourceRunInput(
        source_kind="replay",
        source_index=int(selected_index or 1),
        run_cfg=base_replay_cfg(snapshot),
        source=SourceRef(
            config_path=None if config_path is None else config_path.resolve(),
            config_label=config_label,
            planning_root=str(snapshot.replay_spec.planning_root or "").strip() or None,
            source_batch_root=resolved_batch_root,
            source_run_id=snapshot.run_id,
            source_record_path=resolved_record_path,
        ),
        sweep_case_name=snapshot.sweep_case_name,
        sweep_assignments=copy.deepcopy(snapshot.sweep_assignments),
        original_run_index=snapshot.run_index,
    )


def load_replay_input_from_run_dir(run_dir: Path) -> SourceRunInput:
    from slurmforge.storage import open_batch_storage

    resolved_run_dir = run_dir.expanduser().resolve()
    batch_root = guess_batch_root_from_run_dir(resolved_run_dir)
    if batch_root is None or not batch_root.exists():
        raise FileNotFoundError(
            f"Replay run is not under a valid batch_root/runs layout: {resolved_run_dir}"
        )

    handle = open_batch_storage(batch_root)
    planning_store = handle.planning
    plans = planning_store.load_batch_run_plans(batch_root)

    matched_plan = None
    for plan in plans:
        if Path(plan.run_dir).resolve() == resolved_run_dir:
            matched_plan = plan
            break
    if matched_plan is None:
        raise FileNotFoundError(f"No RunPlan found for run_dir={resolved_run_dir}")

    snapshot = planning_store.load_run_snapshot(batch_root, matched_plan.run_id)
    if snapshot is None:
        raise FileNotFoundError(f"No snapshot found for run_id={matched_plan.run_id}")

    record_path = resolve_dispatch_record_path(batch_root, matched_plan.dispatch)
    if record_path is not None and not record_path.exists():
        record_path = None
    return replay_input_from_snapshot(
        snapshot,
        config_path=None,
        config_label=f"replay run {resolved_run_dir}",
        source_batch_root=batch_root,
        source_record_path=record_path,
        selected_index=1,
    )
