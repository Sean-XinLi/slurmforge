from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from ...config.utils import ensure_dict
from ...records import (
    deserialize_run_snapshot,
    load_batch_run_plans,
    load_run_snapshot,
    resolve_dispatch_record_path,
    run_snapshot_path_for_run,
)
from ..models import SourceRef, SourceRunInput


def base_replay_cfg(snapshot: Any) -> dict[str, Any]:
    return copy.deepcopy(ensure_dict(snapshot.replay_spec.replay_cfg, "replay_spec.replay_cfg"))


def load_snapshot_from_file(snapshot_path: Path):
    resolved_path = snapshot_path.expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Replay snapshot does not exist: {resolved_path}")
    payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"Replay snapshot must be a mapping: {resolved_path}")
    return deserialize_run_snapshot(payload)


def guess_batch_root_from_run_dir(run_dir: Path) -> Path | None:
    resolved_run_dir = run_dir.expanduser().resolve()
    parent = resolved_run_dir.parent
    if parent.name != "runs":
        return None
    return parent.parent.resolve()


def resolve_record_path_for_run(
    *,
    batch_root: Path | None,
    run_dir: Path | None,
    run_id: str,
) -> Path | None:
    if batch_root is None or not batch_root.exists():
        return None
    try:
        plans = load_batch_run_plans(batch_root)
    except FileNotFoundError:
        return None

    resolved_run_dir = None if run_dir is None else run_dir.resolve()
    for plan in plans:
        plan_run_dir = Path(plan.run_dir).resolve()
        if resolved_run_dir is not None and plan_run_dir == resolved_run_dir:
            return resolve_dispatch_record_path(batch_root, plan.dispatch)
        if plan.run_id == run_id:
            return resolve_dispatch_record_path(batch_root, plan.dispatch)
    return None


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
    resolved_run_dir = run_dir.expanduser().resolve()
    snapshot = load_run_snapshot(resolved_run_dir)
    snapshot_path = run_snapshot_path_for_run(resolved_run_dir).resolve()
    batch_root = guess_batch_root_from_run_dir(resolved_run_dir)
    record_path = resolve_record_path_for_run(
        batch_root=batch_root,
        run_dir=resolved_run_dir,
        run_id=snapshot.run_id,
    )
    return replay_input_from_snapshot(
        snapshot,
        config_path=snapshot_path,
        config_label=f"replay run {resolved_run_dir}",
        source_batch_root=batch_root,
        source_record_path=record_path,
        selected_index=1,
    )


def load_replay_input_from_snapshot_path(snapshot_path: Path) -> SourceRunInput:
    resolved_snapshot_path = snapshot_path.expanduser().resolve()
    snapshot = load_snapshot_from_file(resolved_snapshot_path)
    run_dir = resolved_snapshot_path.parent.parent
    batch_root = guess_batch_root_from_run_dir(run_dir)
    record_path = resolve_record_path_for_run(
        batch_root=batch_root,
        run_dir=run_dir if batch_root is not None else None,
        run_id=snapshot.run_id,
    )
    return replay_input_from_snapshot(
        snapshot,
        config_path=resolved_snapshot_path,
        config_label=f"replay snapshot {resolved_snapshot_path}",
        source_batch_root=batch_root,
        source_record_path=record_path,
        selected_index=1,
    )
