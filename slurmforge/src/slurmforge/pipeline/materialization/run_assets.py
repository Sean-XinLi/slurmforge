from __future__ import annotations

import json
from pathlib import Path

import yaml

from ..planning import PlannedRun
from ..records.io_utils import atomic_write_text
from ..records.batch_paths import resolve_run_dir
from ..records.codecs.run_snapshot import serialize_run_snapshot
from ..records.snapshot_io import run_snapshot_path_for_run
from .layout import map_to_staging


def write_run_metadata(
    planned_run: PlannedRun,
    *,
    final_batch_root: Path,
    staging_root: Path,
) -> Path:
    plan = planned_run.plan
    snapshot = planned_run.snapshot
    final_run_dir = resolve_run_dir(final_batch_root, plan)
    run_dir = map_to_staging(final_run_dir, final_root=final_batch_root, staging_root=staging_root)
    run_dir.mkdir(parents=True, exist_ok=True)
    meta_dir = run_dir / "meta"
    meta_dir.mkdir(exist_ok=True)
    atomic_write_text(run_dir / "resolved_config.yaml", yaml.safe_dump(snapshot.replay_spec.replay_cfg, sort_keys=False))
    final_snapshot_path = run_snapshot_path_for_run(final_run_dir)
    snapshot_staging = map_to_staging(
        final_snapshot_path,
        final_root=final_batch_root,
        staging_root=staging_root,
    )
    atomic_write_text(snapshot_staging, json.dumps(serialize_run_snapshot(snapshot), indent=2, sort_keys=True))
    return run_dir
