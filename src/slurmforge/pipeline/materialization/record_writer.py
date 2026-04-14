from __future__ import annotations

import io
import json
import os
from dataclasses import replace
from typing import Any, Iterable

from ..config.runtime import ClusterConfig
from ..planning import PlannedRun
from ..records.batch_paths import batch_relative_path
from ..records.codecs.run_plan import serialize_run_plan
from ..records.io_utils import atomic_write_text
from ..records.models.array_assignment import ArrayAssignment
from ..records.models.run_plan import RunPlan
from .context import ArrayGroupState, MaterializationLayout
from .grouping import (
    array_group_signature,
    array_grouping_fields,
    describe_array_group_reason,
    ensure_cluster_renderable,
)
from .layout import map_to_staging
from .run_assets import write_run_metadata


def create_array_group_state(
    plan: RunPlan,
    *,
    layout: MaterializationLayout,
    group_index: int,
    group_signature: str,
    create_dirs: bool = True,
) -> ArrayGroupState:
    records_dir = layout.final_batch_root / "records" / f"group_{group_index:02d}"
    if create_dirs:
        map_to_staging(records_dir, final_root=layout.final_batch_root, staging_root=layout.staging_root).mkdir(
            parents=True, exist_ok=True
        )
    cluster_cfg: ClusterConfig = plan.cluster
    group = ArrayGroupState(
        group_index=group_index,
        cluster=cluster_cfg,
        env=plan.env,
        group_signature=group_signature,
        grouping_fields=array_grouping_fields(cluster_cfg, plan.env),
        group_reason=describe_array_group_reason(),
        array_sbatch=layout.final_sbatch_dir / f"array_group_{group_index:02d}.sbatch.sh",
        records_dir=records_dir,
    )
    ensure_cluster_renderable(group.cluster, context=f"array_group={group.group_index}")
    return group


def write_run_record(
    plan: RunPlan,
    *,
    layout: MaterializationLayout,
    group: ArrayGroupState,
    task_idx: int,
    runs_manifest_fp: Any,
) -> None:
    record_file_final = group.records_dir / f"task_{task_idx:06d}.json"
    dispatch_info = replace(
        plan.dispatch,
        sbatch_path=str(group.array_sbatch),
        sbatch_path_rel=batch_relative_path(layout.final_batch_root, group.array_sbatch),
        record_path=str(record_file_final),
        record_path_rel=batch_relative_path(layout.final_batch_root, record_file_final),
        array_group=group.group_index,
        array_task_index=task_idx,
        array_assignment=ArrayAssignment(
            group_index=group.group_index,
            group_signature=group.group_signature,
            grouping_fields=dict(group.grouping_fields),
            group_reason=group.group_reason,
        ),
    )
    plan_for_record = replace(plan, dispatch=dispatch_info)

    payload = serialize_run_plan(plan_for_record)
    payload_text = json.dumps(payload, indent=2, sort_keys=True)
    record_file_staging = map_to_staging(
        record_file_final,
        final_root=layout.final_batch_root,
        staging_root=layout.staging_root,
    )
    atomic_write_text(record_file_staging, payload_text)
    runs_manifest_fp.write(json.dumps(payload, sort_keys=True) + "\n")


def stream_run_records(
    planned_runs: Iterable[PlannedRun],
    *,
    layout: MaterializationLayout,
    write_files: bool = True,
) -> list[ArrayGroupState]:
    """Compute array groups and optionally write planning files.

    When ``write_files=False`` (pure DB mode), the group computation still
    runs but no task_*.json, runs_manifest.jsonl, or run_snapshot/
    resolved_config files are written.  The caller writes data directly
    to the DB from the in-memory bundle instead.
    """
    group_by_signature: dict[str, ArrayGroupState] = {}
    groups_in_order: list[ArrayGroupState] = []
    runs_manifest_staging = map_to_staging(
        layout.runs_manifest_path,
        final_root=layout.final_batch_root,
        staging_root=layout.staging_root,
    )
    runs_manifest_staging.parent.mkdir(parents=True, exist_ok=True)

    # When not writing files, use a dummy file object to avoid touching disk
    manifest_fp_ctx = (
        runs_manifest_staging.open("w", encoding="utf-8") if write_files
        else io.StringIO()
    )

    with manifest_fp_ctx as runs_manifest_fp:
        for planned_run in planned_runs:
            plan = planned_run.plan
            if write_files:
                write_run_metadata(
                    planned_run,
                    final_batch_root=layout.final_batch_root,
                    staging_root=layout.staging_root,
                )
            group_signature = array_group_signature(plan.cluster, plan.env)
            group = group_by_signature.get(group_signature)
            if group is None:
                group = create_array_group_state(
                    plan,
                    layout=layout,
                    group_index=len(groups_in_order) + 1,
                    group_signature=group_signature,
                    create_dirs=write_files,
                )
                group_by_signature[group_signature] = group
                groups_in_order.append(group)

            task_idx = group.count
            group.count += 1
            group.run_indices.append(plan.run_index)
            if write_files:
                write_run_record(
                    plan,
                    layout=layout,
                    group=group,
                    task_idx=task_idx,
                    runs_manifest_fp=runs_manifest_fp,
                )
        if write_files:
            runs_manifest_fp.flush()
            os.fsync(runs_manifest_fp.fileno())

    # Clean up the empty manifest file if we didn't write to it
    if not write_files and runs_manifest_staging.exists():
        runs_manifest_staging.unlink()

    return groups_in_order
