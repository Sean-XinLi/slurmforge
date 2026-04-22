from __future__ import annotations

import json
from typing import Any

from ...identity import PACKAGE_NAME, __version__
from ..config.runtime import NotifyConfig, serialize_notify_config
from ..planning import GpuBudgetPlan, serialize_gpu_budget_plan
from ..records.io_utils import atomic_write_text
from .context import MaterializationLayout
from .grouping import summarize_resource_buckets
from .layout import map_to_staging


def build_batch_manifest(
    *,
    layout: MaterializationLayout,
    project: str,
    experiment_name: str,
    batch_name: str,
    total_runs: int,
    array_groups_meta: list[dict[str, Any]],
    notify_cfg: NotifyConfig | None,
    submit_dependencies: dict[str, list[str]],
    manifest_extras: dict[str, Any] | None,
    gpu_budget_plan: GpuBudgetPlan | None = None,
) -> dict[str, Any]:
    manifest = {
        "generated_by": {
            "name": PACKAGE_NAME,
            "version": __version__,
        },
        "project": project,
        "experiment_name": experiment_name,
        "batch_name": batch_name,
        "dispatch_mode": "array",
        "total_runs": total_runs,
        "array_group_count": len(array_groups_meta),
        "batch_root": str(layout.final_batch_root),
        "sbatch_dir": str(layout.final_sbatch_dir),
        "submit_script": str(layout.submit_script),
        "resource_buckets": summarize_resource_buckets(array_groups_meta),
        "array_groups": array_groups_meta,
        "runs_manifest": str(layout.runs_manifest_path),
        "notify": None if notify_cfg is None else serialize_notify_config(notify_cfg),
        "submit_dependencies": submit_dependencies,
        "gpu_budget_plan": None if gpu_budget_plan is None else serialize_gpu_budget_plan(gpu_budget_plan),
    }
    if manifest_extras:
        manifest.update(manifest_extras)
    return manifest


def write_manifest(layout: MaterializationLayout, manifest: dict[str, Any]) -> None:
    manifest_staging = map_to_staging(
        layout.manifest_path,
        final_root=layout.final_batch_root,
        staging_root=layout.staging_root,
    )
    atomic_write_text(manifest_staging, json.dumps(manifest, indent=2, sort_keys=True))
