from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any

from jinja2 import Environment

from ....pipeline.config.normalize.slurm_deps import normalize_dependency_mapping
from ....pipeline.records.batch_paths import bind_run_plan_to_batch, resolve_run_dir
from ....pipeline.records.codecs.run_plan import deserialize_run_plan
from ....pipeline.materialization.array_scripts import (
    render_array_groups,
    render_notify_script,
)
from ....pipeline.materialization.commit import commit_staging
from ....pipeline.materialization.layout import prepare_staging_layout, resolve_materialization_layout
from ....pipeline.materialization.manifest_writer import build_batch_manifest, write_manifest
from ....pipeline.materialization.record_writer import stream_run_records
from ....pipeline.materialization.submit_writer import (
    append_notify_submit_lines as _append_notify_submit_lines,
    build_submit_lines,
    write_submit_script,
)
from ...descriptor import write_storage_descriptor
from ...planning.files import load_batch_run_plans as _load_batch_run_plans, load_run_snapshot as _load_run_snapshot
from ...planning.paths import task_record_path

if TYPE_CHECKING:
    from ....pipeline.materialization.context import MaterializationLayout
    from ....pipeline.records.models.run_plan import RunPlan
    from ....pipeline.records.models.run_snapshot import RunSnapshot
    from ....storage.models import MaterializedBatchBundle


class FileSystemPlanningStore:
    """PlanningStore implementation backed entirely by the filesystem.

    Read I/O goes through ``storage.planning.files`` — not through
    ``pipeline.records.batch_io`` or ``pipeline.records.snapshot_io``.
    """

    def __init__(self, env: Environment) -> None:
        self._env = env

    # ------------------------------------------------------------------
    # Write path — internal staging helpers
    # ------------------------------------------------------------------

    def _write_planning_to_staging(
        self,
        layout: MaterializationLayout,
        bundle: MaterializedBatchBundle,
        *,
        write_planning_files: bool = True,
    ) -> list[dict[str, Any]]:
        normalized_deps = normalize_dependency_mapping(
            bundle.submit_dependencies, field_name="submit_dependencies",
        )
        submit_lines = build_submit_lines(total_runs=bundle.total_runs, notify_cfg=bundle.notify_cfg)
        groups_in_order = stream_run_records(
            bundle.planned_runs, layout=layout, write_planning_files=write_planning_files,
        )
        array_groups_meta = render_array_groups(
            groups_in_order, env=self._env, layout=layout,
            project=bundle.project, experiment_name=bundle.experiment_name,
            notify_cfg=bundle.notify_cfg, submit_dependencies=normalized_deps, submit_lines=submit_lines,
            gpu_budget_plan=bundle.gpu_budget_plan,
        )
        if bundle.notify_cfg is not None and bundle.notify_cfg.enabled:
            render_notify_script(env=self._env, layout=layout, project=bundle.project, experiment_name=bundle.experiment_name)
        _append_notify_submit_lines(
            submit_lines, notify_cfg=bundle.notify_cfg, notify_sbatch=layout.final_notify_sbatch,
            array_log_dir=layout.array_log_dir, project=bundle.project, experiment_name=bundle.experiment_name,
        )
        write_submit_script(submit_lines, layout=layout, group_count=len(array_groups_meta))
        manifest = build_batch_manifest(
            layout=layout, project=bundle.project, experiment_name=bundle.experiment_name,
            batch_name=bundle.batch_name, total_runs=bundle.total_runs,
            array_groups_meta=array_groups_meta, notify_cfg=bundle.notify_cfg,
            submit_dependencies=normalized_deps, manifest_extras=bundle.manifest_extras,
            gpu_budget_plan=bundle.gpu_budget_plan,
        )
        write_manifest(layout, manifest)
        return array_groups_meta

    # ------------------------------------------------------------------
    # Write path — public
    # ------------------------------------------------------------------

    def persist_materialized_batch(self, bundle: MaterializedBatchBundle) -> tuple[dict, ...]:
        layout = resolve_materialization_layout(bundle.batch_root)
        try:
            prepare_staging_layout(layout)
            array_groups_meta = self._write_planning_to_staging(layout, bundle)
            write_storage_descriptor(layout.staging_root, bundle.storage_config, bundle.batch_root)
            commit_staging(layout)
        except Exception:
            shutil.rmtree(layout.staging_root, ignore_errors=True)
            raise
        return tuple(array_groups_meta)

    # ------------------------------------------------------------------
    # Read path — via storage.planning.files
    # ------------------------------------------------------------------

    def load_batch_run_plans(self, batch_root: Path) -> tuple[RunPlan, ...]:
        return tuple(_load_batch_run_plans(batch_root))

    def load_run_snapshot(self, batch_root: Path, run_id: str) -> RunSnapshot | None:
        try:
            plans = _load_batch_run_plans(batch_root)
        except FileNotFoundError:
            return None
        for plan in plans:
            if plan.run_id == run_id:
                run_dir = resolve_run_dir(batch_root, plan)
                try:
                    return _load_run_snapshot(run_dir)
                except FileNotFoundError:
                    return None
        return None

    def load_plan_for_array_task(self, batch_root: Path, group_index: int, task_index: int) -> RunPlan | None:
        record_path = task_record_path(batch_root, group_index, task_index)
        if not record_path.exists():
            return None
        payload = json.loads(record_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return None
        return bind_run_plan_to_batch(batch_root.resolve(), deserialize_run_plan(payload))
