from __future__ import annotations

import shutil

from jinja2 import Environment

from ...errors import InternalCompilerError
from ..planning import PlannedBatch
from ..config.normalize.slurm_deps import normalize_dependency_mapping
from .array_scripts import render_array_groups, render_notify_script
from .commit import commit_staging
from .context import MaterializationResult
from .layout import prepare_staging_layout, resolve_materialization_layout
from .manifest_writer import build_batch_manifest, write_manifest
from .record_writer import stream_run_records
from .submit_writer import append_notify_submit_lines, build_submit_lines, write_submit_script


def materialize_batch(
    *,
    planned_batch: PlannedBatch,
    env: Environment,
) -> MaterializationResult:
    planned_runs = tuple(planned_batch.planned_runs)
    if not planned_runs:
        raise InternalCompilerError("materialize_batch requires at least one planned run")

    layout = resolve_materialization_layout(planned_batch.batch_root)
    try:
        prepare_staging_layout(layout)
        normalized_submit_dependencies = normalize_dependency_mapping(
            planned_batch.submit_dependencies,
            field_name="submit_dependencies",
        )
        submit_lines = build_submit_lines(
            total_runs=planned_batch.total_runs,
            notify_cfg=planned_batch.notify_cfg,
        )
        groups_in_order = stream_run_records(planned_runs, layout=layout)
        array_groups_meta = render_array_groups(
            groups_in_order,
            env=env,
            layout=layout,
            project=planned_batch.project,
            experiment_name=planned_batch.experiment_name,
            notify_cfg=planned_batch.notify_cfg,
            submit_dependencies=normalized_submit_dependencies,
            submit_lines=submit_lines,
        )
        if planned_batch.notify_cfg is not None and planned_batch.notify_cfg.enabled:
            render_notify_script(
                env=env,
                layout=layout,
                project=planned_batch.project,
                experiment_name=planned_batch.experiment_name,
            )
        append_notify_submit_lines(
            submit_lines,
            notify_cfg=planned_batch.notify_cfg,
            notify_sbatch=layout.final_notify_sbatch,
            array_log_dir=layout.array_log_dir,
            project=planned_batch.project,
            experiment_name=planned_batch.experiment_name,
        )
        write_submit_script(submit_lines, layout=layout, group_count=len(array_groups_meta))
        manifest = build_batch_manifest(
            layout=layout,
            project=planned_batch.project,
            experiment_name=planned_batch.experiment_name,
            batch_name=planned_batch.batch_name,
            total_runs=planned_batch.total_runs,
            array_groups_meta=array_groups_meta,
            notify_cfg=planned_batch.notify_cfg,
            submit_dependencies=normalized_submit_dependencies,
            manifest_extras=planned_batch.manifest_extras,
        )
        write_manifest(layout, manifest)
        commit_staging(layout)
    except Exception:
        shutil.rmtree(layout.staging_root, ignore_errors=True)
        raise

    return MaterializationResult(
        submit_script=layout.submit_script,
        manifest_path=layout.manifest_path,
        array_groups_meta=tuple(array_groups_meta),
    )
