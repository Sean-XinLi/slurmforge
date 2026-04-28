from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

from ..io import SchemaVersion, write_json
from ..planner import compile_stage_batch
from ..resolver import resolve_stage_inputs_for_train_eval_pipeline
from ..root_model import iter_stage_run_dirs, refresh_stage_batch_status, refresh_train_eval_pipeline_status
from ..status import StageStatusRecord, commit_stage_status, read_stage_status
from ..status.models import TERMINAL_STATES
from ..storage.batch_layout import write_selected_stage_batch_layout
from ..storage.loader import load_execution_stage_batch_plan, plan_for_run_dir
from .state import record_controller_event, save_controller_state


def project_root_from_pipeline(pipeline_root: Path) -> Path:
    for run_dir in iter_stage_run_dirs(pipeline_root):
        plan = plan_for_run_dir(run_dir)
        if plan is not None and plan.lineage.get("project_root"):
            return Path(str(plan.lineage["project_root"])).resolve()
    return pipeline_root


def _mark_stage_blocked(
    stage_root: Path,
    selected_run_ids: set[str],
    *,
    blocked_reasons: dict[str, str],
) -> list[str]:
    blocked_run_ids: list[str] = []
    for run_dir in iter_stage_run_dirs(stage_root):
        plan = plan_for_run_dir(run_dir)
        if plan is None or plan.run_id in selected_run_ids:
            continue
        blocked_run_ids.append(plan.run_id)
        status = read_stage_status(run_dir)
        if status is not None and status.state in TERMINAL_STATES:
            continue
        commit_stage_status(
            run_dir,
            StageStatusRecord(
                schema_version=SchemaVersion.STATUS,
                stage_instance_id=plan.stage_instance_id,
                run_id=plan.run_id,
                stage_name=plan.stage_name,
                state="blocked",
                reason=blocked_reasons.get(plan.run_id) or "required upstream stage output was not available",
            ),
            source="controller",
        )
    return sorted(blocked_run_ids)


def ensure_stage_materialized(pipeline_root: Path, plan, spec, state: dict[str, Any], stage_name: str):
    materialized = set(state.get("materialized_stages") or [])
    stage_root = Path(plan.stage_batches[stage_name].submission_root)
    if stage_name in materialized:
        return load_execution_stage_batch_plan(stage_root)
    resolved = resolve_stage_inputs_for_train_eval_pipeline(spec, plan, stage_name=stage_name)
    selected_run_ids = {run.run_id for run in resolved.selected_runs}
    blocked = _mark_stage_blocked(
        stage_root,
        selected_run_ids,
        blocked_reasons=resolved.blocked_reasons,
    )
    if not resolved.selected_runs:
        write_json(stage_root / "blocked_runs.json", {"schema_version": SchemaVersion.BLOCKED_RUNS, "run_ids": blocked})
        refresh_stage_batch_status(stage_root)
        refresh_train_eval_pipeline_status(pipeline_root)
        state["materialized_stages"] = sorted(materialized | {stage_name})
        save_controller_state(pipeline_root, state)
        record_controller_event(pipeline_root, "stage_materialized", stage=stage_name, selected_runs=0)
        return load_execution_stage_batch_plan(stage_root)
    batch = compile_stage_batch(
        spec,
        stage_name=stage_name,
        runs=resolved.selected_runs,
        submission_root=stage_root,
        source_ref=f"train_eval_pipeline:{plan.pipeline_id}:{stage_name}",
        input_bindings_by_run=resolved.input_bindings_by_run,
    )
    batch = replace(batch, batch_id=plan.stage_batches[stage_name].batch_id)
    write_selected_stage_batch_layout(batch, blocked_run_ids=blocked)
    refresh_stage_batch_status(stage_root)
    refresh_train_eval_pipeline_status(pipeline_root)
    state["materialized_stages"] = sorted(materialized | {stage_name})
    save_controller_state(pipeline_root, state)
    record_controller_event(pipeline_root, "stage_materialized", stage=stage_name, selected_runs=len(resolved.selected_runs))
    return batch
