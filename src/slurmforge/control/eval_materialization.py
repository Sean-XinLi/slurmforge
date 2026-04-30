from __future__ import annotations

from pathlib import Path
from typing import Any

from ..io import SchemaVersion, write_json
from ..materialization.stage_batch import (
    materialize_selected_stage_batch,
    materialize_stage_batch,
)
from ..planner.stage_batch import compile_stage_batch
from ..plans.train_eval import TRAIN_GROUP_GATE
from ..resolver.train_eval_pipeline import resolve_stage_inputs_for_train_eval_pipeline
from ..root_model.snapshots import (
    refresh_stage_batch_status,
    refresh_train_eval_pipeline_status,
)
from ..spec import load_experiment_spec_from_snapshot
from ..storage.plan_reader import load_execution_stage_batch_plan
from ..storage.runtime_batches import upsert_runtime_batch
from .eval_blocking import mark_blocked_eval_runs
from .eval_selection import eval_shard_root, group_run_definitions
from .gate_ledger import gate_ledger_key
from .project import project_root_from_pipeline
from .state import record_workflow_event, save_workflow_state
from .state_model import EVAL_STAGE, train_groups


def ensure_eval_shard_materialized(
    pipeline_root: Path,
    plan,
    state: dict[str, Any],
    group_id: str,
):
    record = train_groups(state)[group_id]
    shard_root = eval_shard_root(plan, group_id)
    record["eval_shard_root"] = str(shard_root)
    spec = load_experiment_spec_from_snapshot(
        pipeline_root,
        project_root=project_root_from_pipeline(pipeline_root),
    )
    runs = group_run_definitions(plan, group_id)
    batch_id = f"{plan.pipeline_id}_eval_{group_id}"
    source_ref = f"train_eval_pipeline:{plan.pipeline_id}:eval:{group_id}"
    full_batch = compile_stage_batch(
        spec,
        stage_name=EVAL_STAGE,
        runs=runs,
        submission_root=shard_root,
        source_ref=source_ref,
        batch_id=batch_id,
    )
    if not (shard_root / "manifest.json").exists():
        materialize_stage_batch(
            full_batch, spec_snapshot=spec.raw, pipeline_root=pipeline_root
        )
    upsert_runtime_batch(
        pipeline_root,
        full_batch,
        role="eval_shard",
        shard_id=group_id,
        source_train_group_id=group_id,
    )

    resolved = resolve_stage_inputs_for_train_eval_pipeline(
        spec,
        plan,
        stage_name=EVAL_STAGE,
        runs=runs,
    )
    selected_run_ids = {run.run_id for run in resolved.selected_runs}
    blocked = mark_blocked_eval_runs(
        full_batch,
        resolved.blocked_reasons,
        selected_run_ids=selected_run_ids,
    )
    write_json(
        shard_root / "blocked_runs.json",
        {"schema_version": SchemaVersion.BLOCKED_RUNS, "run_ids": blocked},
    )
    refresh_stage_batch_status(shard_root)
    refresh_train_eval_pipeline_status(pipeline_root)

    if not resolved.selected_runs:
        record["state"] = "eval_skipped"
        record["terminal_dependency_gate_key"] = gate_ledger_key(
            TRAIN_GROUP_GATE, group_id=group_id
        )
        save_workflow_state(pipeline_root, state)
        record_workflow_event(
            pipeline_root,
            "eval_shard_skipped",
            group_id=group_id,
            blocked_run_ids=blocked,
        )
        return None

    if (shard_root / "selected_batch_plan.json").exists():
        return load_execution_stage_batch_plan(shard_root)

    selected_batch = compile_stage_batch(
        spec,
        stage_name=EVAL_STAGE,
        runs=resolved.selected_runs,
        submission_root=shard_root,
        source_ref=source_ref,
        input_bindings_by_run=resolved.input_bindings_by_run,
        batch_id=batch_id,
    )
    materialize_selected_stage_batch(
        selected_batch,
        blocked_run_ids=blocked,
        pipeline_root=pipeline_root,
    )
    record["state"] = "eval_materialized"
    save_workflow_state(pipeline_root, state)
    record_workflow_event(
        pipeline_root,
        "eval_shard_materialized",
        group_id=group_id,
        selected_runs=len(resolved.selected_runs),
        blocked_run_ids=blocked,
    )
    return selected_batch
