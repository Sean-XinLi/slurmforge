from __future__ import annotations

from pathlib import Path
from typing import Any

from ..io import SchemaVersion, write_json
from ..materialization.stage_batch import (
    materialize_selected_stage_batch,
    materialize_stage_batch,
)
from ..planner.stage_batch import compile_stage_batch
from ..plans.train_eval import EVAL_SHARD_GATE, TRAIN_GROUP_GATE
from ..resolver.train_eval_pipeline import resolve_stage_inputs_for_train_eval_pipeline
from ..root_model.snapshots import (
    refresh_stage_batch_status,
    refresh_train_eval_pipeline_status,
)
from ..slurm import SlurmClientProtocol
from ..spec import load_experiment_spec_from_snapshot
from ..status.machine import commit_stage_status
from ..status.models import StageStatusRecord, TERMINAL_STATES
from ..status.reader import read_stage_status
from ..storage.execution_index import upsert_execution_batch
from ..storage.plan_reader import (
    load_execution_stage_batch_plan,
    run_definitions_from_stage_batch,
)
from ..submission.reconcile import reconcile_batch_submission
from .gate_ledger import gate_ledger_key
from .project import project_root_from_pipeline
from .state import record_workflow_event, save_workflow_state
from .state_model import EVAL_STAGE, TRAIN_STAGE, train_groups
from .train_group import group_plan


def eval_shard_root(plan, group_id: str) -> Path:
    return Path(plan.root_dir) / "stage_batches" / EVAL_STAGE / "shards" / group_id


def group_run_definitions(plan, group_id: str):
    train_group = group_plan(plan.stage_batches[TRAIN_STAGE], group_id)
    run_ids = set(train_group.run_ids)
    return tuple(
        run
        for run in run_definitions_from_stage_batch(plan.stage_batches[EVAL_STAGE])
        if run.run_id in run_ids
    )


def mark_blocked_eval_runs(
    shard_batch,
    blocked_reasons: dict[str, str],
    *,
    selected_run_ids: set[str],
) -> list[str]:
    blocked: list[str] = []
    root = Path(shard_batch.submission_root)
    instances_by_run = {
        instance.run_id: instance for instance in shard_batch.stage_instances
    }
    for run_id, instance in instances_by_run.items():
        if run_id in selected_run_ids:
            continue
        blocked.append(run_id)
        run_dir = root / instance.run_dir_rel
        current = read_stage_status(run_dir)
        if current is not None and current.state in TERMINAL_STATES:
            continue
        commit_stage_status(
            run_dir,
            StageStatusRecord(
                schema_version=SchemaVersion.STATUS,
                stage_instance_id=instance.stage_instance_id,
                run_id=instance.run_id,
                stage_name=instance.stage_name,
                state="blocked",
                failure_class="upstream_output_unavailable",
                reason=blocked_reasons.get(run_id)
                or "required upstream stage output was not available",
            ),
            source="pipeline_gate",
        )
    return sorted(blocked)


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
    upsert_execution_batch(
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


def reconcile_eval_shard(
    pipeline_root: Path,
    shard_root: Path,
    *,
    client: SlurmClientProtocol,
    missing_output_grace_seconds: int,
) -> None:
    reconcile_batch_submission(
        shard_root,
        client=client,
        missing_output_grace_seconds=missing_output_grace_seconds,
    )
    refresh_stage_batch_status(shard_root)
    refresh_train_eval_pipeline_status(pipeline_root)
