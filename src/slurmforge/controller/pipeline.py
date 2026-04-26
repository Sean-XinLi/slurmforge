from __future__ import annotations

import json
import sys
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import yaml

from ..errors import ConfigContractError
from ..resolver import resolve_stage_inputs_for_pipeline
from ..status import StageStatusRecord, commit_stage_status, read_stage_status
from ..io import SchemaVersion, read_json, write_json
from ..planner import compile_stage_batch
from ..slurm import SlurmClient
from ..spec import parse_experiment_spec, validate_experiment_spec
from ..submission import prepare_stage_submission, read_submission_state, submit_prepared_stage_batch
from ..status import reconcile_stage_batch_with_slurm
from ..status.models import TERMINAL_STATES
from ..storage import (
    collect_stage_statuses,
    controller_events_path,
    controller_state_path,
    iter_stage_run_dirs,
    load_execution_stage_batch_plan,
    load_pipeline_plan,
    load_stage_batch_plan,
    plan_for_run_dir,
    refresh_pipeline_status,
    refresh_stage_batch_status,
    write_controller_status,
    write_selected_stage_batch_layout,
)


def _load_snapshot_yaml(root: Path) -> dict[str, Any]:
    path = root / "spec_snapshot.yaml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"spec_snapshot.yaml must contain a mapping: {path}")
    return payload


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _default_controller_state(plan) -> dict[str, Any]:
    return {
        "schema_version": SchemaVersion.CONTROLLER_STATE,
        "pipeline_id": plan.pipeline_id,
        "state": "ready",
        "current_stage": plan.stage_order[0] if plan.stage_order else None,
        "completed_stages": [],
        "materialized_stages": [],
    }


def _load_controller_state(pipeline_root: Path, plan) -> dict[str, Any]:
    path = controller_state_path(pipeline_root)
    if path.exists():
        return read_json(path)
    state = _default_controller_state(plan)
    write_json(path, state)
    return state


def _save_controller_state(pipeline_root: Path, state: dict[str, Any]) -> None:
    write_json(controller_state_path(pipeline_root), state)


def _record_event(pipeline_root: Path, event: str, **payload: Any) -> None:
    _append_jsonl(controller_events_path(pipeline_root), {"event": event, **payload})


def _batch_terminal(batch_root: Path) -> bool:
    statuses = collect_stage_statuses(batch_root)
    return bool(statuses) and all(status.state in TERMINAL_STATES for status in statuses)


def _wait_terminal(
    batch,
    *,
    client: SlurmClient,
    poll_seconds: int,
    missing_output_grace_seconds: int,
) -> None:
    batch_root = Path(batch.submission_root)
    while True:
        group_job_ids = read_submission_state(batch_root).submitted_group_job_ids
        if not group_job_ids:
            raise ConfigContractError(f"No scheduler job ids recorded in submission ledger for {batch_root}")
        reconcile_stage_batch_with_slurm(
            batch,
            group_job_ids=group_job_ids,
            client=client,
            missing_output_grace_seconds=missing_output_grace_seconds,
        )
        if _batch_terminal(batch_root):
            return
        time.sleep(poll_seconds)


def _project_root_from_pipeline(pipeline_root: Path) -> Path:
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


def _ensure_stage_materialized(pipeline_root: Path, plan, spec, state: dict[str, Any], stage_name: str):
    materialized = set(state.get("materialized_stages") or [])
    stage_root = Path(plan.stage_batches[stage_name].submission_root)
    if stage_name in materialized:
        return load_execution_stage_batch_plan(stage_root)
    resolved = resolve_stage_inputs_for_pipeline(spec, plan, stage_name=stage_name)
    selected_run_ids = {run.run_id for run in resolved.selected_runs}
    blocked = _mark_stage_blocked(
        stage_root,
        selected_run_ids,
        blocked_reasons=resolved.blocked_reasons,
    )
    if not resolved.selected_runs:
        write_json(stage_root / "blocked_runs.json", {"schema_version": SchemaVersion.BLOCKED_RUNS, "run_ids": blocked})
        refresh_stage_batch_status(stage_root)
        refresh_pipeline_status(pipeline_root)
        state["materialized_stages"] = sorted(materialized | {stage_name})
        _save_controller_state(pipeline_root, state)
        _record_event(pipeline_root, "stage_materialized", stage=stage_name, selected_runs=0)
        return load_execution_stage_batch_plan(stage_root)
    batch = compile_stage_batch(
        spec,
        stage_name=stage_name,
        runs=resolved.selected_runs,
        submission_root=stage_root,
        source_ref=f"pipeline:{plan.pipeline_id}:{stage_name}",
        input_bindings_by_run=resolved.input_bindings_by_run,
    )
    batch = replace(batch, batch_id=plan.stage_batches[stage_name].batch_id)
    write_selected_stage_batch_layout(batch, blocked_run_ids=blocked)
    state["materialized_stages"] = sorted(materialized | {stage_name})
    _save_controller_state(pipeline_root, state)
    _record_event(pipeline_root, "stage_materialized", stage=stage_name, selected_runs=len(resolved.selected_runs))
    return batch


def _submit_stage_once(pipeline_root: Path, state: dict[str, Any], batch, *, client: SlurmClient) -> dict[str, str]:
    prepared = prepare_stage_submission(batch)
    group_job_ids = submit_prepared_stage_batch(prepared, client=client, policy="recover_partial")
    state["state"] = "waiting_stage"
    state["current_stage"] = batch.stage_name
    _save_controller_state(pipeline_root, state)
    _record_event(pipeline_root, "stage_submitted", stage=batch.stage_name, job_ids=list(group_job_ids.values()))
    return group_job_ids


def _mark_stage_completed(pipeline_root: Path, state: dict[str, Any], stage_name: str) -> None:
    completed = set(state.get("completed_stages") or [])
    completed.add(stage_name)
    state["completed_stages"] = sorted(completed)
    state["state"] = "stage_complete"
    state["current_stage"] = stage_name
    _save_controller_state(pipeline_root, state)
    _record_event(pipeline_root, "stage_complete", stage=stage_name)


def _write_pipeline_terminal_status(pipeline_root: Path) -> str:
    snapshot = refresh_pipeline_status(pipeline_root)
    if snapshot.pipeline_status is None:
        return "missing"
    return snapshot.pipeline_status.state


def run_controller(
    pipeline_root: Path,
    *,
    client: SlurmClient | None = None,
    poll_seconds: int = 30,
    missing_output_grace_seconds: int = 300,
) -> int:
    slurm = client or SlurmClient()
    plan = load_pipeline_plan(pipeline_root)
    state = _load_controller_state(pipeline_root, plan)
    write_controller_status(pipeline_root, "running")
    try:
        raw = _load_snapshot_yaml(pipeline_root)
        spec = parse_experiment_spec(
            raw,
            config_path=(pipeline_root / "spec_snapshot.yaml").resolve(),
            project_root=_project_root_from_pipeline(pipeline_root),
        )
        validate_experiment_spec(spec)
        for stage_name in plan.stage_order:
            state["current_stage"] = stage_name
            state["state"] = "checking_stage"
            _save_controller_state(pipeline_root, state)
            if stage_name in set(state.get("completed_stages") or []):
                continue
            batch = plan.stage_batches[stage_name]
            stage_spec = spec.enabled_stages[stage_name]
            if stage_spec.depends_on:
                batch = _ensure_stage_materialized(pipeline_root, plan, spec, state, stage_name)
            else:
                batch = load_stage_batch_plan(Path(batch.submission_root))
            if _batch_terminal(Path(batch.submission_root)):
                _mark_stage_completed(pipeline_root, state, stage_name)
                continue
            _submit_stage_once(pipeline_root, state, batch, client=slurm)
            _wait_terminal(
                batch,
                client=slurm,
                poll_seconds=poll_seconds,
                missing_output_grace_seconds=missing_output_grace_seconds,
            )
            _mark_stage_completed(pipeline_root, state, stage_name)
        final_state = _write_pipeline_terminal_status(pipeline_root)
        state["state"] = final_state
        state["current_stage"] = None
        _save_controller_state(pipeline_root, state)
        write_controller_status(pipeline_root, final_state)
        return 0 if final_state == "success" else 1
    except Exception as exc:
        state["state"] = "failed"
        _save_controller_state(pipeline_root, state)
        write_controller_status(pipeline_root, "failed", reason=str(exc))
        _record_event(pipeline_root, "controller_failed", reason=str(exc))
        return 1


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Run a slurmforge pipeline controller")
    parser.add_argument("--pipeline-root", required=True)
    args = parser.parse_args(argv)
    raise SystemExit(run_controller(Path(args.pipeline_root).resolve()))


if __name__ == "__main__":
    main(sys.argv[1:])
