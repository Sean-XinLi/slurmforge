from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

import yaml

from ..errors import ConfigContractError
from ..notifications import deliver_notification
from ..read_models import load_notification_summary_input
from ..read_models.status import refresh_train_eval_pipeline_status
from ..slurm import SlurmClient
from ..spec import parse_experiment_spec, validate_experiment_spec
from ..submission import prepare_stage_submission, read_submission_state, submit_prepared_stage_batch
from ..status import reconcile_stage_batch_with_slurm
from ..status.models import TERMINAL_STATES
from ..storage.controller import write_controller_status
from ..storage.loader import collect_stage_statuses, load_stage_batch_plan, load_train_eval_pipeline_plan
from .materialization import ensure_stage_materialized, project_root_from_pipeline
from .state import load_controller_state, record_controller_event, save_controller_state


def _load_snapshot_yaml(root: Path) -> dict[str, Any]:
    path = root / "spec_snapshot.yaml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"spec_snapshot.yaml must contain a mapping: {path}")
    return payload


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


def _submit_stage_once(pipeline_root: Path, state: dict[str, Any], batch, *, client: SlurmClient) -> dict[str, str]:
    prepared = prepare_stage_submission(batch)
    group_job_ids = submit_prepared_stage_batch(prepared, client=client, policy="recover_partial")
    state["state"] = "waiting_stage"
    state["current_stage"] = batch.stage_name
    save_controller_state(pipeline_root, state)
    record_controller_event(pipeline_root, "stage_submitted", stage=batch.stage_name, job_ids=list(group_job_ids.values()))
    return group_job_ids


def _mark_stage_completed(pipeline_root: Path, state: dict[str, Any], stage_name: str) -> None:
    completed = set(state.get("completed_stages") or [])
    completed.add(stage_name)
    state["completed_stages"] = sorted(completed)
    state["state"] = "stage_complete"
    state["current_stage"] = stage_name
    save_controller_state(pipeline_root, state)
    record_controller_event(pipeline_root, "stage_complete", stage=stage_name)


def _write_pipeline_terminal_status(pipeline_root: Path) -> str:
    snapshot = refresh_train_eval_pipeline_status(pipeline_root)
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
    plan = load_train_eval_pipeline_plan(pipeline_root)
    state = load_controller_state(pipeline_root, plan)
    write_controller_status(pipeline_root, "running")
    try:
        raw = _load_snapshot_yaml(pipeline_root)
        spec = parse_experiment_spec(
            raw,
            config_path=(pipeline_root / "spec_snapshot.yaml").resolve(),
            project_root=project_root_from_pipeline(pipeline_root),
        )
        validate_experiment_spec(spec)
        for stage_name in plan.stage_order:
            state["current_stage"] = stage_name
            state["state"] = "checking_stage"
            save_controller_state(pipeline_root, state)
            if stage_name in set(state.get("completed_stages") or []):
                continue
            batch = plan.stage_batches[stage_name]
            stage_spec = spec.enabled_stages[stage_name]
            if stage_spec.depends_on:
                batch = ensure_stage_materialized(pipeline_root, plan, spec, state, stage_name)
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
        save_controller_state(pipeline_root, state)
        write_controller_status(pipeline_root, final_state)
        record = deliver_notification(
            pipeline_root,
            event="train_eval_pipeline_finished",
            notification_plan=plan.notification_plan,
            summary_input=load_notification_summary_input(pipeline_root, event="train_eval_pipeline_finished"),
        )
        if record is not None:
            record_controller_event(
                pipeline_root,
                "pipeline_notification",
                notification_event="train_eval_pipeline_finished",
                state=record.state,
                reason=record.reason,
            )
        return 0 if final_state == "success" else 1
    except Exception as exc:
        state["state"] = "failed"
        save_controller_state(pipeline_root, state)
        write_controller_status(pipeline_root, "failed", reason=str(exc))
        record_controller_event(pipeline_root, "controller_failed", reason=str(exc))
        return 1


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Run a slurmforge train/eval pipeline controller")
    parser.add_argument("--train-eval-pipeline-root", required=True)
    args = parser.parse_args(argv)
    raise SystemExit(run_controller(Path(args.train_eval_pipeline_root).resolve()))


if __name__ == "__main__":
    main(sys.argv[1:])
