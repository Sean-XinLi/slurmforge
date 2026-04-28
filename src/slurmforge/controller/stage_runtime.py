from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from ..errors import ConfigContractError
from ..root_model.runs import collect_stage_statuses
from ..slurm import SlurmClientProtocol
from ..status.models import TERMINAL_STATES
from ..status.reconcile import reconcile_stage_batch_with_slurm
from ..submission.generation import prepare_stage_submission
from ..submission.state import read_submission_state
from ..submission.submitter import submit_prepared_stage_batch
from .state import record_controller_event, save_controller_state


def batch_terminal(batch_root: Path) -> bool:
    statuses = collect_stage_statuses(batch_root)
    return bool(statuses) and all(
        status.state in TERMINAL_STATES for status in statuses
    )


def submit_stage_once(
    pipeline_root: Path, state: dict[str, Any], batch, *, client: SlurmClientProtocol
) -> dict[str, str]:
    prepared = prepare_stage_submission(batch)
    group_job_ids = submit_prepared_stage_batch(
        prepared, client=client, policy="recover_partial"
    )
    state["state"] = "waiting_stage"
    state["current_stage"] = batch.stage_name
    save_controller_state(pipeline_root, state)
    record_controller_event(
        pipeline_root,
        "stage_submitted",
        stage=batch.stage_name,
        job_ids=list(group_job_ids.values()),
    )
    return group_job_ids


def wait_terminal(
    batch,
    *,
    client: SlurmClientProtocol,
    poll_seconds: int,
    missing_output_grace_seconds: int,
) -> None:
    batch_root = Path(batch.submission_root)
    while True:
        group_job_ids = read_submission_state(batch_root).submitted_group_job_ids
        if not group_job_ids:
            raise ConfigContractError(
                f"No scheduler job ids recorded in submission ledger for {batch_root}"
            )
        reconcile_stage_batch_with_slurm(
            batch,
            group_job_ids=group_job_ids,
            client=client,
            missing_output_grace_seconds=missing_output_grace_seconds,
        )
        if batch_terminal(batch_root):
            return
        time.sleep(poll_seconds)


def mark_stage_completed(
    pipeline_root: Path, state: dict[str, Any], stage_name: str
) -> None:
    completed = set(state.get("completed_stages") or [])
    completed.add(stage_name)
    state["completed_stages"] = sorted(completed)
    state["state"] = "stage_complete"
    state["current_stage"] = stage_name
    save_controller_state(pipeline_root, state)
    record_controller_event(pipeline_root, "stage_complete", stage=stage_name)
