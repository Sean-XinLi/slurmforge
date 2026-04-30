from __future__ import annotations

from pathlib import Path
from typing import Any

from ..slurm import SlurmClientProtocol
from ..submission.generation import prepare_stage_submission
from ..submission.ledger import submitted_group_job_ids
from ..submission.submitter import submit_prepared_stage_batch
from .state import record_workflow_event
from .state_model import record_stage_jobs, stage_key


def ensure_stage_submitted(
    pipeline_root: Path,
    state: dict[str, Any],
    batch,
    *,
    client: SlurmClientProtocol,
    state_group_id: str | None = None,
) -> dict[str, str]:
    batch_root = Path(batch.submission_root)
    expected_group_ids = {group.group_id for group in batch.group_plans}
    existing = submitted_group_job_ids(batch_root)
    if expected_group_ids and expected_group_ids.issubset(existing):
        record_stage_jobs(
            pipeline_root,
            state,
            batch.stage_name,
            existing,
            group_id=state_group_id,
        )
        record_workflow_event(
            pipeline_root,
            "stage_submission_adopted",
            stage=batch.stage_name,
            stage_key=stage_key(batch.stage_name, group_id=state_group_id),
            job_ids=list(existing.values()),
        )
        return existing

    prepared = prepare_stage_submission(batch)
    group_job_ids = submit_prepared_stage_batch(
        prepared, client=client, policy="recover_partial"
    )
    record_stage_jobs(
        pipeline_root,
        state,
        batch.stage_name,
        group_job_ids,
        group_id=state_group_id,
    )
    record_workflow_event(
        pipeline_root,
        "stage_submitted",
        stage=batch.stage_name,
        stage_key=stage_key(batch.stage_name, group_id=state_group_id),
        job_ids=list(group_job_ids.values()),
    )
    return group_job_ids
