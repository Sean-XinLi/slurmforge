from __future__ import annotations

from pathlib import Path

from ..slurm import SlurmClientProtocol
from ..submission.generation import prepare_stage_submission
from ..submission.ledger import submitted_group_job_ids
from ..submission.models import PreparedSubmission
from ..submission.submitter import submit_prepared_stage_batch
from .state import record_workflow_event
from .state_model import stage_key


def ensure_stage_submitted(
    pipeline_root: Path,
    batch,
    *,
    client: SlurmClientProtocol,
    state_dispatch_id: str | None = None,
    prepared: PreparedSubmission | None = None,
) -> dict[str, str]:
    batch_root = Path(batch.submission_root)
    expected_group_ids = {group.group_id for group in batch.group_plans}
    existing = submitted_group_job_ids(batch_root)
    if expected_group_ids and expected_group_ids.issubset(existing):
        record_workflow_event(
            pipeline_root,
            "stage_submission_adopted",
            stage=batch.stage_name,
            stage_key=stage_key(batch.stage_name, dispatch_id=state_dispatch_id),
            job_ids=list(existing.values()),
        )
        return existing

    prepared = prepared or prepare_stage_submission(batch)
    group_job_ids = submit_prepared_stage_batch(
        prepared, client=client, policy="recover_partial"
    )
    record_workflow_event(
        pipeline_root,
        "stage_submitted",
        stage=batch.stage_name,
        stage_key=stage_key(batch.stage_name, dispatch_id=state_dispatch_id),
        job_ids=list(group_job_ids.values()),
    )
    return group_job_ids
