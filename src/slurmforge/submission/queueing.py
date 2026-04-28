from __future__ import annotations

from pathlib import Path

from ..io import SchemaVersion
from ..plans.stage import StageBatchPlan
from ..status import StageStatusRecord, commit_stage_status, read_stage_status


def mark_stage_batch_queued(batch: StageBatchPlan, group_job_ids: dict[str, str]) -> None:
    reason = f"submitted array jobs={','.join(group_job_ids.values())}"
    root = Path(batch.submission_root)
    for instance in batch.stage_instances:
        run_dir = root / instance.run_dir_rel
        status = read_stage_status(run_dir)
        commit_stage_status(
            run_dir,
            StageStatusRecord(
                schema_version=SchemaVersion.STATUS,
                stage_instance_id=instance.stage_instance_id,
                run_id=instance.run_id,
                stage_name=instance.stage_name,
                state="queued",
                latest_attempt_id=None if status is None else status.latest_attempt_id,
                latest_output_digest=None if status is None else status.latest_output_digest,
                failure_class=None,
                reason=reason,
            ),
            source="submission",
        )
