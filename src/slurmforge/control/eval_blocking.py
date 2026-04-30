from __future__ import annotations

from pathlib import Path

from ..io import SchemaVersion
from ..status.machine import commit_stage_status
from ..status.models import StageStatusRecord, TERMINAL_STATES
from ..status.reader import read_stage_status


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
