from __future__ import annotations

from pathlib import Path

from ...plans.resources import ControlResourcesPlan
from ...plans.stage import StageBatchPlan
from ..sbatch_helpers import _job_name, _q
from .headers import render_control_job_headers


def _submit_root(batch: StageBatchPlan) -> Path:
    return Path(batch.submission_root) / "submit"


def _notification_resources(batch: StageBatchPlan) -> ControlResourcesPlan:
    return batch.notification_plan.resources


def render_stage_notification_sbatch(
    batch: StageBatchPlan, event: str, *, generation_id: str
) -> str:
    notification_dir = _submit_root(batch) / "notifications" / generation_id
    logs_dir = _submit_root(batch) / "logs" / generation_id
    resources = _notification_resources(batch)
    lines = render_control_job_headers(
        job_name=_job_name("sforge", batch.project, batch.stage_name, "notify", event),
        stdout_path=logs_dir / f"notify-{event}-%j.out",
        stderr_path=logs_dir / f"notify-{event}-%j.err",
        resources=resources,
    )
    lines.extend(
        [
            "set -euo pipefail",
            f"BATCH_ROOT={_q(batch.submission_root)}",
            f"NOTIFICATION_EVENT={_q(event)}",
            'printf "%s\\n" "[NOTIFY] event=${NOTIFICATION_EVENT} root=${BATCH_ROOT}"',
            "true",
            "",
        ]
    )
    notification_dir.mkdir(parents=True, exist_ok=True)
    return "\n".join(lines)


def render_stage_notification_barrier_sbatch(
    batch: StageBatchPlan,
    event: str,
    *,
    generation_id: str,
    barrier_index: int,
) -> str:
    logs_dir = _submit_root(batch) / "logs" / generation_id
    resources = _notification_resources(batch)
    lines = render_control_job_headers(
        job_name=_job_name(
            "sforge",
            batch.project,
            batch.stage_name,
            "notify-barrier",
            str(barrier_index),
        ),
        stdout_path=logs_dir / f"notify-barrier-{event}-{barrier_index:03d}-%j.out",
        stderr_path=logs_dir / f"notify-barrier-{event}-{barrier_index:03d}-%j.err",
        resources=resources,
    )
    lines.extend(
        [
            "set -euo pipefail",
            f'printf "%s\\n" "notification barrier event={event}"',
            "",
        ]
    )
    return "\n".join(lines)
