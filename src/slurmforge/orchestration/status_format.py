from __future__ import annotations

from ..control_job_contract import ControlJobRecord
from ..storage.workflow_status_records import WorkflowStatusRecord
from .status_read_model import StatusReadModel


def render_status_lines_from_model(model: StatusReadModel) -> list[str]:
    lines: list[str] = []
    if model.workflow_status is not None:
        lines.append(_workflow_status_line(model.workflow_status))
    for materialization in model.materializations:
        reason = _trim(materialization.reason)
        lines.append(
            f"[STATUS] materialization stage={materialization.stage_name} "
            f"state={materialization.state} class={materialization.failure_class} reason={reason}"
        )
    lines.append(
        f"[STATUS] root={model.root} total_stages={len(model.statuses)} "
        f"matched={len(model.matched_statuses)} query={model.query}"
    )
    if model.counts:
        lines.append(
            "[STATUS] counts: "
            + ", ".join(f"{key}={model.counts[key]}" for key in sorted(model.counts))
        )
    for stage_name in sorted(model.stage_counts):
        summary = ", ".join(
            f"{key}={model.stage_counts[stage_name][key]}"
            for key in sorted(model.stage_counts[stage_name])
        )
        lines.append(f"[STATUS] stage={stage_name}: {summary}")
    for status in model.matched_statuses:
        failure_class = status.failure_class or "-"
        attempt = status.latest_attempt_id or "-"
        reason = _trim(status.reason)
        lines.append(
            f"{status.run_id}.{status.stage_name}: state={status.state} "
            f"class={failure_class} attempt={attempt} reason={reason}"
        )
    return lines


def _workflow_status_line(workflow_status: WorkflowStatusRecord) -> str:
    jobs = ",".join(
        _control_job_display(record)
        for _, record in sorted(workflow_status.control_jobs.items())
    ) or "-"
    reason = _trim(workflow_status.reason)
    return f"[STATUS] control state={workflow_status.state} jobs={jobs} reason={reason}"


def _control_job_display(record: ControlJobRecord) -> str:
    if record.scheduler_job_ids:
        job_ids = ",".join(record.scheduler_job_ids)
        return f"{record.key}={job_ids}"
    suffix = record.state if not record.reason else f"{record.state}({_trim(record.reason)})"
    return f"{record.key}={suffix}"


def _trim(value: str, limit: int = 120) -> str:
    cleaned = " ".join((value or "").split())
    return cleaned if len(cleaned) <= limit else cleaned[: limit - 3] + "..."
