from __future__ import annotations

from pathlib import Path

from ..root_model.detection import detect_root
from ..root_model.runs import collect_stage_statuses
from ..root_model.snapshots import refresh_root_status
from ..status.query import state_matches
from ..storage.batch_materialization_records import read_materialization_status
from ..storage.execution_index import iter_execution_batch_roots
from ..storage.workflow import read_workflow_status
from ..submission.reconcile import reconcile_root_submissions


def _trim(value: str, limit: int = 120) -> str:
    cleaned = " ".join((value or "").split())
    return cleaned if len(cleaned) <= limit else cleaned[: limit - 3] + "..."


def render_status_lines(
    *,
    root: Path,
    status_query: str = "all",
    stage: str | None = None,
    reconcile: bool = False,
    missing_output_grace_seconds: int = 300,
) -> list[str]:
    descriptor = detect_root(root)
    root = descriptor.root
    lines: list[str] = []
    root_is_pipeline = descriptor.kind == "train_eval_pipeline"
    if reconcile:
        reconcile_root_submissions(
            root,
            stage=stage,
            missing_output_grace_seconds=missing_output_grace_seconds,
        )
        refresh_root_status(root)
    if descriptor.kind == "stage_batch":
        materialization = read_materialization_status(root)
        if materialization is not None:
            failure_class = materialization.failure_class or "-"
            reason = _trim(materialization.reason)
            lines.append(
                f"[STATUS] materialization stage={materialization.stage_name} "
                f"state={materialization.state} class={failure_class} reason={reason}"
            )
    elif root_is_pipeline:
        workflow_status = read_workflow_status(root)
        if workflow_status is not None:
            workflow_state = str(workflow_status.get("state") or "unknown")
            gate_job_records = dict(workflow_status.get("gate_jobs") or {})
            gate_jobs = []
            for name, record in sorted(gate_job_records.items()):
                if isinstance(record, dict) and record.get("scheduler_job_id"):
                    gate_jobs.append(f"{name}={record['scheduler_job_id']}")
            job_id = ",".join(gate_jobs) or str(
                workflow_status.get("scheduler_job_id") or "-"
            )
            reason = _trim(str(workflow_status.get("reason") or ""))
            lines.append(
                f"[STATUS] control state={workflow_state} jobs={job_id} reason={reason}"
            )
        for stage_root in iter_execution_batch_roots(root):
            materialization = read_materialization_status(stage_root)
            if materialization is None:
                continue
            if stage is not None and materialization.stage_name != stage:
                continue
            failure_class = materialization.failure_class or "-"
            reason = _trim(materialization.reason)
            lines.append(
                f"[STATUS] materialization stage={materialization.stage_name} "
                f"state={materialization.state} class={failure_class} reason={reason}"
            )
    statuses = collect_stage_statuses(root)
    if stage:
        statuses = [item for item in statuses if item.stage_name == stage]
    matched = [item for item in statuses if state_matches(item, status_query)]
    counts: dict[str, int] = {}
    stage_counts: dict[str, dict[str, int]] = {}
    for status in statuses:
        counts[status.state] = counts.get(status.state, 0) + 1
        by_stage = stage_counts.setdefault(status.stage_name, {})
        by_stage[status.state] = by_stage.get(status.state, 0) + 1
    lines.append(
        f"[STATUS] root={root} total_stages={len(statuses)} matched={len(matched)} query={status_query}"
    )
    if counts:
        lines.append(
            "[STATUS] counts: "
            + ", ".join(f"{key}={counts[key]}" for key in sorted(counts))
        )
    for stage_name in sorted(stage_counts):
        summary = ", ".join(
            f"{key}={stage_counts[stage_name][key]}"
            for key in sorted(stage_counts[stage_name])
        )
        lines.append(f"[STATUS] stage={stage_name}: {summary}")
    for status in matched:
        failure_class = status.failure_class or "-"
        attempt = status.latest_attempt_id or "-"
        reason = _trim(status.reason)
        lines.append(
            f"{status.run_id}.{status.stage_name}: state={status.state} "
            f"class={failure_class} attempt={attempt} reason={reason}"
        )
    return lines
