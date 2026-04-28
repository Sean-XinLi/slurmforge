from __future__ import annotations

from .models import FailedStageSummary, NotificationSummary, NotificationSummaryInput


def _run_counts(summary_input: NotificationSummaryInput) -> dict[str, int]:
    counts: dict[str, int] = {}
    for status in summary_input.run_statuses:
        counts[status.state] = counts.get(status.state, 0) + 1
    return counts


def _stage_counts(summary_input: NotificationSummaryInput) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    for status in summary_input.stage_statuses:
        stage = counts.setdefault(status.stage_name, {})
        stage[status.state] = stage.get(status.state, 0) + 1
    return counts


def _failed_stages(summary_input: NotificationSummaryInput) -> tuple[FailedStageSummary, ...]:
    failed = []
    for status in summary_input.stage_statuses:
        if status.state == "success":
            continue
        if status.state not in {"failed", "blocked", "cancelled", "skipped"}:
            continue
        failed.append(
            FailedStageSummary(
                run_id=status.run_id,
                stage_name=status.stage_name,
                state=status.state,
                failure_class=status.failure_class or status.state,
                reason=status.reason,
            )
        )
    return tuple(sorted(failed, key=lambda item: (item.run_id, item.stage_name)))


def build_notification_summary(summary_input: NotificationSummaryInput) -> NotificationSummary:
    return NotificationSummary(
        event=summary_input.event,
        root_kind=summary_input.root_kind,
        root=summary_input.root,
        project=summary_input.project,
        experiment=summary_input.experiment,
        object_id=summary_input.object_id,
        state=summary_input.state,
        total_runs=len(summary_input.run_statuses),
        run_counts=_run_counts(summary_input),
        stage_counts=_stage_counts(summary_input),
        failed_stages=_failed_stages(summary_input),
    )


def render_summary_text(summary: NotificationSummary) -> str:
    title = (
        "SlurmForge train/eval pipeline finished"
        if summary.root_kind == "train_eval_pipeline"
        else "SlurmForge stage batch finished"
    )
    object_label = "Train/eval pipeline" if summary.root_kind == "train_eval_pipeline" else "Stage batch"
    lines = [
        title,
        "",
        f"Project: {summary.project}",
        f"Experiment: {summary.experiment}",
        f"{object_label}: {summary.object_id}",
        f"Root: {summary.root}",
        "",
        "Summary:",
        f"  total runs: {summary.total_runs}",
    ]
    for state in sorted(summary.run_counts):
        lines.append(f"  {state}: {summary.run_counts[state]}")
    lines.extend(["", "Stages:"])
    for stage_name in sorted(summary.stage_counts):
        counts = summary.stage_counts[stage_name]
        state_text = ", ".join(f"{state} {counts[state]}" for state in sorted(counts))
        lines.append(f"  {stage_name}: {state_text}")
    lines.extend(["", "Failed runs:"])
    if summary.failed_stages:
        for item in summary.failed_stages:
            reason = item.failure_class or item.state
            if item.reason:
                reason = f"{reason}: {item.reason}"
            lines.append(f"  {item.run_id}.{item.stage_name} {item.state}: {reason}")
    else:
        lines.append("  none")
    lines.extend(
        [
            "",
            "Inspect:",
            f"  sforge status --from {summary.root} --reconcile",
            "",
        ]
    )
    return "\n".join(lines)
