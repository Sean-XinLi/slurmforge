from __future__ import annotations

from ..io import SchemaVersion
from ..status.models import RunStatusRecord, StageStatusRecord, TERMINAL_STATES, TrainEvalPipelineStatusRecord


def aggregate_run_status(run_id: str, statuses: list[StageStatusRecord]) -> RunStatusRecord:
    stage_states = {status.stage_name: status.state for status in statuses}
    if not statuses:
        state = "missing"
    elif any(status.state == "failed" for status in statuses):
        state = "failed"
    elif any(status.state == "blocked" for status in statuses):
        state = "blocked"
    elif all(status.state == "success" for status in statuses):
        state = "success"
    elif any(status.state == "running" for status in statuses):
        state = "running"
    elif any(status.state == "queued" for status in statuses):
        state = "queued"
    else:
        state = "planned"
    return RunStatusRecord(
        schema_version=SchemaVersion.STATUS,
        run_id=run_id,
        state=state,
        stage_states=stage_states,
    )


def aggregate_run_statuses(statuses: list[StageStatusRecord]) -> tuple[RunStatusRecord, ...]:
    grouped: dict[str, list[StageStatusRecord]] = {}
    for status in statuses:
        grouped.setdefault(status.run_id, []).append(status)
    return tuple(aggregate_run_status(run_id, grouped[run_id]) for run_id in sorted(grouped))


def aggregate_train_eval_pipeline_status(
    pipeline_id: str, statuses: list[StageStatusRecord]
) -> TrainEvalPipelineStatusRecord:
    run_ids = sorted({status.run_id for status in statuses})
    stage_counts: dict[str, dict[str, int]] = {}
    for status in statuses:
        counts = stage_counts.setdefault(status.stage_name, {})
        counts[status.state] = counts.get(status.state, 0) + 1
    return TrainEvalPipelineStatusRecord(
        schema_version=SchemaVersion.STATUS,
        pipeline_id=pipeline_id,
        state=pipeline_state(statuses),
        total_runs=len(run_ids),
        stage_counts=stage_counts,
    )


def root_state(run_statuses: tuple[RunStatusRecord, ...]) -> str:
    states = [status.state for status in run_statuses]
    if not states:
        return "missing"
    if any(state == "failed" for state in states):
        return "failed"
    if any(state == "blocked" for state in states):
        return "blocked"
    if all(state == "success" for state in states):
        return "success"
    if any(state == "running" for state in states):
        return "running"
    if any(state == "queued" for state in states):
        return "queued"
    return "planned"


def pipeline_state(statuses: list[StageStatusRecord]) -> str:
    if not statuses:
        return "missing"
    if any(status.state == "failed" for status in statuses):
        return "failed"
    if any(status.state == "blocked" for status in statuses):
        return "blocked"
    if all(status.state in TERMINAL_STATES for status in statuses) and all(
        status.state == "success" for status in statuses
    ):
        return "success"
    if any(status.state == "running" for status in statuses):
        return "running"
    if any(status.state == "queued" for status in statuses):
        return "queued"
    return "planned"
