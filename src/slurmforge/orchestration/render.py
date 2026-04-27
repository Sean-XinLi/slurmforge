from __future__ import annotations

from pathlib import Path

from .controller import reconcile_controller_job
from ..read_models.status import refresh_root_status
from ..submission import reconcile_root_submissions
from ..status import state_matches
from ..storage.controller import read_controller_status
from ..storage.loader import collect_stage_statuses, is_stage_batch_root, is_train_eval_pipeline_root
from ..storage.materialization import read_materialization_status


def _trim(value: str, limit: int = 120) -> str:
    cleaned = " ".join((value or "").split())
    return cleaned if len(cleaned) <= limit else cleaned[: limit - 3] + "..."


def render_status(
    *,
    root: Path,
    status_query: str = "all",
    stage: str | None = None,
    reconcile: bool = False,
    missing_output_grace_seconds: int = 300,
) -> None:
    if not root.exists():
        raise FileNotFoundError(f"status root does not exist: {root}")
    if not is_stage_batch_root(root) and not is_train_eval_pipeline_root(root):
        raise FileNotFoundError(f"not a stage batch or train/eval pipeline root: {root}")
    root_is_pipeline = is_train_eval_pipeline_root(root)
    if reconcile and root_is_pipeline:
        reconcile_controller_job(root)
    if reconcile:
        reconcile_root_submissions(
            root,
            stage=stage,
            missing_output_grace_seconds=missing_output_grace_seconds,
        )
        refresh_root_status(root)
    if is_stage_batch_root(root):
        materialization = read_materialization_status(root)
        if materialization is not None:
            failure_class = materialization.failure_class or "-"
            reason = _trim(materialization.reason)
            print(
                f"[STATUS] materialization stage={materialization.stage_name} "
                f"state={materialization.state} class={failure_class} reason={reason}"
            )
    elif root_is_pipeline:
        controller_status = read_controller_status(root)
        if controller_status is not None:
            controller_state = str(controller_status.get("state") or "unknown")
            job_id = str(controller_status.get("scheduler_job_id") or "-")
            reason = _trim(str(controller_status.get("reason") or ""))
            print(f"[STATUS] controller state={controller_state} job={job_id} reason={reason}")
        for stage_root in sorted((root / "stage_batches").glob("*")):
            if not is_stage_batch_root(stage_root):
                continue
            materialization = read_materialization_status(stage_root)
            if materialization is None:
                continue
            if stage is not None and materialization.stage_name != stage:
                continue
            failure_class = materialization.failure_class or "-"
            reason = _trim(materialization.reason)
            print(
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
    print(f"[STATUS] root={root} total_stages={len(statuses)} matched={len(matched)} query={status_query}")
    if counts:
        print("[STATUS] counts: " + ", ".join(f"{key}={counts[key]}" for key in sorted(counts)))
    for stage_name in sorted(stage_counts):
        summary = ", ".join(f"{key}={stage_counts[stage_name][key]}" for key in sorted(stage_counts[stage_name]))
        print(f"[STATUS] stage={stage_name}: {summary}")
    for status in matched:
        failure_class = status.failure_class or "-"
        attempt = status.latest_attempt_id or "-"
        reason = _trim(status.reason)
        print(
            f"{status.run_id}.{status.stage_name}: state={status.state} "
            f"class={failure_class} attempt={attempt} reason={reason}"
        )
