from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

from slurmforge.storage import PlanningStore, RunExecutionView

from ...records import RunPlan
from ...status import status_matches_query
from ..failures import build_source_failure
from .models import RetryCandidate, RetrySelectionResult, SelectedRetryCandidate


def select_retry_candidates(
    *,
    batch_root: Path,
    planning_store: PlanningStore,
    plans_by_run_id: Mapping[str, RunPlan],
    views: tuple[RunExecutionView, ...],
    status_query: str,
) -> RetrySelectionResult:
    candidates: list[SelectedRetryCandidate] = []
    failed_runs = []
    selected_indices: list[int] = []
    selected_run_ids: list[str] = []
    selected_failure_counts: dict[str, int] = {}
    matched_count = 0

    for view in views:
        status = view.latest_status
        if not status_matches_query(status, status_query):
            continue

        matched_count += 1
        plan = plans_by_run_id.get(view.run_id)
        if plan is None:
            failed_runs.append(
                build_source_failure(
                    source_index=matched_count,
                    total_inputs=max(matched_count, 1),
                    source_label=f"retry run {view.run_id}",
                    run_cfg=None,
                    sweep_case_name=None,
                    sweep_assignments={},
                    exc=FileNotFoundError(f"No RunPlan found for run_id={view.run_id}"),
                )
            )
            continue

        source_label = f"retry run {plan.run_id}"
        try:
            snapshot = planning_store.load_run_snapshot(batch_root, plan.run_id)
            if snapshot is None:
                raise FileNotFoundError(f"No snapshot found for run_id={plan.run_id}")
            selected = SelectedRetryCandidate(
                source_index=matched_count,
                candidate=RetryCandidate(
                    plan=plan,
                    snapshot=snapshot,
                    status=status,
                    latest_result_dir=view.latest_result_dir,
                ),
            )
            candidates.append(selected)
            selected_indices.append(selected.candidate.plan.run_index)
            selected_run_ids.append(selected.candidate.plan.run_id)
            failure_key = (
                "missing"
                if selected.candidate.status is None
                else selected.candidate.status.failure_class or selected.candidate.status.state or "unknown"
            )
            selected_failure_counts[failure_key] = selected_failure_counts.get(failure_key, 0) + 1
        except (FileNotFoundError, TypeError, ValueError, json.JSONDecodeError) as exc:
            failed_runs.append(
                build_source_failure(
                    source_index=matched_count,
                    total_inputs=max(matched_count, 1),
                    source_label=source_label,
                    run_cfg=None,
                    sweep_case_name=None,
                    sweep_assignments={},
                    exc=exc,
                )
            )

    return RetrySelectionResult(
        matched_count=matched_count,
        candidates=tuple(candidates),
        failed_runs=tuple(failed_runs),
        selected_run_indices=tuple(selected_indices),
        selected_run_ids=tuple(selected_run_ids),
        selected_failure_counts=selected_failure_counts,
    )
