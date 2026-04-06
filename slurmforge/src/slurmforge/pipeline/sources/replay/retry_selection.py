from __future__ import annotations

import json
from pathlib import Path

from ...records import RunPlan, load_run_snapshot
from ...status import load_or_infer_execution_status, status_matches_query
from ..failures import build_source_failure
from .models import RetryCandidate, RetrySelectionResult, SelectedRetryCandidate


def select_retry_candidates(
    *,
    plans: tuple[RunPlan, ...],
    status_query: str,
) -> RetrySelectionResult:
    candidates: list[SelectedRetryCandidate] = []
    failed_runs = []
    selected_indices: list[int] = []
    selected_run_ids: list[str] = []
    selected_failure_counts: dict[str, int] = {}
    matched_count = 0

    for plan in plans:
        try:
            status = load_or_infer_execution_status(Path(plan.run_dir))
        except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
            matched_count += 1
            failed_runs.append(
                build_source_failure(
                    source_index=matched_count,
                    total_inputs=max(matched_count, 1),
                    source_label=f"retry run {plan.run_id} (status source={plan.run_dir})",
                    run_cfg=None,
                    sweep_case_name=None,
                    sweep_assignments={},
                    exc=exc,
                )
            )
            continue

        if not status_matches_query(status, status_query):
            continue

        matched_count += 1
        source_label = f"retry run {plan.run_id}"
        try:
            snapshot = load_run_snapshot(Path(plan.run_dir))
            selected = SelectedRetryCandidate(
                source_index=matched_count,
                candidate=RetryCandidate(plan=plan, snapshot=snapshot, status=status),
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
