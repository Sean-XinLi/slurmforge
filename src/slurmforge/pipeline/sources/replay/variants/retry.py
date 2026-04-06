from __future__ import annotations

from pathlib import Path
from typing import Sequence

from ....records import load_batch_run_plans
from ...failures import source_diagnostic
from ...models import FailedCompiledRun, SourceInputBatch, SourceRunInput
from ..resume_patch import prepare_retry_cfg
from ..retry_refs import build_retry_source_ref
from ..retry_selection import select_retry_candidates


def collect_retry_batch_source(
    *,
    source_batch_root: Path,
    status_query: str,
    cli_overrides: Sequence[str],
) -> SourceInputBatch:
    resolved_batch_root = source_batch_root.expanduser().resolve()
    manifest_extras = {"retry_source": {"source_batch_root": str(resolved_batch_root), "status_query": status_query}}
    source_summary = f"batch={resolved_batch_root} status={status_query}"
    try:
        plans = load_batch_run_plans(resolved_batch_root)
    except (FileNotFoundError, ValueError) as exc:
        return SourceInputBatch(
            source_inputs=(),
            batch_diagnostics=(source_diagnostic(str(exc), code="source_selection_error"),),
            manifest_extras=manifest_extras,
            source_summary=source_summary,
        )

    selection = select_retry_candidates(
        plans=plans,
        status_query=status_query,
    )

    if selection.matched_count == 0:
        return SourceInputBatch(
            source_inputs=(),
            batch_diagnostics=(
                source_diagnostic(
                    f"No runs matched status={status_query!r} under batch_root={resolved_batch_root}",
                    code="source_selection_error",
                ),
            ),
            manifest_extras=manifest_extras,
            source_summary=source_summary,
        )

    source_inputs = tuple(
        SourceRunInput(
            source_kind="retry",
            source_index=selected.source_index,
            run_cfg=prepare_retry_cfg(selected.candidate),
            source=build_retry_source_ref(
                source_batch_root=resolved_batch_root,
                candidate=selected.candidate,
            ),
            sweep_case_name=selected.candidate.snapshot.sweep_case_name,
            sweep_assignments=dict(selected.candidate.snapshot.sweep_assignments),
            original_run_index=selected.candidate.plan.run_index,
        )
        for selected in selection.candidates
    )

    manifest_extras["retry_source"].update(
        {
            "cli_overrides": list(cli_overrides),
            "selected_run_count": len(source_inputs),
            "selected_run_indices": list(selection.selected_run_indices),
            "selected_run_ids": list(selection.selected_run_ids),
            "selected_failure_counts": dict(selection.selected_failure_counts),
        }
    )

    normalized_failed_runs = tuple(
        FailedCompiledRun(
            run_index=failed_run.run_index,
            total_runs=selection.matched_count,
            project=failed_run.project,
            experiment_name=failed_run.experiment_name,
            model_name=failed_run.model_name,
            train_mode=failed_run.train_mode,
            phase=failed_run.phase,
            source_label=failed_run.source_label,
            sweep_case_name=failed_run.sweep_case_name,
            sweep_assignments=failed_run.sweep_assignments,
            diagnostics=failed_run.diagnostics,
        )
        for failed_run in selection.failed_runs
    )

    return SourceInputBatch(
        source_inputs=source_inputs,
        failed_runs=normalized_failed_runs,
        checked_inputs=selection.matched_count,
        manifest_extras=manifest_extras,
        source_summary=source_summary,
    )
