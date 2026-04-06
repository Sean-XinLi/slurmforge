from __future__ import annotations

import json
from pathlib import Path
from typing import Sequence

from ....records import load_batch_run_plans, load_run_snapshot, resolve_dispatch_record_path
from ...failures import build_source_failure, source_diagnostic
from ...models import SourceInputBatch, SourceRunInput
from ..loaders import replay_input_from_snapshot
from ..selectors import select_batch_plans


def collect_replay_batch_source(
    *,
    source_batch_root: Path,
    run_ids: Sequence[str],
    run_indices: Sequence[int],
) -> SourceInputBatch:
    resolved_batch_root = source_batch_root.expanduser().resolve()
    manifest_extras = {
        "replay_source": {
            "source_kind": "batch",
            "source_batch_root": str(resolved_batch_root),
        }
    }
    source_summary = f"batch={resolved_batch_root}"
    try:
        plans = load_batch_run_plans(resolved_batch_root)
        selected_plans = select_batch_plans(plans, run_ids=run_ids, run_indices=run_indices)
    except (FileNotFoundError, ValueError) as exc:
        return SourceInputBatch(
            source_inputs=(),
            batch_diagnostics=(source_diagnostic(str(exc), code="source_selection_error"),),
            manifest_extras=manifest_extras,
            source_summary=source_summary,
        )

    total_inputs = len(selected_plans)
    source_inputs: list[SourceRunInput] = []
    failed_runs = []
    for selected_index, plan in enumerate(selected_plans, start=1):
        source_label = f"replay run {plan.run_id} (batch={resolved_batch_root})"
        try:
            snapshot = load_run_snapshot(Path(plan.run_dir))
            record_path = resolve_dispatch_record_path(resolved_batch_root, plan.dispatch)
            source_inputs.append(
                replay_input_from_snapshot(
                    snapshot,
                    config_path=None if record_path is None else record_path.resolve(),
                    config_label=source_label,
                    source_batch_root=resolved_batch_root,
                    source_record_path=record_path,
                    selected_index=selected_index,
                )
            )
        except (FileNotFoundError, TypeError, ValueError, json.JSONDecodeError) as exc:
            failed_runs.append(
                build_source_failure(
                    source_index=selected_index,
                    total_inputs=total_inputs,
                    source_label=source_label,
                    run_cfg=None,
                    sweep_case_name=None,
                    sweep_assignments={},
                    exc=exc,
                )
            )

    return SourceInputBatch(
        source_inputs=tuple(source_inputs),
        failed_runs=tuple(failed_runs),
        checked_inputs=total_inputs,
        manifest_extras=manifest_extras,
        source_summary=source_summary,
    )
