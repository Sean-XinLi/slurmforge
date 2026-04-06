from __future__ import annotations

import json
from pathlib import Path

from ...failures import build_source_failure, source_diagnostic
from ...models import SourceInputBatch
from ..loaders import load_replay_input_from_run_dir


def collect_replay_run_source(source_run_dir: Path) -> SourceInputBatch:
    resolved_run_dir = source_run_dir.expanduser().resolve()
    manifest_extras = {
        "replay_source": {
            "source_kind": "run",
            "source_run_dir": str(resolved_run_dir),
        }
    }
    try:
        return SourceInputBatch(
            source_inputs=(load_replay_input_from_run_dir(resolved_run_dir),),
            checked_inputs=1,
            manifest_extras=manifest_extras,
            source_summary=f"run={resolved_run_dir}",
        )
    except (FileNotFoundError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return SourceInputBatch(
            source_inputs=(),
            failed_runs=(
                build_source_failure(
                    source_index=1,
                    total_inputs=1,
                    source_label=f"replay run {resolved_run_dir}",
                    run_cfg=None,
                    sweep_case_name=None,
                    sweep_assignments={},
                    exc=exc,
                ),
            ),
            checked_inputs=1,
            manifest_extras=manifest_extras,
            source_summary=f"run={resolved_run_dir}",
        )


def invalid_replay_selector_batch(source_summary: str) -> SourceInputBatch:
    return SourceInputBatch(
        source_inputs=(),
        batch_diagnostics=(
            source_diagnostic(
                "replay --run_id/--run_index are only valid with --from-batch",
                code="source_selection_error",
            ),
        ),
        manifest_extras={},
        source_summary=source_summary,
    )
