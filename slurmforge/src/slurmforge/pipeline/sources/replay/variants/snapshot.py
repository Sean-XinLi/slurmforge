from __future__ import annotations

import json
from pathlib import Path

from ...failures import build_source_failure
from ...models import SourceInputBatch
from ..loaders import load_replay_input_from_snapshot_path


def collect_replay_snapshot_source(source_snapshot_path: Path) -> SourceInputBatch:
    resolved_snapshot_path = source_snapshot_path.expanduser().resolve()
    manifest_extras = {
        "replay_source": {
            "source_kind": "snapshot",
            "source_snapshot_path": str(resolved_snapshot_path),
        }
    }
    try:
        return SourceInputBatch(
            source_inputs=(load_replay_input_from_snapshot_path(resolved_snapshot_path),),
            checked_inputs=1,
            manifest_extras=manifest_extras,
            source_summary=f"snapshot={resolved_snapshot_path}",
        )
    except (FileNotFoundError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return SourceInputBatch(
            source_inputs=(),
            failed_runs=(
                build_source_failure(
                    source_index=1,
                    total_inputs=1,
                    source_label=f"replay snapshot {resolved_snapshot_path}",
                    run_cfg=None,
                    sweep_case_name=None,
                    sweep_assignments={},
                    exc=exc,
                ),
            ),
            checked_inputs=1,
            manifest_extras=manifest_extras,
            source_summary=f"snapshot={resolved_snapshot_path}",
        )
