from __future__ import annotations

from pathlib import Path
from typing import Sequence

from ..failures import source_diagnostic
from ..models import SourceInputBatch
from .variants.batch import collect_replay_batch_source
from .variants.retry import collect_retry_batch_source
from .variants.run import collect_replay_run_source, invalid_replay_selector_batch


def collect_replay_source_inputs(
    *,
    source_run_dir: Path | None,
    source_batch_root: Path | None,
    run_ids: Sequence[str] = (),
    run_indices: Sequence[int] = (),
) -> SourceInputBatch:
    if source_run_dir is not None:
        resolved_run_dir = source_run_dir.expanduser().resolve()
        if run_ids or run_indices:
            return invalid_replay_selector_batch(f"run={resolved_run_dir}")
        return collect_replay_run_source(resolved_run_dir)

    if source_batch_root is None:
        return SourceInputBatch(
            source_inputs=(),
            batch_diagnostics=(
                source_diagnostic(
                    "replay requires one of --from-run or --from-batch",
                    code="source_selection_error",
                ),
            ),
            manifest_extras={},
            source_summary="<missing replay source>",
        )
    return collect_replay_batch_source(
        source_batch_root=source_batch_root,
        run_ids=run_ids,
        run_indices=run_indices,
    )


def collect_retry_source_inputs(
    *,
    source_batch_root: Path,
    status_query: str,
    cli_overrides: Sequence[str],
) -> SourceInputBatch:
    return collect_retry_batch_source(
        source_batch_root=source_batch_root,
        status_query=status_query,
        cli_overrides=cli_overrides,
    )
