from __future__ import annotations

from ....sources.replay import collect_replay_source_inputs, collect_retry_source_inputs
from ...diagnostics import raise_internal_compiler_error
from ...reports import SourceCollectionReport
from ...requests import ReplaySourceRequest, RetrySourceRequest
from ...state import CollectedSourceBundle


def collect_replay_source_bundle(source: ReplaySourceRequest) -> CollectedSourceBundle:
    try:
        batch = collect_replay_source_inputs(
            source_run_dir=source.source_run_dir,
            source_batch_root=source.source_batch_root,
            run_ids=source.run_ids,
            run_indices=source.run_indices,
        )
    except Exception as exc:
        raise_internal_compiler_error(exc, context="collecting replay sources")

    return CollectedSourceBundle(
        report=SourceCollectionReport(
            request=source,
            batch=batch,
        ),
    )


def collect_retry_source_bundle(source: RetrySourceRequest) -> CollectedSourceBundle:
    try:
        batch = collect_retry_source_inputs(
            source_batch_root=source.source_batch_root,
            status_query=source.status_query,
            cli_overrides=list(source.cli_overrides),
        )
    except Exception as exc:
        raise_internal_compiler_error(exc, context="collecting retry sources")

    return CollectedSourceBundle(
        report=SourceCollectionReport(
            request=source,
            batch=batch,
        ),
    )
