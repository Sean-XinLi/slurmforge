from __future__ import annotations

from ....sources.authoring import collect_authoring_source_inputs as collect_authoring_source_batch
from ...diagnostics import raise_internal_compiler_error
from ...reports import SourceCollectionReport
from ...requests import AuthoringSourceRequest
from ...state import AuthoringCollectedState, CollectedSourceBundle


def collect_authoring_source_bundle(source: AuthoringSourceRequest) -> CollectedSourceBundle:
    try:
        collection = collect_authoring_source_batch(
            config_path=source.config_path,
            cli_overrides=source.cli_overrides,
            project_root=source.project_root,
            manifest_extras=source.manifest_extras,
        )
    except Exception as exc:
        raise_internal_compiler_error(exc, context="collecting authoring sources")

    return CollectedSourceBundle(
        report=SourceCollectionReport(
            request=source,
            batch=collection.batch,
        ),
        payload=(
            None
            if collection.context is None
            else AuthoringCollectedState(
                config_path=collection.context.config_path,
                project_root=collection.context.project_root,
                shared=collection.context.shared,
            )
        ),
    )
