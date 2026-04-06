from __future__ import annotations

from ....sources.replay import (
    augment_manifest_extras_context,
    parse_cli_overrides,
    resolve_replay_project_root,
)
from ...diagnostics import diagnostic_from_exception, raise_internal_compiler_error
from ...requests import ReplaySourceRequest, RetrySourceRequest
from ...state import CollectedSourceBundle, MaterializedSourceBundle, ReplayMaterializedState


def materialize_replay_bundle(bundle: CollectedSourceBundle) -> MaterializedSourceBundle:
    report = bundle.report
    request = report.request
    assert isinstance(request, (ReplaySourceRequest, RetrySourceRequest))
    manifest_context_key = "replay_source" if isinstance(request, ReplaySourceRequest) else "retry_source"
    batch_diagnostics = list(report.batch_diagnostics)
    manifest_extras = report.manifest_extras

    if not report.source_inputs:
        return MaterializedSourceBundle(
            report=report,
            context=None,
            batch_diagnostics=tuple(batch_diagnostics),
            manifest_extras=manifest_extras,
        )

    project_root_override = None if request.project_root is None else request.project_root.expanduser().resolve()
    try:
        project_root = resolve_replay_project_root(
            report.source_inputs,
            project_root_override=project_root_override,
        )
    except (FileNotFoundError, ValueError) as exc:
        batch_diagnostics.extend(
            diagnostic_from_exception(exc, category="config", code="config_error", stage="batch")
        )
        return MaterializedSourceBundle(
            report=report,
            context=None,
            batch_diagnostics=tuple(batch_diagnostics),
            manifest_extras=manifest_extras,
        )
    except Exception as exc:
        raise_internal_compiler_error(exc, context="resolving replay project root")

    try:
        parsed_overrides = parse_cli_overrides(request.cli_overrides)
    except (FileNotFoundError, ValueError) as exc:
        batch_diagnostics.extend(
            diagnostic_from_exception(exc, category="config", code="config_error", stage="batch")
        )
        return MaterializedSourceBundle(
            report=report,
            context=None,
            batch_diagnostics=tuple(batch_diagnostics),
            manifest_extras=manifest_extras,
        )
    except Exception as exc:
        raise_internal_compiler_error(exc, context="parsing replay overrides")

    try:
        manifest_extras = augment_manifest_extras_context(
            report.manifest_extras,
            context_key=manifest_context_key,
            project_root=project_root,
            source_inputs=report.source_inputs,
            cli_overrides=request.cli_overrides,
        )
    except (FileNotFoundError, ValueError) as exc:
        batch_diagnostics.extend(
            diagnostic_from_exception(exc, category="config", code="manifest_error", stage="batch")
        )
    except Exception as exc:
        raise_internal_compiler_error(exc, context="augmenting replay manifest context")

    return MaterializedSourceBundle(
        report=report,
        context=ReplayMaterializedState(
            project_root_override=project_root_override,
            project_root=project_root,
            cli_overrides=request.cli_overrides,
            parsed_overrides=parsed_overrides,
            default_batch_name=str(request.default_batch_name or ""),
            manifest_context_key=manifest_context_key,
        ),
        batch_diagnostics=tuple(batch_diagnostics),
        manifest_extras=manifest_extras,
    )
