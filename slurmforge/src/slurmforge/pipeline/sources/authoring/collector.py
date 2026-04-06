from __future__ import annotations

from pathlib import Path
from typing import Sequence

from ....sweep import iter_sweep_expansions, materialize_override_assignments
from ...config.assembly.authoring import prepare_authoring_batch_input
from ...planning.contracts import PlanDiagnostic
from ..models import SourceInputBatch, SourceRunInput
from .models import AuthoringPreparedContext, AuthoringSourceCollection
from .loader import load_authoring_source_cfg


def _error_collection(
    *,
    message: str,
    code: str,
    stage: str,
    source_summary: str,
    manifest_extras: dict | None,
) -> AuthoringSourceCollection:
    return AuthoringSourceCollection(
        batch=SourceInputBatch(
            source_inputs=(),
            checked_inputs=0,
            batch_diagnostics=(
                PlanDiagnostic(
                    severity="error",
                    category="config",
                    code=code,
                    message=message,
                    stage=stage,
                ),
            ),
            manifest_extras={} if manifest_extras is None else dict(manifest_extras),
            source_summary=source_summary,
        ),
        context=None,
    )


def collect_authoring_source_inputs(
    *,
    config_path: Path,
    cli_overrides: Sequence[str],
    project_root: Path | None,
    manifest_extras: dict | None = None,
) -> AuthoringSourceCollection:
    resolved_config_path = config_path.expanduser().resolve()
    source_summary = f"config={resolved_config_path}"
    try:
        resolved_config_path, cfg = load_authoring_source_cfg(
            config_path,
            cli_overrides=cli_overrides,
        )
    except (FileNotFoundError, ValueError) as exc:
        return _error_collection(
            message=str(exc),
            code="source_error",
            stage="source",
            source_summary=source_summary,
            manifest_extras=manifest_extras,
        )

    source_summary = f"config={resolved_config_path}"
    resolved_project_root = resolved_config_path.parent if project_root is None else project_root.expanduser().resolve()
    try:
        prepared = prepare_authoring_batch_input(
            cfg,
            config_path=resolved_config_path,
            project_root=resolved_project_root,
        )
    except (FileNotFoundError, ValueError) as exc:
        return _error_collection(
            message=str(exc),
            code="config_error",
            stage="batch",
            source_summary=source_summary,
            manifest_extras=manifest_extras,
        )

    try:
        expansions = tuple(iter_sweep_expansions(prepared.sweep_spec))
    except (FileNotFoundError, ValueError) as exc:
        return _error_collection(
            message=str(exc),
            code="sweep_expansion_error",
            stage="batch",
            source_summary=source_summary,
            manifest_extras=manifest_extras,
        )

    total_runs = len(expansions)
    if total_runs == 0:
        return _error_collection(
            message=f"{resolved_config_path}: sweep expansion produced no runs",
            code="sweep_empty",
            stage="batch",
            source_summary=source_summary,
            manifest_extras=manifest_extras,
        )

    source_inputs = tuple(
        SourceRunInput(
            source_kind="authoring",
            source_index=run_index,
            run_cfg=materialize_override_assignments(prepared.base_cfg, expansion.assignments),
            source={
                "config_label": str(resolved_config_path),
                "config_path": resolved_config_path,
            },
            sweep_case_name=expansion.case_name,
            sweep_assignments=dict(expansion.assignments),
        )
        for run_index, expansion in enumerate(expansions, start=1)
    )
    return AuthoringSourceCollection(
        batch=SourceInputBatch(
            source_inputs=source_inputs,
            checked_inputs=total_runs,
            manifest_extras={} if manifest_extras is None else dict(manifest_extras),
            source_summary=source_summary,
        ),
        context=AuthoringPreparedContext(
            config_path=resolved_config_path,
            project_root=prepared.project_root,
            shared=prepared.shared,
        ),
    )
