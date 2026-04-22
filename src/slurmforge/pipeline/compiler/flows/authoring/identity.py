from __future__ import annotations

import copy

from ....planning import build_batch_identity
from ...diagnostics import diagnostic_from_exception, raise_internal_compiler_error
from ...reports import BatchCompileReport, build_compile_failure_report
from ...state import (
    AuthoringMaterializedState,
    BatchFirstWinsState,
    CompileState,
    MaterializedSourceBundle,
)


def initialize_authoring_compile_state(
    materialized: MaterializedSourceBundle,
    *,
    include_planning: bool,
) -> CompileState | BatchCompileReport:
    context = materialized.context
    assert context is None or isinstance(context, AuthoringMaterializedState)
    if context is None:
        return CompileState(batch_diagnostics=materialized.batch_diagnostics)

    identity = None
    if include_planning:
        try:
            identity = build_batch_identity(
                project_root=context.shared.project_root,
                project=context.shared.project,
                experiment_name=context.shared.experiment_name,
                base_output_dir=context.shared.output.base_output_dir,
                configured_batch_name=context.shared.output.batch_name,
                default_batch_name=context.default_batch_name,
            )
        except (FileNotFoundError, ValueError) as exc:
            return build_compile_failure_report(
                materialized=materialized,
                failed_runs=list(materialized.report.source_failures),
                checked_runs=materialized.report.checked_inputs,
                notify_cfg=context.shared.notify,
                submit_dependencies=copy.deepcopy(context.shared.output.dependencies),
                exc=exc,
                category="config",
                code="batch_identity_error",
                stage="batch",
                diagnostics_from_exception=diagnostic_from_exception,
            )
        except Exception as exc:
            raise_internal_compiler_error(exc, context="building batch identity")

    # Authoring seeds first-wins from BatchSharedSpec directly: the whole
    # batch is defined by a single top-level config, so every sweep-expanded
    # spec ends up with the same values.  No per-spec disagreement is
    # possible for these four fields.
    first_wins: BatchFirstWinsState | None = None
    if identity is not None:
        first_wins = BatchFirstWinsState(
            identity=identity,
            notify_cfg=context.shared.notify,
            submit_dependencies=copy.deepcopy(context.shared.output.dependencies),
            storage_config=context.shared.storage,
        )
    return CompileState(
        first_wins=first_wins,
        batch_diagnostics=materialized.batch_diagnostics,
        # max_available_gpus / dispatch candidates are populated by
        # accept_authoring_spec as each sweep-expanded spec is built — the
        # authoring path goes through the same candidate-resolve path as
        # replay so the two flows share diagnostic behavior.
    )
