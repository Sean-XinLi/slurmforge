from __future__ import annotations

from ..planning import PlannedRun
from ..sources.models import FailedCompiledRun
from .reports import BatchCompileReport, build_materialized_report
from .state import CompileState, MaterializedSourceBundle


def build_contextless_report(
    *,
    materialized: MaterializedSourceBundle,
    state: CompileState,
    failed_runs: list[FailedCompiledRun],
    checked_runs: int,
) -> BatchCompileReport:
    return build_materialized_report(
        materialized=materialized,
        identity=None,
        successful_runs=(),
        failed_runs=failed_runs,
        batch_diagnostics=state.batch_diagnostics or materialized.batch_diagnostics,
        checked_runs=checked_runs,
        notify_cfg=state.notify_cfg,
        submit_dependencies=state.submit_dependencies,
    )


def build_compile_report(
    *,
    materialized: MaterializedSourceBundle,
    state: CompileState,
    successful_runs: list[PlannedRun],
    failed_runs: list[FailedCompiledRun],
    checked_runs: int,
    include_planning: bool,
) -> BatchCompileReport:
    return build_materialized_report(
        materialized=materialized,
        identity=state.identity if include_planning else None,
        successful_runs=successful_runs,
        failed_runs=failed_runs,
        batch_diagnostics=state.batch_diagnostics or materialized.batch_diagnostics,
        checked_runs=checked_runs,
        notify_cfg=state.notify_cfg,
        submit_dependencies=state.submit_dependencies,
    )
