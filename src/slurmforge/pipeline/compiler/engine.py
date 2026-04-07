from __future__ import annotations

from .config_pass import apply_config_pass
from .planning_pass import failed_run_from_source_input, planned_run_from_source_input
from .base import CompilerFlowBase
from .reporting import build_compile_report, build_contextless_report
from .reports import BatchCompileReport
from .requests import SourceRequest
from .state import MaterializedSourceBundle
from ..planning import PlannedRun
from ..planning.validator import PlanningValidationError
from ..sources.models import FailedCompiledRun
from .diagnostics import raise_internal_compiler_error


def resolve_strategy(
    source: SourceRequest,
    *,
    strategies: tuple[CompilerFlowBase, ...],
) -> CompilerFlowBase:
    for strategy in strategies:
        if isinstance(source, strategy.request_types):
            return strategy
    raise TypeError("unsupported source request type")


def compile_materialized_bundle(
    materialized: MaterializedSourceBundle,
    *,
    include_planning: bool,
    strategy: CompilerFlowBase,
) -> BatchCompileReport:
    report = materialized.report
    total_runs = report.checked_inputs
    failed_runs: list[FailedCompiledRun] = list(report.source_failures)
    successful_runs: list[PlannedRun] = []
    initial_state = strategy.initialize_compile_state(
        materialized,
        include_planning=include_planning,
    )
    if isinstance(initial_state, BatchCompileReport):
        return initial_state
    if materialized.context is None:
        return build_contextless_report(
            materialized=materialized,
            state=initial_state,
            failed_runs=failed_runs,
            checked_runs=total_runs,
        )

    state = initial_state
    for source_input in sorted(report.source_inputs, key=lambda item: item.source_index):
        config_result = apply_config_pass(
            strategy=strategy,
            state=state,
            materialized=materialized,
            source_input=source_input,
            total_runs=total_runs,
        )
        if config_result.failed_run is not None:
            failed_runs.append(config_result.failed_run)
            continue

        state = config_result.state
        assert config_result.spec is not None
        if not include_planning:
            continue

        try:
            if state.identity is None:
                raise_internal_compiler_error(
                    RuntimeError("state.identity is None after config pass"),
                    context="planning pass: identity not initialized",
                    run_index=source_input.source_index,
                    total_runs=total_runs,
                    sweep_case_name=source_input.sweep_case_name,
                )
            successful_runs.append(
                planned_run_from_source_input(
                    spec=config_result.spec,
                    source_input=source_input,
                    total_runs=total_runs,
                    identity=state.identity,
                )
            )
        except (PlanningValidationError, FileNotFoundError, ValueError) as exc:
            failed_runs.append(
                failed_run_from_source_input(
                    source_input=source_input,
                    total_runs=total_runs,
                    phase="planning",
                    exc=exc,
                    spec=config_result.spec,
                )
            )

    return build_compile_report(
        materialized=materialized,
        state=state,
        successful_runs=successful_runs,
        failed_runs=failed_runs,
        checked_runs=total_runs,
        include_planning=include_planning,
    )
