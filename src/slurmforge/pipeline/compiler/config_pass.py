from __future__ import annotations

from dataclasses import dataclass

from ..config.api import ExperimentSpec
from ..planning.validator import PlanningValidationError
from ..sources.models import FailedCompiledRun, SourceRunInput
from .diagnostics import raise_internal_compiler_error
from .planning_pass import failed_run_from_source_input
from .base import CompilerFlowBase
from .state import CompileState, MaterializedSourceBundle


@dataclass(frozen=True)
class ConfigPassResult:
    state: CompileState
    spec: ExperimentSpec | None = None
    failed_run: FailedCompiledRun | None = None


def apply_config_pass(
    *,
    strategy: CompilerFlowBase,
    state: CompileState,
    materialized: MaterializedSourceBundle,
    source_input: SourceRunInput,
    total_runs: int,
) -> ConfigPassResult:
    try:
        spec = strategy.build_spec(materialized, source_input)
    except (PlanningValidationError, FileNotFoundError, ValueError) as exc:
        return ConfigPassResult(
            state=state,
            failed_run=failed_run_from_source_input(
                source_input=source_input,
                total_runs=total_runs,
                phase="config",
                exc=exc,
            ),
        )
    except Exception as exc:
        raise_internal_compiler_error(
            exc,
            context="materializing experiment spec",
            run_index=source_input.source_index,
            total_runs=total_runs,
            sweep_case_name=source_input.sweep_case_name,
        )

    try:
        next_state = strategy.accept_spec(
            state,
            spec=spec,
            materialized=materialized,
            source_input=source_input,
        )
    except (PlanningValidationError, FileNotFoundError, ValueError) as exc:
        return ConfigPassResult(
            state=state,
            failed_run=failed_run_from_source_input(
                source_input=source_input,
                total_runs=total_runs,
                phase="config",
                exc=exc,
                spec=spec,
            ),
        )
    except Exception as exc:
        raise_internal_compiler_error(
            exc,
            context="resolving batch contract",
            run_index=source_input.source_index,
            total_runs=total_runs,
            sweep_case_name=source_input.sweep_case_name,
        )

    return ConfigPassResult(state=next_state, spec=spec)
