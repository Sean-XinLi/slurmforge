from __future__ import annotations

import copy
import itertools
from typing import Any

from ..config_contract.options import RUN_CASES, RUN_MATRIX, RUN_SINGLE
from ..contracts import RunDefinition
from .models import ExperimentSpec
from .run_ids import matrix_run_id_for, run_id_for, validate_case_run_id


def iter_run_overrides(spec: ExperimentSpec) -> tuple[dict[str, Any], ...]:
    if spec.runs.type == RUN_SINGLE:
        return ({},)
    if spec.runs.type == RUN_CASES:
        return tuple(copy.deepcopy(case.set) for case in spec.runs.cases)
    if spec.runs.type == RUN_MATRIX:
        return tuple(
            copy.deepcopy(overrides)
            for _case_name, _combo_index, overrides in _matrix_run_expansions(spec)
        )
    return _grid_overrides(spec.runs.axes)


def expand_run_definitions(spec: ExperimentSpec) -> tuple[RunDefinition, ...]:
    if spec.runs.type == RUN_MATRIX:
        return _expand_matrix_run_definitions(spec)
    runs: list[RunDefinition] = []
    overrides = iter_run_overrides(spec)
    for index, run_overrides in enumerate(overrides, start=1):
        run_id = (
            validate_case_run_id(spec.runs.cases[index - 1].name)
            if spec.runs.type == RUN_CASES
            else run_id_for(index, run_overrides, spec.spec_snapshot_digest)
        )
        runs.append(
            RunDefinition(
                run_id=run_id,
                run_index=index,
                run_overrides=copy.deepcopy(run_overrides),
                spec_snapshot_digest=spec.spec_snapshot_digest,
            )
        )
    return tuple(runs)


def _grid_overrides(
    axes: tuple[tuple[str, tuple[Any, ...]], ...],
) -> tuple[dict[str, Any], ...]:
    keys = [key for key, _values in axes]
    value_sets = [tuple(values) for _key, values in axes]
    return tuple(dict(zip(keys, combo)) for combo in itertools.product(*value_sets))


def _matrix_run_expansions(
    spec: ExperimentSpec,
) -> tuple[tuple[str, int, dict[str, Any]], ...]:
    expansions: list[tuple[str, int, dict[str, Any]]] = []
    for case in spec.runs.cases:
        for combo_index, axis_overrides in enumerate(
            _grid_overrides(case.axes), start=1
        ):
            overrides = copy.deepcopy(case.set)
            overrides.update(axis_overrides)
            expansions.append((case.name, combo_index, overrides))
    return tuple(expansions)


def _expand_matrix_run_definitions(spec: ExperimentSpec) -> tuple[RunDefinition, ...]:
    runs: list[RunDefinition] = []
    for index, (case_name, combo_index, run_overrides) in enumerate(
        _matrix_run_expansions(spec), start=1
    ):
        runs.append(
            RunDefinition(
                run_id=matrix_run_id_for(
                    case_name,
                    combo_index,
                    run_overrides,
                    spec.spec_snapshot_digest,
                ),
                run_index=index,
                run_overrides=copy.deepcopy(run_overrides),
                spec_snapshot_digest=spec.spec_snapshot_digest,
            )
        )
    return tuple(runs)
