from __future__ import annotations

import itertools
import math
from typing import Any, Iterator

from .materialize import materialize_override_assignments
from .models import SweepExpansion, SweepSpec


def axis_combo_count(axes: tuple[tuple[str, tuple[Any, ...]], ...]) -> int:
    return math.prod(len(values) for _key, values in axes) if axes else 1


def iter_axis_combos(axes: tuple[tuple[str, tuple[Any, ...]], ...]) -> Iterator[list[tuple[str, Any]]]:
    if not axes:
        yield []
        return

    keys = [key for key, _values in axes]
    values = [list(axis_values) for _key, axis_values in axes]
    for combo in itertools.product(*values):
        yield list(zip(keys, combo))


def merge_axes(
    shared_axes: tuple[tuple[str, tuple[Any, ...]], ...],
    case_axes: tuple[tuple[str, tuple[Any, ...]], ...],
) -> tuple[tuple[str, tuple[Any, ...]], ...]:
    return tuple(sorted(shared_axes + case_axes, key=lambda item: item[0]))


def iter_sweep_expansions(spec: SweepSpec) -> Iterator[SweepExpansion]:
    if not spec.enabled:
        yield SweepExpansion(case_name=None, assignments=())
        return

    def expansion_iter() -> Iterator[SweepExpansion]:
        for case in spec.cases:
            case_assignments = list(case.set_values)
            merged_axes = merge_axes(spec.shared_axes, case.axes)
            for axis_combo in iter_axis_combos(merged_axes):
                yield SweepExpansion(
                    case_name=case.name,
                    assignments=tuple(case_assignments + axis_combo),
                )

    expansions_iter: Iterator[SweepExpansion] = expansion_iter()
    if spec.max_runs is not None:
        expansions_iter = itertools.islice(expansions_iter, spec.max_runs)
    yield from expansions_iter


def iter_sweep_assignments(spec: SweepSpec) -> Iterator[tuple[tuple[str, Any], ...]]:
    for expansion in iter_sweep_expansions(spec):
        yield expansion.assignments


def count_sweep_spec(spec: SweepSpec) -> int:
    if not spec.enabled:
        return 1

    shared_count = axis_combo_count(spec.shared_axes)
    total = sum(shared_count * axis_combo_count(case.axes) for case in spec.cases)
    if spec.max_runs is None:
        return total
    return min(total, spec.max_runs)


def expand_sweep(cfg: dict[str, Any], spec: SweepSpec) -> Iterator[dict[str, Any]]:
    for expansion in iter_sweep_expansions(spec):
        if not expansion.assignments:
            yield cfg
            continue
        yield materialize_override_assignments(cfg, expansion.assignments)


def count_sweep(cfg: dict[str, Any]) -> int:
    from .validation import normalize_sweep_config

    return count_sweep_spec(normalize_sweep_config(cfg))


def iter_sweep(cfg: dict[str, Any]):
    from .validation import normalize_sweep_config

    yield from expand_sweep(cfg, normalize_sweep_config(cfg))
